from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.responses import HTMLResponse
import os
import requests
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone
from psycopg2.extras import RealDictCursor
from etl.inventory.repository import db_connect
import subprocess

app = FastAPI()

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"


@app.get("/")
def root():
    return {"message": "Exos SaaS rodando 🚀"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ml/oauth/start")
def start_oauth(connected_seller_id: int):
    state = f"seller:{connected_seller_id}"

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
    }

    url = f"{AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url)


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")

    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    if not state or not state.startswith("seller:"):
        raise HTTPException(status_code=400, detail="Invalid state")

    connected_seller_id = int(state.split(":")[1])

    token_resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": ML_CLIENT_ID,
            "client_secret": ML_CLIENT_SECRET,
            "code": code,
            "redirect_uri": ML_REDIRECT_URI,
        },
        timeout=30,
    )

    if token_resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Token error: {token_resp.text}")

    tokens = token_resp.json()

    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    token_type = tokens.get("token_type", "Bearer")
    scope = tokens.get("scope")
    expires_in = int(tokens["expires_in"])
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    me_resp = requests.get(
        ME_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )

    if me_resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to fetch user: {me_resp.text}")

    me = me_resp.json()

    ml_user_id = me["id"]
    seller_nickname = me["nickname"]
    site_id = me["site_id"]

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE ml.connected_seller
                   SET ml_user_id = %s,
                       seller_nickname = %s,
                       site_id = %s,
                       status = 'active',
                       authorized_at = now(),
                       updated_at = now()
                 WHERE id = %s
                RETURNING id
                """,
                (ml_user_id, seller_nickname, site_id, connected_seller_id),
            )
            row = cur.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"connected_seller_id {connected_seller_id} não encontrado",
                )

            cur.execute(
                """
                INSERT INTO ml.oauth_token (
                    connected_seller_id,
                    access_token,
                    refresh_token,
                    token_type,
                    scope,
                    expires_at,
                    last_refresh_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (connected_seller_id)
                DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_type = EXCLUDED.token_type,
                    scope = EXCLUDED.scope,
                    expires_at = EXCLUDED.expires_at,
                    last_refresh_at = now(),
                    updated_at = now()
                """,
                (
                    connected_seller_id,
                    access_token,
                    refresh_token,
                    token_type,
                    scope,
                    expires_at,
                ),
            )

        conn.commit()

    #return {
    #    "status": "connected",
    #    "connected_seller_id": connected_seller_id,
    #    "ml_user_id": ml_user_id,
    #    "seller_nickname": seller_nickname,
    #    "site_id": site_id,
    #}

    return RedirectResponse(url="/painel?connected=1")

@app.get("/run/optimizer")
def run_optimizer(connected_seller_id: int, limit: int = 50, dry_run: bool = True):

    cmd = [
        "python3",
        "-m",
        "etl.ml_campaign_optimizer",
        "--connected-seller-id", str(connected_seller_id),
        "--limit", str(limit)
    ]

    if dry_run:
        cmd.append("--dry-run")

    result = subprocess.run(cmd, capture_output=True, text=True)

    return {
        "status": "executado",
        "stdout": result.stdout,
        "stderr": result.stderr
    }

@app.get("/run/full")
def run_full(connected_seller_id: int):

    import subprocess

    results = {}

    # 1. INVENTORY
    cmd_inventory = [
        "python3", "-m", "etl.ml_inventory_snapshot_basic",
        "--connected-seller-id", str(connected_seller_id),
        "--limit-items", "100"
    ]
    r1 = subprocess.run(cmd_inventory, capture_output=True, text=True)
    results["inventory"] = r1.stdout or r1.stderr

    # 2. REBATE
    cmd_rebate = [
        "python3", "-m", "etl.ml_item_promo_rebate_snapshot",
        "--connected-seller-id", str(connected_seller_id),
        "--limit-items", "100"
    ]
    r2 = subprocess.run(cmd_rebate, capture_output=True, text=True)
    results["rebate"] = r2.stdout or r2.stderr

    # 3. OPTIMIZER
    cmd_optimizer = [
        "python3", "-m", "etl.ml_campaign_optimizer",
        "--connected-seller-id", str(connected_seller_id),
        "--limit", "100",
        "--use-cost", "false"
    ]
    r3 = subprocess.run(cmd_optimizer, capture_output=True, text=True)
    results["optimizer"] = r3.stdout or r3.stderr

    return {
        "status": "full run executado",
        "results": results
    }


@app.get("/painel", response_class=HTMLResponse)
def painel(connected: int = 0):
    status_msg = "❌ Não conectado"

    if connected == 1:
        status_msg = "✅ Conta conectada com sucesso"

    return f"""
    <html>
    <head>
        <title>Exos SaaS</title>
        <style>
            body {{
                font-family: Arial;
                background: #0f172a;
                color: white;
                text-align: center;
                padding: 50px;
            }}
            button {{
                padding: 15px 25px;
                margin: 10px;
                font-size: 16px;
                border: none;
                border-radius: 8px;
                cursor: pointer;
            }}
            .btn-connect {{ background: #22c55e; }}
            .btn-run {{ background: #3b82f6; }}
            .box {{
                margin-top: 30px;
                padding: 20px;
                background: #1e293b;
                border-radius: 10px;
                display: inline-block;
            }}
        </style>
    </head>
    <body>

        <h1>🚀 Exos SaaS</h1>
        <p>Otimize suas campanhas automaticamente</p>

        <div class="box">
            <p>{status_msg}</p>
        </div>

        <br><br>

        <a href="/ml/oauth/start?connected_seller_id=1">
            <button class="btn-connect">Conectar Mercado Livre</button>
        </a>

        <br><br>

        <div class="box">

            <h3>Configuração</h3>

            <label>
                <input type="checkbox" id="dryrun" checked>
                Simulação (não altera campanhas)
            </label>

            <br><br>

            <label>Quantidade de anúncios:</label><br>
            <input type="number" id="limit" value="50" style="padding:10px; width:100px">

            <br><br>

            <button class="btn-run" onclick="rodar()">Rodar Otimização</button>

        </div>

        <br><br>

        <pre id="output"></pre>

        <script>
            function rodar() {{
                const dry = document.getElementById("dryrun").checked;
                const limit = document.getElementById("limit").value;

                document.getElementById("output").innerText = "Executando...";

                fetch(`/run/optimizer?connected_seller_id=1&limit=${{limit}}&dry_run=${{dry}}`)
                    .then(res => res.json())
                    .then(data => {{
                        document.getElementById("output").innerText =
                            JSON.stringify(data, null, 2);
                    }})
                    .catch(err => {{
                        document.getElementById("output").innerText = err;
                    }});
            }}
        </script>

    </body>
    </html>
    """