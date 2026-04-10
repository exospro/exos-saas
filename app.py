from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, FileResponse
import os
import re
import time
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests
from psycopg2.extras import RealDictCursor

from etl.inventory.repository import db_connect

app = FastAPI()

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"

BASE_DIR = Path("/opt/render/project/src")
CSV_DIR = BASE_DIR / "runtime_csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)


def build_csv_path(prefix: str = "campaign_optimizer_audit") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return CSV_DIR / f"{prefix}_{timestamp}.csv"


def get_connected_seller_summary(connected_seller_id: int) -> dict:
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    cs.id,
                    cs.ml_user_id,
                    cs.seller_nickname,
                    cs.site_id,
                    cs.status,
                    cs.authorized_at,
                    ot.expires_at,
                    ot.updated_at AS token_updated_at
                FROM ml.connected_seller cs
                LEFT JOIN ml.oauth_token ot
                  ON ot.connected_seller_id = cs.id
                WHERE cs.id = %s
                """,
                (connected_seller_id,),
            )
            row = cur.fetchone()

    if not row:
        return {
            "exists": False,
            "connected": False,
            "connected_seller_id": connected_seller_id,
        }

    connected = bool(row["ml_user_id"]) and row["status"] == "active"

    return {
        "exists": True,
        "connected": connected,
        "connected_seller_id": row["id"],
        "ml_user_id": row["ml_user_id"],
        "seller_nickname": row["seller_nickname"],
        "site_id": row["site_id"],
        "status": row["status"],
        "authorized_at": row["authorized_at"],
        "expires_at": row["expires_at"],
        "token_updated_at": row["token_updated_at"],
    }


def extract_csv_name(text: str) -> str | None:
    match = re.search(r'campaign_optimizer_audit_[0-9_]+\.csv', text or "")
    return match.group(0) if match else None


@app.get("/")
def root():
    return RedirectResponse(url="/painel")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ml/oauth/start")
def start_oauth(connected_seller_id: int = 1):
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

    return RedirectResponse(url=f"/painel?connected_seller_id={connected_seller_id}&connected=1")

import time

@app.get("/run/optimizer")
def run_optimizer(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    csv_path = build_csv_path()

    cmd = [
        "python3",
        "-m",
        "etl.ml_campaign_optimizer",
        "--connected-seller-id",
        str(connected_seller_id),
        "--use-cost",
        "true" if use_cost else "false",
        "--out",
        str(csv_path),
    ]

    if limit and limit > 0:
        cmd.extend(["--limit", str(limit)])

    if dry_run:
        cmd.append("--dry-run")

    started = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": elapsed,
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_file": csv_path.name if csv_path.exists() else None,
        }
    )


import time

@app.get("/run/full")
def run_full(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    started = time.time()
    results = {}

    cmd_inventory = [
        "python3",
        "-m",
        "etl.ml_inventory_snapshot_basic",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit and limit > 0:
        cmd_inventory.extend(["--limit-items", str(limit)])
    r1 = subprocess.run(cmd_inventory, capture_output=True, text=True)
    results["inventory"] = r1.stdout or r1.stderr

    cmd_rebate = [
        "python3",
        "-m",
        "etl.ml_item_promo_rebate_snapshot",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit and limit > 0:
        cmd_rebate.extend(["--limit-items", str(limit)])
    r2 = subprocess.run(cmd_rebate, capture_output=True, text=True)
    results["rebate"] = r2.stdout or r2.stderr

    csv_path = build_csv_path()

    cmd_optimizer = [
        "python3",
        "-m",
        "etl.ml_campaign_optimizer",
        "--connected-seller-id",
        str(connected_seller_id),
        "--use-cost",
        "true" if use_cost else "false",
        "--out",
        str(csv_path),
    ]
    if limit and limit > 0:
        cmd_optimizer.extend(["--limit", str(limit)])
    if dry_run:
        cmd_optimizer.append("--dry-run")

    r3 = subprocess.run(cmd_optimizer, capture_output=True, text=True)
    results["optimizer"] = r3.stdout or r3.stderr

    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "full run executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": elapsed,
            "csv_file": csv_path.name if csv_path.exists() else None,
            "results": results,
        }
    )

@app.get("/download/csv")
def download_csv(filename: str):
    file_path = CSV_DIR / filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="CSV não encontrado")

    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="text/csv",
    )


@app.get("/painel", response_class=HTMLResponse)
def painel(connected_seller_id: int = 1, connected: int = 0):
    seller = get_connected_seller_summary(connected_seller_id)

    if seller["connected"]:
        status_html = f"""
        <div class="status-card connected">
            <div class="status-title">✅ Conectado</div>
            <div class="status-line"><strong>Conta:</strong> {seller.get("seller_nickname") or "-"}</div>
            <div class="status-line"><strong>ML User ID:</strong> {seller.get("ml_user_id") or "-"}</div>
            <div class="status-line"><strong>Site:</strong> {seller.get("site_id") or "-"}</div>
        </div>
        """
    else:
        status_html = """
        <div class="status-card disconnected">
            <div class="status-title">❌ Não conectado</div>
            <div class="status-line">Conecte sua conta do Mercado Livre para liberar a execução.</div>
        </div>
        """

    connected_banner = ""
    if connected == 1:
        connected_banner = """
        <div class="flash success">
            Conta conectada com sucesso.
        </div>
        """

    return f"""
    <html>
    <head>
        <title>Exos SaaS</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            * {{
                box-sizing: border-box;
            }}
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: linear-gradient(180deg, #06122b 0%, #091a3f 100%);
                color: #ffffff;
            }}
            .container {{
                max-width: 980px;
                margin: 0 auto;
                padding: 40px 20px 60px;
            }}
            .hero {{
                text-align: center;
                margin-bottom: 28px;
            }}
            .hero h1 {{
                font-size: 56px;
                margin: 0 0 10px;
                font-weight: 800;
            }}
            .hero p {{
                font-size: 20px;
                color: #d8e3ff;
                margin: 0;
            }}
            .flash {{
                max-width: 760px;
                margin: 0 auto 20px;
                padding: 14px 18px;
                border-radius: 14px;
                text-align: center;
                font-weight: 700;
            }}
            .flash.success {{
                background: rgba(34, 197, 94, 0.18);
                border: 1px solid rgba(34, 197, 94, 0.35);
                color: #d8ffe5;
            }}
            .grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
                align-items: start;
            }}
            .card {{
                background: rgba(255,255,255,0.08);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 20px;
                padding: 24px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.22);
            }}
            .card h2 {{
                margin: 0 0 18px;
                font-size: 28px;
            }}
            .status-card {{
                border-radius: 16px;
                padding: 18px;
            }}
            .status-card.connected {{
                background: rgba(34, 197, 94, 0.14);
                border: 1px solid rgba(34, 197, 94, 0.28);
            }}
            .status-card.disconnected {{
                background: rgba(239, 68, 68, 0.12);
                border: 1px solid rgba(239, 68, 68, 0.25);
            }}
            .status-title {{
                font-size: 24px;
                font-weight: 800;
                margin-bottom: 10px;
            }}
            .status-line {{
                font-size: 16px;
                color: #e8eeff;
                margin-top: 6px;
            }}
            .actions {{
                display: flex;
                flex-direction: column;
                gap: 12px;
                margin-top: 16px;
            }}
            .btn {{
                width: 100%;
                border: none;
                border-radius: 14px;
                padding: 16px 18px;
                font-size: 18px;
                font-weight: 700;
                cursor: pointer;
                transition: transform .05s ease, opacity .2s ease;
            }}
            .btn:hover {{
                opacity: .94;
            }}
            .btn:active {{
                transform: scale(0.99);
            }}
            .btn-connect {{
                background: #22c55e;
                color: #051b0d;
            }}
            .btn-primary {{
                background: #3b82f6;
                color: #ffffff;
            }}
            .btn-secondary {{
                background: #0f172a;
                color: #ffffff;
                border: 1px solid rgba(255,255,255,0.15);
            }}
            .form-row {{
                margin-bottom: 14px;
                text-align: left;
            }}
            .form-row label {{
                display: block;
                margin-bottom: 8px;
                font-weight: 700;
                color: #dbe6ff;
            }}
            .form-inline {{
                display: flex;
                align-items: center;
                gap: 10px;
                flex-wrap: wrap;
            }}
            input[type="number"], select {{
                width: 220px;
                padding: 12px 14px;
                border-radius: 12px;
                border: none;
                font-size: 18px;
            }}
            .check {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 18px;
                margin: 10px 0;
            }}
            .output-wrap {{
                margin-top: 24px;
            }}
            .output-head {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                flex-wrap: wrap;
                margin-bottom: 10px;
            }}
            .download-area {{
                display: none;
            }}
            .download-area.show {{
                display: inline-block;
            }}
            pre {{
                white-space: pre-wrap;
                word-break: break-word;
                text-align: left;
                background: #020817;
                color: #dbeafe;
                border: 1px solid rgba(255,255,255,0.08);
                padding: 18px;
                border-radius: 16px;
                min-height: 180px;
                max-height: 520px;
                overflow: auto;
                font-size: 13px;
                line-height: 1.45;
            }}
            .muted {{
                color: #9fb0d9;
                font-size: 14px;
                margin-top: 8px;
            }}
            #customLimitRow {{
                display: none;
            }}
            @media (max-width: 820px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}
                .hero h1 {{
                    font-size: 42px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="hero">
                <h1>🚀 Exos SaaS</h1>
                <p>Otimize suas campanhas automaticamente com segurança</p>
            </div>

            {connected_banner}

            <div class="grid">
                <div class="card">
                    <h2>Conta Mercado Livre</h2>
                    {status_html}

                    <div class="actions">
                        <a href="/ml/oauth/start?connected_seller_id={connected_seller_id}">
                            <button class="btn btn-connect">Conectar ou Reconectar Mercado Livre</button>
                        </a>
                    </div>
                </div>

                <div class="card">
                    <h2>Executar otimização</h2>

                    <div class="form-row">
                        <label for="connectedSellerId">Connected Seller ID</label>
                        <input type="number" id="connectedSellerId" value="{connected_seller_id}" />
                    </div>

                    <div class="form-row">
                        <label for="limitMode">Escopo</label>
                        <select id="limitMode" onchange="toggleLimitInput()">
                            <option value="50">50 anúncios</option>
                            <option value="100">100 anúncios</option>
                            <option value="custom">Quantidade personalizada</option>
                            <option value="all">Todos os anúncios</option>
                        </select>
                    </div>

                    <div class="form-row" id="customLimitRow">
                        <label for="limit">Quantidade personalizada</label>
                        <input type="number" id="limit" value="50" />
                    </div>

                    <div class="check">
                        <input type="checkbox" id="dryrun" checked />
                        <label for="dryrun">Simulação (não altera campanhas)</label>
                    </div>

                    <div class="check">
                        <input type="checkbox" id="usecost" />
                        <label for="usecost">Usar custo do produto</label>
                    </div>

                    <div class="actions">
                        <button class="btn btn-primary" onclick="rodarOptimizer()">Rodar apenas optimizer</button>
                        <button class="btn btn-secondary" onclick="rodarFull()">Rodar pipeline completo</button>
                    </div>

                    <div class="muted">
                        Pipeline completo executa: inventory snapshot → rebate snapshot → optimizer
                    </div>
                </div>
            </div>

            <div class="output-wrap">
                <div class="output-head">
                    <h2>Resultado</h2>
                    <div id="downloadArea" class="download-area">
                        <a id="downloadLink" href="#" target="_blank">
                            <button class="btn btn-secondary">Baixar CSV de auditoria</button>
                        </a>
                    </div>
                </div>

                <pre id="output">Nenhuma execução ainda.</pre>
            </div>
        </div>

        <script>
            function toggleLimitInput() {{
                const mode = document.getElementById("limitMode").value;
                const row = document.getElementById("customLimitRow");
                row.style.display = mode === "custom" ? "block" : "none";
            }}

            function getLimitValue() {{
                const mode = document.getElementById("limitMode").value;

                if (mode === "all") {{
                    return 0;
                }}
                if (mode === "custom") {{
                    return document.getElementById("limit").value || 0;
                }}
                return mode;
            }}

            function getParams() {{
                const connectedSellerId = document.getElementById("connectedSellerId").value;
                const limit = getLimitValue();
                const dryRun = document.getElementById("dryrun").checked;
                const useCost = document.getElementById("usecost").checked;

                return {{
                    connectedSellerId,
                    limit,
                    dryRun,
                    useCost
                }};
            }}

            function updateOutput(data) {{
                document.getElementById("output").innerText = JSON.stringify(data, null, 2);

                const downloadArea = document.getElementById("downloadArea");
                const downloadLink = document.getElementById("downloadLink");

                let csvFile = null;
                if (data && data.csv_file) {{
                    csvFile = data.csv_file;
                }}

                if (csvFile) {{
                    downloadLink.href = `/download/csv?filename=${{encodeURIComponent(csvFile)}}`;
                    downloadArea.classList.add("show");
                }} else {{
                    downloadArea.classList.remove("show");
                    downloadLink.href = "#";
                }}
            }}

            async function rodarOptimizer() {{
                const p = getParams();
                document.getElementById("output").innerText = "Executando optimizer...";

                const url = `/run/optimizer?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`;

                try {{
                    const res = await fetch(url);
                    const data = await res.json();
                    updateOutput(data);
                }} catch (err) {{
                    document.getElementById("output").innerText = String(err);
                }}
            }}

            async function rodarFull() {{
                const p = getParams();
                document.getElementById("output").innerText = "Executando pipeline completo...";

                const url = `/run/full?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`;

                try {{
                    const res = await fetch(url);
                    const data = await res.json();
                    updateOutput(data);
                }} catch (err) {{
                    document.getElementById("output").innerText = String(err);
                }}
            }}

            toggleLimitInput();
        </script>
    </body>
    </html>
    """