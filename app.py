from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse
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

    return {
        "status": "connected",
        "connected_seller_id": connected_seller_id,
        "ml_user_id": ml_user_id,
        "seller_nickname": seller_nickname,
        "site_id": site_id,
    }

@app.post("/run/optimizer")
def run_optimizer(connected_seller_id: int):

    cmd = [
        "python3",
        "etl/ml_campaign_optimizer.py",
        "--connected-seller-id", str(connected_seller_id),
        "--limit", "50",
        "--use-cost", "false"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    return {
        "status": "executado",
        "stdout": result.stdout,
        "stderr": result.stderr
    }