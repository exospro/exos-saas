from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
import os
import requests
from urllib.parse import urlencode
import psycopg2

app = FastAPI()

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]
DATABASE_URL = os.environ["DATABASE_URL"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"


@app.get("/")
def root():
    return {"message": "Exos SaaS rodando 🚀"}


@app.get("/health")
def health():
    return {"status": "ok"}


# 🔹 STEP 1 - REDIRECT PARA AUTORIZAÇÃO
@app.get("/ml/oauth/start")
def oauth_start():
    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url)


# 🔹 STEP 2 - CALLBACK
@app.get("/ml/oauth/callback")
def oauth_callback(request: Request):
    code = request.query_params.get("code")

    if not code:
        return {"error": "code não encontrado"}

    # troca por token
    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    resp = requests.post(TOKEN_URL, data=payload)
    tokens = resp.json()

    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    expires_in = tokens["expires_in"]

    # pega dados do seller
    headers = {"Authorization": f"Bearer {access_token}"}
    me = requests.get(ME_URL, headers=headers).json()

    # salva no banco
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO ml.connected_seller (
            ml_user_id, seller_ickname, site_id, email,
            access_token, refresh_token, token_expires_at
        )
        VALUES (%s,%s,%s,%s,%s,%s, NOW() + (%s || ' seconds')::interval)
        ON CONFLICT (ml_user_id)
        DO UPDATE SET
            access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            token_expires_at = EXCLUDED.token_expires_at
    """, (
        me["id"],
        me["nickname"],
        me["site_id"],
        me["email"],
        access_token,
        refresh_token,
        expires_in
    ))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "seller conectado com sucesso 🚀"}