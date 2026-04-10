from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"


def build_auth_url(state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def parse_callback_input(raw: str) -> tuple[str, str | None]:
    raw = raw.strip()

    # Se o usuário colar só o code
    if raw and "http" not in raw and "code=" not in raw:
        return raw, None

    parsed = urlparse(raw)
    qs = parse_qs(parsed.query)

    code = qs.get("code", [None])[0]
    state = qs.get("state", [None])[0]

    if not code:
        raise ValueError("Não encontrei o parâmetro 'code' na URL informada.")

    return code, state


def exchange_code_for_token(code: str) -> dict:
    payload = {
        "grant_type": "authorization_code",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "code": code,
        "redirect_uri": ML_REDIRECT_URI,
    }

    resp = requests.post(
        TOKEN_URL,
        data=payload,
        headers={
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        },
        timeout=60,
    )

    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text}
        raise RuntimeError(f"Erro ao trocar code por token: {err}")

    return resp.json()


def get_me(access_token: str) -> dict:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(ME_URL, headers=headers, timeout=60)

    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text}
        raise RuntimeError(f"Erro ao consultar /users/me: {err}")

    return resp.json()


def mask_token(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 12:
        return value
    return value[:6] + "..." + value[-6:]


def main():
    expected_state = "test-state-001"

    print("\n=== URL para autorizar a aplicação ===\n")
    print(build_auth_url(expected_state))

    print("\nDepois de autorizar, cole aqui a URL completa da callback")
    print("ou, se preferir, apenas o valor do code.\n")

    raw_input_value = input("CALLBACK URL ou CODE: ").strip()

    code, returned_state = parse_callback_input(raw_input_value)

    if returned_state is not None and returned_state != expected_state:
        raise RuntimeError(
            f"State inválido. Esperado: {expected_state} | Recebido: {returned_state}"
        )

    tokens = exchange_code_for_token(code)

    print("\n=== TOKENS COMPLETOS ===")
    print(tokens)
    
    print("\n=== TOKENS ===")
    print(
        {
            "access_token": mask_token(tokens.get("access_token")),
            "refresh_token": mask_token(tokens.get("refresh_token")),
            "token_type": tokens.get("token_type"),
            "expires_in": tokens.get("expires_in"),
            "scope": tokens.get("scope"),
            "user_id": tokens.get("user_id"),
        }
    )

    me = get_me(tokens["access_token"])
    print("\n=== USERS/ME ===")
    print(
        {
            "id": me.get("id"),
            "nickname": me.get("nickname"),
            "site_id": me.get("site_id"),
            "email": me.get("email"),
        }
    )


if __name__ == "__main__":
    main()