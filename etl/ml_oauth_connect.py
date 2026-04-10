from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from typing import Any

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]
DATABASE_URL = os.environ["DATABASE_URL"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"


def db_connect():
    return psycopg2.connect(DATABASE_URL)


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

    if raw and "http" not in raw and "code=" not in raw:
        return raw, None

    parsed = urlparse(raw)
    qs = parse_qs(parsed.query)

    code = qs.get("code", [None])[0]
    state = qs.get("state", [None])[0]

    if not code:
        raise ValueError("Não encontrei o parâmetro 'code' na URL informada.")

    return code, state


def exchange_code_for_token(code: str) -> dict[str, Any]:
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


def get_me(access_token: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(ME_URL, headers=headers, timeout=60)

    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text}
        raise RuntimeError(f"Erro ao consultar /users/me: {err}")

    return resp.json()


def upsert_connected_seller(
    conn,
    *,
    connected_seller_id: int,
    ml_user_id: int,
    seller_nickname: str | None,
    site_id: str | None,
) -> None:
    sql = '''
    UPDATE ml.connected_seller
       SET ml_user_id = %s,
           seller_nickname = %s,
           site_id = %s,
           status = 'active',
           authorized_at = now(),
           updated_at = now()
     WHERE id = %s
    '''
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (ml_user_id, seller_nickname, site_id, connected_seller_id),
        )
        if cur.rowcount == 0:
            raise RuntimeError(
                f"connected_seller_id {connected_seller_id} não encontrado em ml.connected_seller"
            )
    conn.commit()


def upsert_oauth_token(
    conn,
    *,
    connected_seller_id: int,
    tokens: dict[str, Any],
) -> None:
    expires_in = int(tokens["expires_in"])

    sql = '''
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
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        now() + make_interval(secs => %s),
        now(),
        now()
    )
    ON CONFLICT (connected_seller_id)
    DO UPDATE SET
        access_token = EXCLUDED.access_token,
        refresh_token = EXCLUDED.refresh_token,
        token_type = EXCLUDED.token_type,
        scope = EXCLUDED.scope,
        expires_at = EXCLUDED.expires_at,
        last_refresh_at = EXCLUDED.last_refresh_at,
        updated_at = EXCLUDED.updated_at
    '''
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                connected_seller_id,
                tokens["access_token"],
                tokens["refresh_token"],
                tokens.get("token_type"),
                tokens.get("scope"),
                expires_in,
            ),
        )
    conn.commit()


def save_oauth_event(
    conn,
    *,
    connected_seller_id: int,
    me: dict[str, Any],
    tokens: dict[str, Any],
) -> None:
    sql = '''
    INSERT INTO ml.run (
        connected_seller_id,
        run_type,
        status,
        params,
        totals,
        finished_at,
        created_at
    ) VALUES (
        %s,
        'oauth_connect',
        'finished',
        %s,
        %s,
        now(),
        now()
    )
    '''
    params = {
        "ml_user_id": me.get("id"),
        "nickname": me.get("nickname"),
        "site_id": me.get("site_id"),
    }
    totals = {
        "token_type": tokens.get("token_type"),
        "expires_in": tokens.get("expires_in"),
        "scope_present": bool(tokens.get("scope")),
    }
    with conn.cursor() as cur:
        cur.execute(sql, (connected_seller_id, Json(params), Json(totals)))
    conn.commit()


def mask_token(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 12:
        return value
    return value[:6] + "..." + value[-6:]


def main() -> None:
    connected_seller_id_raw = input("CONNECTED_SELLER_ID: ").strip()
    if not connected_seller_id_raw.isdigit():
        raise ValueError("CONNECTED_SELLER_ID deve ser numérico.")
    connected_seller_id = int(connected_seller_id_raw)

    expected_state = f"connect-seller-{connected_seller_id}"

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
    me = get_me(tokens["access_token"])

    with db_connect() as conn:
        upsert_connected_seller(
            conn,
            connected_seller_id=connected_seller_id,
            ml_user_id=int(me["id"]),
            seller_nickname=me.get("nickname"),
            site_id=me.get("site_id"),
        )
        upsert_oauth_token(
            conn,
            connected_seller_id=connected_seller_id,
            tokens=tokens,
        )
        save_oauth_event(
            conn,
            connected_seller_id=connected_seller_id,
            me=me,
            tokens=tokens,
        )

    print("\n=== SELLER CONECTADO COM SUCESSO ===")
    print(
        {
            "connected_seller_id": connected_seller_id,
            "ml_user_id": me.get("id"),
            "nickname": me.get("nickname"),
            "site_id": me.get("site_id"),
            "email": me.get("email"),
            "access_token": mask_token(tokens.get("access_token")),
            "refresh_token": mask_token(tokens.get("refresh_token")),
            "expires_in": tokens.get("expires_in"),
        }
    )


if __name__ == "__main__":
    main()
