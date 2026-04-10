from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv

load_dotenv()

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
DATABASE_URL = os.environ["DATABASE_URL"]
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"

# Renova o token alguns minutos antes do vencimento, para evitar erro no meio da execução.
TOKEN_REFRESH_BUFFER_MINUTES = 5
REQUEST_TIMEOUT_SECONDS = 30


@dataclass
class SellerToken:
    connected_seller_id: int
    access_token: str
    refresh_token: str
    expires_at: datetime
    token_type: str | None = None
    scope: str | None = None


class OAuthSellerNotFoundError(Exception):
    pass


class OAuthRefreshError(Exception):
    pass


def db_connect():
    return psycopg2.connect(DATABASE_URL)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_oauth_row(conn, connected_seller_id: int) -> SellerToken:
    sql = """
        SELECT
            connected_seller_id,
            access_token,
            refresh_token,
            expires_at,
            token_type,
            scope
        FROM ml.oauth_token
        WHERE connected_seller_id = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (connected_seller_id,))
        row = cur.fetchone()

    if not row:
        raise OAuthSellerNotFoundError(
            f"Nenhum token OAuth encontrado para connected_seller_id={connected_seller_id}."
        )

    return SellerToken(
        connected_seller_id=int(row["connected_seller_id"]),
        access_token=row["access_token"],
        refresh_token=row["refresh_token"],
        expires_at=_ensure_utc(row["expires_at"]),
        token_type=row.get("token_type"),
        scope=row.get("scope"),
    )


def is_token_expiring_soon(expires_at: datetime, buffer_minutes: int = TOKEN_REFRESH_BUFFER_MINUTES) -> bool:
    expires_at = _ensure_utc(expires_at)
    now_utc = datetime.now(timezone.utc)
    return expires_at <= now_utc + timedelta(minutes=buffer_minutes)


def refresh_access_token(refresh_token: str) -> dict[str, Any]:
    payload = {
        "grant_type": "refresh_token",
        "client_id": ML_CLIENT_ID,
        "client_secret": ML_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }

    response = requests.post(ML_TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)

    if not response.ok:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise OAuthRefreshError(
            f"Falha ao renovar token no Mercado Livre. status={response.status_code} detail={detail}"
        )

    data = response.json()

    if "access_token" not in data or "expires_in" not in data:
        raise OAuthRefreshError(f"Resposta inesperada ao renovar token: {data}")

    return data


def update_oauth_row(
    conn,
    *,
    connected_seller_id: int,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    token_type: str | None = None,
    scope: str | None = None,
) -> None:
    new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))

    sql = """
        UPDATE ml.oauth_token
           SET access_token = %s,
               refresh_token = %s,
               token_type = COALESCE(%s, token_type),
               scope = COALESCE(%s, scope),
               expires_at = %s,
               last_refresh_at = NOW(),
               updated_at = NOW()
         WHERE connected_seller_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                access_token,
                refresh_token,
                token_type,
                scope,
                new_expires_at,
                connected_seller_id,
            ),
        )

    conn.commit()


def get_valid_access_token(connected_seller_id: int) -> str:
    """
    Retorna um access_token válido para o seller informado.
    Se estiver perto do vencimento, renova automaticamente.
    """
    conn = db_connect()
    try:
        token_row = get_oauth_row(conn, connected_seller_id)

        if is_token_expiring_soon(token_row.expires_at):
            refreshed = refresh_access_token(token_row.refresh_token)
            update_oauth_row(
                conn,
                connected_seller_id=connected_seller_id,
                access_token=refreshed["access_token"],
                refresh_token=refreshed.get("refresh_token", token_row.refresh_token),
                expires_in=int(refreshed["expires_in"]),
                token_type=refreshed.get("token_type"),
                scope=refreshed.get("scope"),
            )
            token_row = get_oauth_row(conn, connected_seller_id)

        return token_row.access_token
    finally:
        conn.close()


def get_headers(connected_seller_id: int) -> dict[str, str]:
    token = get_valid_access_token(connected_seller_id)
    return {"Authorization": f"Bearer {token}"}
