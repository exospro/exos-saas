from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from etl.ml_auth_db_multi import get_valid_access_token

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]


def main():
    connected_seller_id = 1

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ml.oauth_token
                   SET expires_at = now() - interval '10 minutes',
                       updated_at = now()
                 WHERE connected_seller_id = %s
                """,
                (connected_seller_id,),
            )
        conn.commit()

    print("Token marcado como expirado no banco.")

    token = get_valid_access_token(connected_seller_id)

    print("Refresh executado com sucesso.")
    print(token[:20] + "...")


if __name__ == "__main__":
    main()