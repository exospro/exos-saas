from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import Json


def db_connect():
    database_url = os.environ["DATABASE_URL"]
    return psycopg2.connect(database_url)


def create_run(
    conn,
    *,
    connected_seller_id: int,
    run_type: str,
    status: str = "running",
    params: dict[str, Any] | None = None,
) -> int:
    params = params or {}
    sql = """
    INSERT INTO ml.run (
        connected_seller_id,
        run_type,
        status,
        params
    ) VALUES (%s, %s, %s, %s)
    RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (connected_seller_id, run_type, status, Json(params)))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(
    conn,
    run_id: int,
    *,
    status: str = "finished",
    totals: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    totals = totals or {}
    sql = """
    UPDATE ml.run
       SET finished_at = now(),
           status = %s,
           totals = %s,
           error = %s
     WHERE id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (status, Json(totals), error, run_id))
    conn.commit()
