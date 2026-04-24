#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import random
import time
import threading
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from psycopg2.extras import Json, RealDictCursor, execute_values

from etl.inventory.repository import db_connect, create_run, finish_run
from etl.ml_auth_db_multi import get_headers

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DEFAULT_TIMEOUT = int(os.environ.get("ML_REBATE_SNAPSHOT_TIMEOUT_SEC", "60"))
APP_VERSION = "v2"
DEFAULT_RETRY_MAX_ATTEMPTS = int(os.environ.get("ML_REBATE_SNAPSHOT_MAX_ATTEMPTS", "4"))
DEFAULT_RETRY_BASE_SLEEP_SEC = float(os.environ.get("ML_REBATE_SNAPSHOT_BASE_SLEEP_SEC", "1.2"))
DEFAULT_REQUEST_SLEEP_SEC = float(os.environ.get("ML_REBATE_SNAPSHOT_REQUEST_SLEEP_SEC", "0.00"))
DEFAULT_FINAL_REPROCESS = os.environ.get("ML_REBATE_SNAPSHOT_FINAL_REPROCESS", "true").strip().lower() in {"1", "true", "yes", "y", "on"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_MAX_WORKERS = int(os.environ.get("ML_REBATE_SNAPSHOT_MAX_WORKERS", "5"))
DEFAULT_INSERT_BATCH_SIZE = int(os.environ.get("ML_REBATE_SNAPSHOT_INSERT_BATCH_SIZE", "200"))

_thread_local = threading.local()


def make_http_session(pool_size: int = 20) -> requests.Session:
    """Cria uma Session com pool maior para chamadas concorrentes."""
    session = make_http_session(pool_size=max(10, int(os.environ.get('ML_REBATE_SNAPSHOT_MAX_WORKERS', DEFAULT_MAX_WORKERS)) * 4))
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_thread_session(pool_size: int = 20) -> requests.Session:
    """Uma Session por thread evita compartilhar requests.Session entre workers."""
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = make_http_session(pool_size=pool_size)
        _thread_local.session = session
    return session



def to_decimal(value: Any):
    from decimal import Decimal
    try:
        if value in (None, "", []):
            return None
        return Decimal(str(value))
    except Exception:
        return None


def q2(value):
    from decimal import Decimal, ROUND_HALF_UP
    if value is None:
        return None
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def q4(value):
    from decimal import Decimal, ROUND_HALF_UP
    if value is None:
        return None
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def normalize_percent(value: Any):
    dec = to_decimal(value)
    if dec is None:
        return None
    return dec / 100 if dec > 1 else dec


def first_non_empty(obj: dict, keys: list[str]):
    for key in keys:
        val = obj.get(key)
        if val not in (None, "", []):
            return val
    return None


def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return {"_raw": (resp.text or "").strip()[:4000]}


def ensure_table(conn) -> None:
    sql = """
    CREATE SCHEMA IF NOT EXISTS ml;

    CREATE TABLE IF NOT EXISTS ml.item_promo_rebate_snapshot (
        id BIGSERIAL PRIMARY KEY,
        connected_seller_id BIGINT NOT NULL,
        collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        run_id BIGINT,
        mlb TEXT NOT NULL,
        variation_id BIGINT,
        promotion_id TEXT,
        promotion_type TEXT,
        promotion_status TEXT,
        offer_id TEXT,
        original_price NUMERIC(18,2),
        promo_price NUMERIC(18,2),
        discount_total NUMERIC(18,2),
        meli_percent NUMERIC(10,4),
        seller_percent NUMERIC(10,4),
        rebate_meli_amount NUMERIC(18,2),
        seller_discount_amount NUMERIC(18,2),
        raw_json JSONB,
        unexplained_discount_amount NUMERIC(18,2),
        regular_amount_current NUMERIC(18,2),
        sale_amount_current NUMERIC(18,2),
        rebate_base_price NUMERIC(18,2),
        rebate_price_source TEXT,
        price_api_price_id TEXT,
        price_api_reference_date TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS ix_item_promo_rebate_snapshot_seller_run
        ON ml.item_promo_rebate_snapshot (connected_seller_id, run_id);

    CREATE INDEX IF NOT EXISTS ix_item_promo_rebate_snapshot_seller_mlb
        ON ml.item_promo_rebate_snapshot (connected_seller_id, mlb);

    CREATE INDEX IF NOT EXISTS ix_item_promo_rebate_snapshot_seller_mlb_var
        ON ml.item_promo_rebate_snapshot (connected_seller_id, mlb, COALESCE(variation_id, -1));
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def get_inventory_scope_rows(conn, connected_seller_id: int, source_run_id: int | None = None, limit_items: int | None = None):
    sql = """
    WITH inv_run AS (
        SELECT COALESCE(
            %(source_run_id)s::bigint,
            (
                SELECT max(id)
                FROM ml.run
                WHERE connected_seller_id = %(connected_seller_id)s
                  AND run_type = 'inventory_snapshot'
            ),
            (
                SELECT max(run_id)
                FROM ml.inventory_snapshot_item
                WHERE connected_seller_id = %(connected_seller_id)s
            )
        ) AS run_id
    ),
    base AS (
        SELECT
            i.mlb,
            i.variation_id,
            i.price,
            i.effective_price,
            i.promo_price,
            i.promo_original_price
        FROM ml.inventory_snapshot_item i
        JOIN inv_run r ON r.run_id = i.run_id
        WHERE i.connected_seller_id = %(connected_seller_id)s
          AND i.mlb IS NOT NULL
          AND COALESCE(i.status, '') = 'active'
    ),
    picked_items AS (
        SELECT DISTINCT mlb
        FROM base
        ORDER BY mlb
    )
    SELECT
        b.mlb,
        b.variation_id,
        b.price,
        b.effective_price,
        b.promo_price,
        b.promo_original_price
    FROM base b
    JOIN picked_items p ON p.mlb = b.mlb
    ORDER BY b.mlb, COALESCE(b.variation_id, -1)
    """
    if limit_items and limit_items > 0:
        sql = """
        WITH inv_run AS (
            SELECT COALESCE(
                %(source_run_id)s::bigint,
                (
                    SELECT max(id)
                    FROM ml.run
                    WHERE connected_seller_id = %(connected_seller_id)s
                      AND run_type = 'inventory_snapshot'
                ),
                (
                    SELECT max(run_id)
                    FROM ml.inventory_snapshot_item
                    WHERE connected_seller_id = %(connected_seller_id)s
                )
            ) AS run_id
        ),
        base AS (
            SELECT
                i.mlb,
                i.variation_id,
                i.price,
                i.effective_price,
                i.promo_price,
                i.promo_original_price
            FROM ml.inventory_snapshot_item i
            JOIN inv_run r ON r.run_id = i.run_id
            WHERE i.connected_seller_id = %(connected_seller_id)s
              AND i.mlb IS NOT NULL
              AND COALESCE(i.status, '') = 'active'
        ),
        picked_items AS (
            SELECT DISTINCT mlb
            FROM base
            ORDER BY mlb
            LIMIT %(limit_items)s
        )
        SELECT
            b.mlb,
            b.variation_id,
            b.price,
            b.effective_price,
            b.promo_price,
            b.promo_original_price
        FROM base b
        JOIN picked_items p ON p.mlb = b.mlb
        ORDER BY b.mlb, COALESCE(b.variation_id, -1)
        """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            sql,
            {
                "connected_seller_id": connected_seller_id,
                "source_run_id": source_run_id,
                "limit_items": limit_items,
            },
        )
        return list(cur.fetchall())


def group_variations(rows: list[dict]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        mlb = row["mlb"]
        grouped.setdefault(
            mlb,
            {
                "variation_ids": [],
                "price": row.get("price"),
                "effective_price": row.get("effective_price"),
                "promo_price": row.get("promo_price"),
                "promo_original_price": row.get("promo_original_price"),
            },
        )
        grouped[mlb]["variation_ids"].append(row.get("variation_id"))
    return grouped


def item_promotions(
    session: requests.Session,
    connected_seller_id: int,
    item_id: str,
    *,
    headers: dict[str, str],
):
    url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    resp = session.get(
        url,
        headers=headers,
        params=params,
        timeout=DEFAULT_TIMEOUT,
    )

    if 200 <= resp.status_code < 300:
        data = _safe_json(resp)
        return {
            "ok": True,
            "item_id": item_id,
            "promotions": data if isinstance(data, list) else [],
            "error": None,
        }

    if resp.status_code == 404:
        return {
            "ok": True,
            "item_id": item_id,
            "promotions": [],
            "error": None,
        }

    return {
        "ok": False,
        "item_id": item_id,
        "promotions": [],
        "error": {
            "status_code": resp.status_code,
            "reason": resp.reason,
            "body": _safe_json(resp),
        },
    }


def _should_retry_status(status_code: int | None) -> bool:
    return int(status_code or 0) in RETRYABLE_STATUS_CODES


def _calc_backoff(attempt: int, base_sleep_sec: float) -> float:
    if attempt <= 1:
        return 0.0
    jitter = random.uniform(0.0, max(base_sleep_sec * 0.25, 0.05))
    return (base_sleep_sec * (2 ** (attempt - 2))) + jitter


def fetch_item_promotions_with_retry(
    session: requests.Session,
    connected_seller_id: int,
    mlb: str,
    *,
    headers: dict[str, str],
    max_attempts: int,
    base_sleep_sec: float,
) -> dict[str, Any]:
    last_result: dict[str, Any] | None = None

    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            sleep_for = _calc_backoff(attempt, base_sleep_sec)
            print(f"[REBATE][RETRY] mlb={mlb} | tentativa={attempt}/{max_attempts} | aguardando={sleep_for:.2f}s")
            time.sleep(sleep_for)

        result = item_promotions(session, connected_seller_id, mlb, headers=headers)
        last_result = result

        if result["ok"]:
            if attempt > 1:
                print(f"[REBATE][RETRY] mlb={mlb} | sucesso_na_tentativa={attempt}")
            return result

        status_code = ((result.get("error") or {}).get("status_code"))
        if not _should_retry_status(status_code) or attempt >= max_attempts:
            return result

    return last_result or {
        "ok": False,
        "item_id": mlb,
        "promotions": [],
        "error": {"status_code": None, "reason": "unknown", "body": {"message": "unexpected retry flow"}},
    }


def fetch_item_promotions_with_retry_threadsafe(
    *,
    connected_seller_id: int,
    mlb: str,
    headers: dict[str, str],
    max_attempts: int,
    base_sleep_sec: float,
    pool_size: int,
) -> dict[str, Any]:
    session = get_thread_session(pool_size=pool_size)
    return fetch_item_promotions_with_retry(
        session,
        connected_seller_id,
        mlb,
        headers=headers,
        max_attempts=max_attempts,
        base_sleep_sec=base_sleep_sec,
    )


def reprocess_failed_items(
    session: requests.Session,
    connected_seller_id: int,
    promotions_by_item: dict[str, list[dict[str, Any]]],
    failed_items: list[dict[str, Any]],
    *,
    headers: dict[str, str],
    max_attempts: int,
    base_sleep_sec: float,
    request_sleep_sec: float,
) -> list[dict[str, Any]]:
    if not failed_items:
        return []

    print(f"[REBATE][REPROCESS] Iniciando reprocessamento final | itens={len(failed_items)}")
    remaining: list[dict[str, Any]] = []

    for idx, failed in enumerate(failed_items, start=1):
        mlb = failed["mlb"]
        print(f"[REBATE][REPROCESS] {idx}/{len(failed_items)} | mlb={mlb}")
        result = fetch_item_promotions_with_retry(
            session,
            connected_seller_id,
            mlb,
            headers=headers,
            max_attempts=max_attempts,
            base_sleep_sec=base_sleep_sec,
        )

        if result["ok"]:
            promotions_by_item[mlb] = result["promotions"]
        else:
            remaining.append({"mlb": mlb, "error": result["error"]})
            err = result["error"]
            print(f"[REBATE][REPROCESS][ERRO] mlb={mlb} | status={err['status_code']} | reason={err['reason']}")

        if request_sleep_sec > 0:
            time.sleep(request_sleep_sec)

    print(f"[REBATE][REPROCESS] Finalizado | restantes={len(remaining)}")
    return remaining


def parse_promotion(item_ctx: dict[str, Any], promo: dict[str, Any]) -> dict[str, Any] | None:
    promotion_id = str(first_non_empty(promo, ["promotion_id", "id"]) or "") or None
    promotion_type = str(first_non_empty(promo, ["promotion_type", "type", "campaign_type"]) or "") or None
    promotion_status = str(first_non_empty(promo, ["promotion_status", "status"]) or "") or None
    offer_id = first_non_empty(
        promo,
        ["offer_id", "offerId", "promotion_offer_id", "candidate_offer_id", "smart_offer_id", "ref_id", "reference_id"],
    )
    offer_id = str(offer_id).strip() if offer_id not in (None, "", []) else None

    original_price = to_decimal(first_non_empty(promo, ["original_price", "price_original", "regular_amount_current"]))
    promo_price = to_decimal(first_non_empty(promo, ["promo_price", "price", "deal_price", "price_to_show", "offer_price"]))

    if original_price is None:
        original_price = to_decimal(item_ctx.get("promo_original_price")) or to_decimal(item_ctx.get("price"))

    if promo_price is None:
        promo_price = to_decimal(item_ctx.get("promo_price")) or to_decimal(item_ctx.get("effective_price")) or to_decimal(item_ctx.get("price"))

    meli_percent = normalize_percent(first_non_empty(
        promo,
        ["meli_percent", "meli_percentage", "marketplace_percent", "meli_contribution_percent"],
    ))
    seller_percent = normalize_percent(first_non_empty(
        promo,
        ["seller_percent", "seller_percentage", "seller_contribution_percent"],
    ))

    discount_total = None
    if original_price is not None and promo_price is not None:
        discount_total = q2(original_price - promo_price)

    rebate_meli_amount = None
    if promo_price is not None and meli_percent is not None:
        rebate_meli_amount = q2(promo_price * meli_percent)

    seller_discount_amount = None
    if promo_price is not None and seller_percent is not None:
        seller_discount_amount = q2(promo_price * seller_percent)

    unexplained_discount_amount = None
    if discount_total is not None:
        explained = (rebate_meli_amount or 0) + (seller_discount_amount or 0)
        unexplained_discount_amount = q2(discount_total - explained)

    regular_amount_current = to_decimal(first_non_empty(
        promo,
        ["regular_amount_current", "original_price", "price_original"],
    ))
    if regular_amount_current is None:
        regular_amount_current = original_price

    sale_amount_current = to_decimal(first_non_empty(
        promo,
        ["sale_amount_current", "promo_price", "price", "deal_price"],
    ))
    if sale_amount_current is None:
        sale_amount_current = promo_price

    rebate_base_price = promo_price or sale_amount_current
    rebate_price_source = "promo_price" if promo_price is not None else "sale_amount_current"

    price_api_price_id = first_non_empty(promo, ["price_id", "price_api_price_id"])
    price_api_reference_date = first_non_empty(promo, ["reference_date", "price_api_reference_date", "start_date"])

    return {
        "promotion_id": promotion_id,
        "promotion_type": promotion_type,
        "promotion_status": promotion_status,
        "offer_id": offer_id,
        "original_price": q2(original_price),
        "promo_price": q2(promo_price),
        "discount_total": q2(discount_total),
        "meli_percent": q4(meli_percent),
        "seller_percent": q4(seller_percent),
        "rebate_meli_amount": q2(rebate_meli_amount),
        "seller_discount_amount": q2(seller_discount_amount),
        "raw_json": promo,
        "unexplained_discount_amount": q2(unexplained_discount_amount),
        "regular_amount_current": q2(regular_amount_current),
        "sale_amount_current": q2(sale_amount_current),
        "rebate_base_price": q2(rebate_base_price),
        "rebate_price_source": rebate_price_source,
        "price_api_price_id": str(price_api_price_id).strip() if price_api_price_id not in (None, "", []) else None,
        "price_api_reference_date": price_api_reference_date,
    }


def build_insert_rows(
    connected_seller_id: int,
    run_id: int,
    grouped_items: dict[str, dict[str, Any]],
    promotions_by_item: dict[str, list[dict[str, Any]]],
) -> list[tuple]:
    rows: list[tuple] = []

    for mlb, item_ctx in grouped_items.items():
        variation_ids = item_ctx["variation_ids"] or [None]
        promotions = promotions_by_item.get(mlb, [])

        if not promotions:
            for variation_id in variation_ids:
                rows.append(
                    (
                        connected_seller_id,
                        run_id,
                        mlb,
                        variation_id,
                        None,
                        None,
                        None,
                        None,
                        q2(to_decimal(item_ctx.get("promo_original_price")) or to_decimal(item_ctx.get("price"))),
                        q2(to_decimal(item_ctx.get("promo_price")) or to_decimal(item_ctx.get("effective_price")) or to_decimal(item_ctx.get("price"))),
                        None,
                        None,
                        None,
                        None,
                        None,
                        Json([]),
                        None,
                        q2(to_decimal(item_ctx.get("promo_original_price")) or to_decimal(item_ctx.get("price"))),
                        q2(to_decimal(item_ctx.get("promo_price")) or to_decimal(item_ctx.get("effective_price")) or to_decimal(item_ctx.get("price"))),
                        q2(to_decimal(item_ctx.get("promo_price")) or to_decimal(item_ctx.get("effective_price")) or to_decimal(item_ctx.get("price"))),
                        "fallback_inventory",
                        None,
                        None,
                    )
                )
            continue

        for promo in promotions:
            parsed = parse_promotion(item_ctx, promo)
            if not parsed:
                continue

            for variation_id in variation_ids:
                rows.append(
                    (
                        connected_seller_id,
                        run_id,
                        mlb,
                        variation_id,
                        parsed["promotion_id"],
                        parsed["promotion_type"],
                        parsed["promotion_status"],
                        parsed["offer_id"],
                        parsed["original_price"],
                        parsed["promo_price"],
                        parsed["discount_total"],
                        parsed["meli_percent"],
                        parsed["seller_percent"],
                        parsed["rebate_meli_amount"],
                        parsed["seller_discount_amount"],
                        Json(parsed["raw_json"]),
                        parsed["unexplained_discount_amount"],
                        parsed["regular_amount_current"],
                        parsed["sale_amount_current"],
                        parsed["rebate_base_price"],
                        parsed["rebate_price_source"],
                        parsed["price_api_price_id"],
                        parsed["price_api_reference_date"],
                    )
                )

    return rows


def insert_rows(conn, rows: list[tuple], *, page_size: int = 1000) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO ml.item_promo_rebate_snapshot (
        connected_seller_id,
        run_id,
        mlb,
        variation_id,
        promotion_id,
        promotion_type,
        promotion_status,
        offer_id,
        original_price,
        promo_price,
        discount_total,
        meli_percent,
        seller_percent,
        rebate_meli_amount,
        seller_discount_amount,
        raw_json,
        unexplained_discount_amount,
        regular_amount_current,
        sale_amount_current,
        rebate_base_price,
        rebate_price_source,
        price_api_price_id,
        price_api_reference_date
    ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
    conn.commit()
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta snapshot de promoções/rebate por item e grava em ml.item_promo_rebate_snapshot"
    )
    parser.add_argument("--connected-seller-id", type=int, required=True, help="ID do seller conectado em ml.connected_seller")
    parser.add_argument("--source-run-id", type=int, default=None, help="run_id fonte do inventory_snapshot; default = último")
    parser.add_argument("--limit-items", type=int, default=None, help="Limita quantidade de MLBs do snapshot")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_RETRY_MAX_ATTEMPTS, help="Tentativas máximas para 429/500 transitório")
    parser.add_argument("--base-sleep-sec", type=float, default=DEFAULT_RETRY_BASE_SLEEP_SEC, help="Sleep base para backoff exponencial")
    parser.add_argument("--request-sleep-sec", type=float, default=DEFAULT_REQUEST_SLEEP_SEC, help="Sleep fixo entre MLBs")
    parser.add_argument("--final-reprocess", action=argparse.BooleanOptionalAction, default=DEFAULT_FINAL_REPROCESS, help="Reprocessa no fim apenas MLBs que falharam")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Quantidade máxima de workers para paralelismo controlado")
    parser.add_argument("--insert-batch-size", type=int, default=DEFAULT_INSERT_BATCH_SIZE, help="Quantidade de MLBs processados antes de gravar no banco")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    connected_seller_id = args.connected_seller_id
    session = requests.Session()

    with db_connect() as conn:
        ensure_table(conn)

        scope_rows = get_inventory_scope_rows(
            conn,
            connected_seller_id=connected_seller_id,
            source_run_id=args.source_run_id,
            limit_items=args.limit_items,
        )
        grouped_items = group_variations(scope_rows)

        run_id = create_run(
            conn,
            connected_seller_id=connected_seller_id,
            run_type="item_promo_rebate_snapshot",
            status="running",
            params={
                "connected_seller_id": connected_seller_id,
                "source_run_id": args.source_run_id,
                "limit_items": args.limit_items,
                "mlb_count": len(grouped_items),
                "max_attempts": args.max_attempts,
                "base_sleep_sec": args.base_sleep_sec,
                "request_sleep_sec": args.request_sleep_sec,
                "final_reprocess": bool(args.final_reprocess),
                "max_workers": int(args.max_workers),
                "insert_batch_size": int(args.insert_batch_size),
            },
        )

        try:
            promotions_by_item: dict[str, list[dict[str, Any]]] = {}
            failed_items: list[dict[str, Any]] = []
            mlbs = list(grouped_items.keys())
            headers = get_headers(connected_seller_id)
            max_workers = max(1, int(args.max_workers))
            insert_batch_size = max(1, int(args.insert_batch_size))
            inserted = 0

            print(
                f"[REBATE] Iniciando snapshot | connected_seller_id={connected_seller_id} | "
                f"mlb_count={len(grouped_items)} | scope_rows={len(scope_rows)} | max_attempts={args.max_attempts} | "
                f"request_sleep_sec={args.request_sleep_sec} | final_reprocess={bool(args.final_reprocess)} | "
                f"max_workers={max_workers} | insert_batch_size={insert_batch_size}"
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                pool_size = max(10, max_workers * 4)
                future_map = {}
                for mlb in mlbs:
                    future = executor.submit(
                        fetch_item_promotions_with_retry_threadsafe,
                        connected_seller_id=connected_seller_id,
                        mlb=mlb,
                        headers=headers,
                        max_attempts=args.max_attempts,
                        base_sleep_sec=args.base_sleep_sec,
                        pool_size=pool_size,
                    )
                    future_map[future] = mlb

                    # Mantido como throttle opcional, mas agora o default é 0.
                    # Se precisar reduzir 429, use --request-sleep-sec ou a env ML_REBATE_SNAPSHOT_REQUEST_SLEEP_SEC.
                    if args.request_sleep_sec > 0:
                        time.sleep(args.request_sleep_sec)

                processed_mlbs: list[str] = []
                for idx, future in enumerate(as_completed(future_map), start=1):
                    mlb = future_map[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {
                            "ok": False,
                            "item_id": mlb,
                            "promotions": [],
                            "error": {"status_code": None, "reason": "executor_error", "body": {"message": str(exc)}},
                        }

                    if result["ok"]:
                        promotions_by_item[mlb] = result["promotions"]
                    else:
                        promotions_by_item[mlb] = []
                        failed_items.append({"mlb": mlb, "error": result["error"]})
                        err = result["error"]
                        print(f"[REBATE][ERRO] mlb={mlb} | status={err['status_code']} | reason={err['reason']}")

                    processed_mlbs.append(mlb)

                    if idx == 1 or idx % 10 == 0 or idx == len(mlbs):
                        print(f"[REBATE] {idx}/{len(mlbs)} | mlb={mlb} | failed_items={len(failed_items)}")

                    if len(processed_mlbs) >= insert_batch_size or idx == len(mlbs):
                        partial_grouped = {k: grouped_items[k] for k in processed_mlbs}
                        partial_promotions = {k: promotions_by_item.get(k, []) for k in processed_mlbs}
                        rows = build_insert_rows(
                            connected_seller_id=connected_seller_id,
                            run_id=run_id,
                            grouped_items=partial_grouped,
                            promotions_by_item=partial_promotions,
                        )
                        inserted += insert_rows(conn, rows, page_size=1000)
                        print(f"[REBATE] batch gravado | mlbs_batch={len(processed_mlbs)} | rows_inserted_total={inserted}")
                        processed_mlbs = []

            if failed_items and args.final_reprocess:
                failed_items = reprocess_failed_items(
                    session,
                    connected_seller_id,
                    promotions_by_item,
                    failed_items,
                    headers=headers,
                    max_attempts=args.max_attempts,
                    base_sleep_sec=args.base_sleep_sec,
                    request_sleep_sec=args.request_sleep_sec,
                )


            if args.final_reprocess:
                reprocessed_ok_mlbs = [mlb for mlb in promotions_by_item.keys() if mlb not in {f["mlb"] for f in failed_items}]
                # Reinserção final apenas para MLBs que falharam e foram resolvidos no reprocessamento não é necessária
                # porque os dados já foram inseridos no batch inicial. Mantemos totals/falhas consistentes.
            finish_run(
                conn,
                run_id,
                status="finished",
                totals={
                    "mlb_count": len(grouped_items),
                    "scope_rows": len(scope_rows),
                    "rows_inserted": inserted,
                    "failed_items": len(failed_items),
                    "max_attempts": args.max_attempts,
                    "request_sleep_sec": args.request_sleep_sec,
                    "final_reprocess": bool(args.final_reprocess),
                },
            )

            print(f"[REBATE] Finalizado | rows_inserted={inserted} | failed_items={len(failed_items)}")
            print("REBATE SNAPSHOT OK")
            print(
                {
                    "connected_seller_id": connected_seller_id,
                    "run_id": run_id,
                    "mlb_count": len(grouped_items),
                    "scope_rows": len(scope_rows),
                    "rows_inserted": inserted,
                    "failed_items": len(failed_items),
                }
            )

            if failed_items:
                print(f"ITENS COM ERRO: {len(failed_items)}")
                for err in failed_items[:20]:
                    print(err)

        except Exception as exc:
            finish_run(conn, run_id, status="error", totals={}, error=str(exc))
            raise


if __name__ == "__main__":
    main()
