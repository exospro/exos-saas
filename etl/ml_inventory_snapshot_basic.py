from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

from etl.inventory.repository import create_run, db_connect, finish_run
from etl.ml_auth_db_multi import get_valid_access_token

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DEFAULT_TIMEOUT = 60
SEARCH_URL = "https://api.mercadolibre.com/users/{user_id}/items/search"
ITEM_URL = "https://api.mercadolibre.com/items/{item_id}"
MAX_PAGE_SIZE = 100


def get_headers(connected_seller_id: int) -> dict[str, str]:
    token = get_valid_access_token(connected_seller_id)
    return {"Authorization": f"Bearer {token}"}


def should_log_progress(index: int, total: int, step: int = 25) -> bool:
    return index == 1 or index % step == 0 or index == total



def _safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"_raw": (resp.text or "").strip()[:4000]}



def get_user_id_for_connected_seller(conn, connected_seller_id: int) -> int:
    sql = """
    SELECT ml_user_id
    FROM ml.connected_seller
    WHERE id = %s
      AND status = 'active'
    """
    with conn.cursor() as cur:
        cur.execute(sql, (connected_seller_id,))
        row = cur.fetchone()

    if not row:
        raise RuntimeError(
            f"connected_seller_id={connected_seller_id} não encontrado ou inativo em ml.connected_seller"
        )

    ml_user_id = row[0]
    if ml_user_id is None:
        raise RuntimeError(
            f"connected_seller_id={connected_seller_id} sem ml_user_id preenchido"
        )

    return int(ml_user_id)



def fetch_item_ids_offset(
    session: requests.Session,
    *,
    connected_seller_id: int,
    headers: dict[str, str],
    user_id: int,
    limit: int = 50,
    max_items: int | None = None,
) -> list[str]:
    results: list[str] = []
    offset = 0
    page_size = min(max(1, int(limit)), MAX_PAGE_SIZE)

    while True:
        url = SEARCH_URL.format(user_id=user_id)
        params = {"limit": page_size, "offset": offset}
        resp = session.get(
            url,
            headers=headers,
            params=params,
            timeout=DEFAULT_TIMEOUT,
        )

        if not resp.ok:
            raise RuntimeError(
                f"Erro na paginação por offset do inventory. "
                f"status={resp.status_code} offset={offset} body={_safe_json(resp)}"
            )

        data = _safe_json(resp)
        page_results = data.get("results") or []
        if not page_results:
            break

        results.extend([str(x) for x in page_results])

        if max_items is not None and len(results) >= max_items:
            return results[:max_items]

        paging = data.get("paging") or {}
        total = int(paging.get("total") or 0)
        offset += page_size

        if offset >= total:
            break

    return results



def fetch_item_ids_scan(
    session: requests.Session,
    *,
    connected_seller_id: int,
    headers: dict[str, str],
    user_id: int,
    limit: int = 100,
    max_items: int | None = None,
) -> list[str]:
    results: list[str] = []
    scroll_id: str | None = None
    page_size = min(max(1, int(limit)), MAX_PAGE_SIZE)

    while True:
        url = SEARCH_URL.format(user_id=user_id)
        params = {
            "search_type": "scan",
            "limit": page_size,
        }
        if scroll_id:
            params["scroll_id"] = scroll_id

        resp = session.get(
            url,
            headers=headers,
            params=params,
            timeout=DEFAULT_TIMEOUT,
        )

        if not resp.ok:
            raise RuntimeError(
                f"Erro na paginação scan do inventory. "
                f"status={resp.status_code} scroll_id={scroll_id} body={_safe_json(resp)}"
            )

        data = _safe_json(resp)
        page_results = data.get("results")

        if not page_results:
            break

        results.extend([str(x) for x in page_results])

        if max_items is not None and len(results) >= max_items:
            return results[:max_items]

        scroll_id = data.get("scroll_id")
        if not scroll_id:
            break

    return results



def fetch_item_ids(
    session: requests.Session,
    *,
    connected_seller_id: int,
    headers: dict[str, str],
    user_id: int,
    limit: int = 50,
    max_items: int | None = None,
) -> list[str]:
    """
    Usa search_type=scan para suportar sellers com mais de 1000 anúncios.
    A própria doc do Mercado Livre indica scan para passar de 1000 registros.
    Se scan falhar, faz fallback para paginação por offset.
    """
    try:
        return fetch_item_ids_scan(
            session,
            connected_seller_id=connected_seller_id,
            headers=headers,
            user_id=user_id,
            limit=limit,
            max_items=max_items,
        )
    except Exception as scan_exc:
        print(f"SCAN FALHOU, tentando OFFSET. detalhe={scan_exc}")
        return fetch_item_ids_offset(
            session,
            connected_seller_id=connected_seller_id,
            headers=headers,
            user_id=user_id,
            limit=limit,
            max_items=max_items,
        )



def fetch_item_detail(
    session: requests.Session,
    *,
    connected_seller_id: int,
    headers: dict[str, str],
    item_id: str,
) -> dict[str, Any]:
    url = ITEM_URL.format(item_id=item_id)
    resp = session.get(
        url,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()





def extract_sku_from_item(item: dict) -> str | None:
    scf = item.get("seller_custom_field")
    if scf:
        return scf

    for attr in item.get("attributes", []) or []:
        if (attr.get("id") or "").upper() == "SELLER_SKU":
            return attr.get("value_name") or attr.get("value_id")

    return None

def build_rows(
    *,
    connected_seller_id: int,
    run_id: int,
    items: list[dict[str, Any]],
) -> list[tuple]:
    rows: list[tuple] = []

    for item in items:
        mlb = item.get("id")
        if not mlb:
            continue

        title = item.get("title")
        status = item.get("status")
        price = item.get("price")
        base_price = item.get("base_price")
        listing_type_id = item.get("listing_type_id")
        category_id = item.get("category_id")

        variations = item.get("variations") or []

        if variations:
            for var in variations:
                variation_id = var.get("id")
                stock = var.get("available_quantity")
                seller_sku = var.get("seller_custom_field")

                if not seller_sku:
                    for attr in var.get("attributes", []) or []:
                        if (attr.get("id") or "").upper() == "SELLER_SKU":
                            seller_sku = attr.get("value_name") or attr.get("value_id")
                            break

                if not seller_sku:
                    seller_sku = extract_sku_from_item(item)

                rows.append(
                    (
                        connected_seller_id,
                        run_id,
                        mlb,
                        status,
                        variation_id,
                        seller_sku,
                        stock,
                        price,
                        base_price,
                        title,
                        listing_type_id,
                        category_id,
                        Json(item),
                    )
                )
        else:
            rows.append(
                (
                    connected_seller_id,
                    run_id,
                    mlb,
                    status,
                    None,
                    extract_sku_from_item(item),
                    item.get("available_quantity"),
                    price,
                    base_price,
                    title,
                    listing_type_id,
                    category_id,
                    Json(item),
                )
            )

    return rows



def insert_rows(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO ml.inventory_snapshot_item (
        connected_seller_id,
        run_id,
        mlb,
        status,
        variation_id,
        sku,
        stock,
        price,
        base_price,
        title,
        listing_type_id,
        category_id,
        raw_json
    ) VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)

    conn.commit()
    return len(rows)



def ensure_raw_json_column(conn) -> None:
    sql = """
    ALTER TABLE ml.inventory_snapshot_item
    ADD COLUMN IF NOT EXISTS raw_json JSONB
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta snapshot básico de inventário do seller e grava em ml.inventory_snapshot_item"
    )
    parser.add_argument(
        "--connected-seller-id",
        type=int,
        required=True,
        help="ID do seller conectado em ml.connected_seller",
    )
    parser.add_argument(
        "--limit-items",
        type=int,
        default=None,
        help="Limita quantidade total de anúncios coletados",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Quantidade por página na busca de item ids. Máximo efetivo: 100",
    )
    return parser.parse_args()



def main() -> None:
    args = parse_args()
    connected_seller_id = args.connected_seller_id

    session = requests.Session()

    with db_connect() as conn:
        ensure_raw_json_column(conn)

        user_id = get_user_id_for_connected_seller(conn, connected_seller_id)

        run_id = create_run(
            conn,
            connected_seller_id=connected_seller_id,
            run_type="inventory_snapshot",
            status="running",
            params={
                "connected_seller_id": connected_seller_id,
                "user_id": user_id,
                "page_size": min(max(1, int(args.page_size)), MAX_PAGE_SIZE),
                "limit_items": args.limit_items,
                "mode": "basic_scan",
            },
        )

        try:
            print(f"[INVENTORY] Iniciando snapshot | connected_seller_id={connected_seller_id} | user_id={user_id}")
            headers = get_headers(connected_seller_id)

            item_ids = fetch_item_ids(
                session,
                connected_seller_id=connected_seller_id,
                headers=headers,
                user_id=user_id,
                limit=args.page_size,
                max_items=args.limit_items,
            )
            print(f"[INVENTORY] IDs encontrados: {len(item_ids)}")

            total_ids = len(item_ids)
            inserted = 0
            batch_items: list[dict[str, Any]] = []
            batch_size = 50

            for idx, item_id in enumerate(item_ids, start=1):
                batch_items.append(
                    fetch_item_detail(
                        session,
                        connected_seller_id=connected_seller_id,
                        headers=headers,
                        item_id=item_id,
                    )
                )
                if should_log_progress(idx, total_ids, step=10):
                    print(f"[INVENTORY] detalhe {idx}/{total_ids} | mlb={item_id}")

                if len(batch_items) >= batch_size or idx == total_ids:
                    rows = build_rows(
                        connected_seller_id=connected_seller_id,
                        run_id=run_id,
                        items=batch_items,
                    )
                    inserted += insert_rows(conn, rows)
                    print(f"[INVENTORY] batch gravado | batch_items={len(batch_items)} | rows_inserted_total={inserted}")
                    batch_items = []

            finish_run(
                conn,
                run_id,
                status="finished",
                totals={
                    "item_ids_found": len(item_ids),
                    "rows_inserted": inserted,
                    "mode": "basic_scan",
                },
            )

            print(f"[INVENTORY] Finalizado | rows_inserted={inserted}")
            print("SNAPSHOT OK")
            print(
                {
                    "connected_seller_id": connected_seller_id,
                    "user_id": user_id,
                    "run_id": run_id,
                    "item_ids_found": len(item_ids),
                    "rows_inserted": inserted,
                    "mode": "basic_scan",
                }
            )

        except Exception as exc:
            finish_run(
                conn,
                run_id,
                status="error",
                totals={},
                error=str(exc),
            )
            raise


if __name__ == "__main__":
    main()
