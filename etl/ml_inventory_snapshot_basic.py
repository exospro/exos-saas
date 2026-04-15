# -*- coding: utf-8 -*-
# Versão otimizada com paralelismo + SKU correto (user_product_id)

import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS_DEFAULT = 5
BATCH_SIZE = 50

def get_valid_access_token(connected_seller_id: int) -> str:
    raise NotImplementedError("Integrar com seu sistema")

def get_headers(connected_seller_id: int) -> dict:
    token = get_valid_access_token(connected_seller_id)
    return {"Authorization": f"Bearer {token}"}

def insert_rows(conn, rows: list) -> int:
    return len(rows)

def build_rows(connected_seller_id: int, run_id: int, items: list):
    rows = []
    for item in items:
        mlb = item.get("id")
        variations = item.get("variations") or []

        if variations:
            for var in variations:
                sku = var.get("seller_custom_field") or var.get("user_product_id")

                if not sku:
                    for attr in var.get("attributes", []) or []:
                        if (attr.get("id") or "").upper() == "SELLER_SKU":
                            sku = attr.get("value_name") or attr.get("value_id")
                            break

                rows.append({
                    "mlb": mlb,
                    "variation_id": var.get("id"),
                    "sku": sku,
                    "qty": var.get("available_quantity"),
                    "price": var.get("price"),
                })
        else:
            rows.append({
                "mlb": mlb,
                "variation_id": None,
                "sku": item.get("seller_custom_field"),
                "qty": item.get("available_quantity"),
                "price": item.get("price"),
            })
    return rows

def fetch_item_ids(session, headers, user_id, limit, max_items):
    url = f"https://api.mercadolibre.com/users/{user_id}/items/search"
    offset = 0
    results = []

    while True:
        resp = session.get(url, headers=headers, params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()

        ids = data.get("results", [])
        results.extend(ids)

        if not ids or (max_items and len(results) >= max_items):
            break

        offset += limit

    return results[:max_items] if max_items else results

def fetch_item_detail(session, headers, item_id):
    url = f"https://api.mercadolibre.com/items/{item_id}"
    resp = session.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connected-seller-id", type=int, required=True)
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument("--limit-items", type=int, default=None)
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS_DEFAULT)
    args = parser.parse_args()

    session = requests.Session()
    headers = get_headers(args.connected_seller_id)

    print("[START] buscando itens...")
    item_ids = fetch_item_ids(session, headers, args.user_id, args.page_size, args.limit_items)
    print(f"[INFO] total ids: {len(item_ids)}")

    inserted_total = 0
    batch_items = []

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(fetch_item_detail, session, headers, item_id): item_id for item_id in item_ids}

        for idx, future in enumerate(as_completed(futures), start=1):
            item = future.result()
            batch_items.append(item)

            if idx % 10 == 0:
                print(f"[PROGRESS] {idx}/{len(item_ids)}")

            if len(batch_items) >= BATCH_SIZE:
                rows = build_rows(args.connected_seller_id, 0, batch_items)
                inserted_total += insert_rows(None, rows)
                print(f"[BATCH] total_inserted={inserted_total}")
                batch_items = []

    if batch_items:
        rows = build_rows(args.connected_seller_id, 0, batch_items)
        inserted_total += insert_rows(None, rows)

    print(f"[DONE] total_inserted={inserted_total}")

if __name__ == "__main__":
    main()
