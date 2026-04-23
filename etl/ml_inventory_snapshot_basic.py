from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_HALF_UP
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
VARIATION_URL = "https://api.mercadolibre.com/items/{item_id}/variations/{variation_id}"
LISTING_PRICES_URL = "https://api.mercadolibre.com/sites/{site_id}/listing_prices"
SHIPPING_OPTIONS_URL = "https://api.mercadolibre.com/items/{item_id}/shipping_options"
SITE_ID = "MLB"
SHIP_ZIP = "04111080"
MAX_PAGE_SIZE = 100
DEFAULT_MAX_WORKERS = 5
DEFAULT_BATCH_SIZE = 50


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


def to_decimal(value: Any) -> Decimal | None:
    try:
        if value in (None, "", []):
            return None
        return Decimal(str(value))
    except Exception:
        return None


def q2(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def q6(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def safe_div(num: Decimal | None, den: Decimal | None) -> Decimal | None:
    if num is None or den in (None, Decimal("0")):
        return None
    return num / den


def first_non_empty(obj: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        val = obj.get(key)
        if val not in (None, "", []):
            return val
    return None

def make_json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [make_json_safe(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    return obj



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


def extract_sku_from_item(item: dict) -> str | None:
    scf = item.get("seller_custom_field")
    if scf:
        return scf

    for attr in item.get("attributes", []) or []:
        if (attr.get("id") or "").upper() == "SELLER_SKU":
            return attr.get("value_name") or attr.get("value_id")

    return None


def fetch_variation_detail(
    session: requests.Session,
    *,
    headers: dict[str, str],
    item_id: str,
    variation_id: int | str,
) -> dict[str, Any]:
    url = VARIATION_URL.format(item_id=item_id, variation_id=variation_id)
    resp = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def extract_real_variation_sku(variation_detail: dict[str, Any]) -> str | None:
    scf = variation_detail.get("seller_custom_field")
    if scf:
        return scf

    for attr in variation_detail.get("attributes", []) or []:
        if (attr.get("id") or "").upper() == "SELLER_SKU":
            return (
                attr.get("value_name")
                or attr.get("value_id")
                or ((attr.get("values") or [{}])[0].get("name"))
                or ((attr.get("values") or [{}])[0].get("id"))
            )

    return None


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
        resp = session.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)

        if not resp.ok:
            raise RuntimeError(
                f"Erro na paginação por offset do inventory. status={resp.status_code} "
                f"offset={offset} body={_safe_json(resp)}"
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
        params = {"search_type": "scan", "limit": page_size}
        if scroll_id:
            params["scroll_id"] = scroll_id

        resp = session.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)

        if not resp.ok:
            raise RuntimeError(
                f"Erro na paginação scan do inventory. status={resp.status_code} "
                f"scroll_id={scroll_id} body={_safe_json(resp)}"
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
    resp = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_listing_prices(
    session: requests.Session,
    *,
    headers: dict[str, str],
    category_id: str | None,
    listing_type_id: str | None,
    price: Any,
) -> dict[str, Any]:
    dec_price = q2(to_decimal(price))
    if dec_price is None or dec_price <= 0 or not category_id or not listing_type_id:
        return {}

    params = {
        "price": float(dec_price),
        "category_id": category_id,
        "listing_type_id": listing_type_id,
    }
    resp = session.get(LISTING_PRICES_URL.format(site_id=SITE_ID), headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
    if not resp.ok:
        print(
            f"[INVENTORY] falha ao buscar fee | category_id={category_id} listing_type_id={listing_type_id} "
            f"price={dec_price} status={resp.status_code}"
        )
        return {}
    data = _safe_json(resp)
    fee_amount = q2(to_decimal(data.get("sale_fee_amount")))
    fee_pct = q6(safe_div(fee_amount, dec_price)) if fee_amount is not None else None
    return {
        "fee_amount": fee_amount,
        "fee_pct": fee_pct,
        "raw": data,
    }


def pick_shipping_option(shipping_options: dict[str, Any]) -> dict[str, Any]:
    options = shipping_options.get("options") or []
    if not options:
        return {}

    def effective_shipping_cost(opt: dict[str, Any]) -> Decimal | None:
        list_cost = to_decimal(opt.get("list_cost"))
        if list_cost is not None and list_cost > 0:
            return list_cost

        base_cost = to_decimal(opt.get("base_cost"))
        if base_cost is not None and base_cost > 0:
            return base_cost

        cost = to_decimal(opt.get("cost"))
        if cost is not None and cost > 0:
            return cost

        return None

    fulfillment_options = [
        opt for opt in options
        if (opt.get("shipping_method_type") or "").lower() == "fulfillment"
        and effective_shipping_cost(opt) is not None
    ]
    if fulfillment_options:
        return min(fulfillment_options, key=lambda opt: effective_shipping_cost(opt) or Decimal("999999"))

    valid_options = [opt for opt in options if effective_shipping_cost(opt) is not None]
    if not valid_options:
        return {}

    return min(valid_options, key=lambda opt: effective_shipping_cost(opt) or Decimal("999999"))


def fetch_shipping_option(
    session: requests.Session,
    *,
    headers: dict[str, str],
    item_id: str,
) -> dict[str, Any]:
    if not item_id:
        return {}

    params = {"zip_code": SHIP_ZIP, "quantity": 1}
    resp = session.get(
        SHIPPING_OPTIONS_URL.format(item_id=item_id),
        headers=headers,
        params=params,
        timeout=DEFAULT_TIMEOUT,
    )
    if not resp.ok:
        print(f"[INVENTORY] falha ao buscar frete | mlb={item_id} status={resp.status_code} body={_safe_json(resp)}")
        return {}

    data = _safe_json(resp)
    chosen = pick_shipping_option(data)

    shipping_cost = to_decimal(chosen.get("list_cost"))
    if shipping_cost is None or shipping_cost <= 0:
        shipping_cost = to_decimal(chosen.get("base_cost"))
    if shipping_cost is None or shipping_cost <= 0:
        shipping_cost = to_decimal(chosen.get("cost"))

    return {
        "shipping_list_cost": q2(shipping_cost),
        "shipping_method_id": str(chosen.get("shipping_method_id")) if chosen.get("shipping_method_id") is not None else None,
        "shipping_method_type": chosen.get("shipping_method_type"),
        "raw": data,
    }


def parse_promotion_fields(item: dict[str, Any]) -> dict[str, Any]:
    original_price = q2(to_decimal(item.get("original_price")))
    price = q2(to_decimal(item.get("price")))
    sale_terms = item.get("sale_terms") or []

    promo_price = None
    promo_original_price = None
    promo_finish_date = None
    promo_status = None
    promo_active = None
    promo_type = None
    promo_campaign_type = None
    promo_id = None

    if original_price is not None and price is not None and original_price > price:
        promo_price = price
        promo_original_price = original_price
        promo_active = "active"
        promo_status = "active"

    for term in sale_terms:
        term_id = (term.get("id") or "").upper()
        if term_id in {"SALE_PRICE", "PROMOTIONAL_PRICE"}:
            promo_price = q2(to_decimal(first_non_empty(term, ["value_name", "value_id"]))) or promo_price
        elif term_id in {"START_TIME", "END_TIME", "FINISH_DATE"}:
            promo_finish_date = first_non_empty(term, ["value_name", "value_id"]) or promo_finish_date

    effective_price = promo_price or price
    discount_value = None
    discount_pct = None
    has_discount = False
    if promo_price is not None and promo_original_price is not None and promo_original_price > promo_price:
        has_discount = True
        discount_value = q2(promo_original_price - promo_price)
        discount_pct = q2((promo_original_price - promo_price) * Decimal("100") / promo_original_price)

    return {
        "has_discount": has_discount,
        "discount_value": discount_value,
        "discount_pct": discount_pct,
        "promo_active": promo_active,
        "promo_type": promo_type,
        "promo_campaign_type": promo_campaign_type,
        "promo_id": promo_id,
        "promo_status": promo_status,
        "promo_price": promo_price,
        "promo_original_price": promo_original_price,
        "promo_finish_date": promo_finish_date,
        "effective_price": effective_price,
    }


def enrich_item(
    session: requests.Session,
    *,
    headers: dict[str, str],
    item: dict[str, Any],
) -> dict[str, Any]:
    mlb = str(item.get("id") or "")
    price = item.get("price")
    effective_price = item.get("price")
    category_id = item.get("category_id")
    listing_type_id = item.get("listing_type_id")

    promo = parse_promotion_fields(item)
    effective_price = promo.get("effective_price") or effective_price

    fee_regular = fetch_listing_prices(
        session,
        headers=headers,
        category_id=category_id,
        listing_type_id=listing_type_id,
        price=price,
    )
    fee_effective = fetch_listing_prices(
        session,
        headers=headers,
        category_id=category_id,
        listing_type_id=listing_type_id,
        price=effective_price,
    )
    shipping = fetch_shipping_option(session, headers=headers, item_id=mlb)

    enriched = dict(item)
    enriched["__inventory_enriched__"] = {
        "fee_regular": {
            "fee_amount": fee_regular.get("fee_amount"),
            "fee_pct": fee_regular.get("fee_pct"),
        },
        "fee_effective": {
            "fee_amount": fee_effective.get("fee_amount"),
            "fee_pct": fee_effective.get("fee_pct"),
        },
        "shipping": {
            "shipping_list_cost": shipping.get("shipping_list_cost"),
            "shipping_method_id": shipping.get("shipping_method_id"),
            "shipping_method_type": shipping.get("shipping_method_type"),
        },
        "promotion": promo,
    }
    return enriched


def build_rows(
    *,
    session: requests.Session,
    headers: dict[str, str],
    connected_seller_id: int,
    run_id: int,
    items: list[dict[str, Any]],
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> list[tuple]:
    rows: list[tuple] = []
    pending_lookup: list[tuple[str, int]] = []

    for item in items:
        mlb = item.get("id")
        if not mlb:
            continue

        enriched = item.get("__inventory_enriched__") or {}
        promo = enriched.get("promotion") or {}
        fee_regular = enriched.get("fee_regular") or {}
        fee_effective = enriched.get("fee_effective") or {}
        shipping = enriched.get("shipping") or {}

        title = item.get("title")
        status = item.get("status")
        price = q2(to_decimal(item.get("price")))
        base_price = q2(to_decimal(item.get("base_price")))
        listing_type_id = item.get("listing_type_id")
        listing_kind = item.get("listing_strategy") or item.get("buying_mode")
        category_id = item.get("category_id")
        variations = item.get("variations") or []

        common_values = {
            "sku_source": "variation_detail" if variations else "item",
            "has_discount": bool(promo.get("has_discount")),
            "discount_value": promo.get("discount_value"),
            "discount_pct": promo.get("discount_pct"),
            "promo_active": promo.get("promo_active"),
            "promo_type": promo.get("promo_type"),
            "promo_campaign_type": promo.get("promo_campaign_type"),
            "promo_id": promo.get("promo_id"),
            "promo_status": promo.get("promo_status"),
            "promo_price": promo.get("promo_price"),
            "promo_original_price": promo.get("promo_original_price"),
            "promo_finish_date": promo.get("promo_finish_date"),
            "effective_price": q2(to_decimal(promo.get("effective_price"))) or price,
            "fee_amount_regular": fee_regular.get("fee_amount"),
            "fee_pct_regular": fee_regular.get("fee_pct"),
            "fee_amount_effective": fee_effective.get("fee_amount"),
            "fee_pct_effective": fee_effective.get("fee_pct"),
            "shipping_list_cost": shipping.get("shipping_list_cost"),
            "shipping_method_id": shipping.get("shipping_method_id"),
            "shipping_method_type": shipping.get("shipping_method_type"),
        }

        if variations:
            for var in variations:
                variation_id = var.get("id")
                stock = var.get("available_quantity")
                seller_sku = var.get("seller_custom_field")

                if not seller_sku:
                    for attr in var.get("attributes", []) or []:
                        if (attr.get("id") or "").upper() == "SELLER_SKU":
                            seller_sku = (
                                attr.get("value_name")
                                or attr.get("value_id")
                                or ((attr.get("values") or [{}])[0].get("name"))
                                or ((attr.get("values") or [{}])[0].get("id"))
                            )
                            break

                if variation_id is not None:
                    pending_lookup.append((str(mlb), int(variation_id)))

                rows.append(
                    (
                        connected_seller_id,
                        run_id,
                        mlb,
                        status,
                        variation_id,
                        seller_sku,
                        common_values["sku_source"],
                        stock,
                        price,
                        base_price,
                        common_values["has_discount"],
                        common_values["discount_value"],
                        common_values["discount_pct"],
                        common_values["promo_active"],
                        common_values["promo_type"],
                        common_values["promo_campaign_type"],
                        common_values["promo_id"],
                        common_values["promo_status"],
                        common_values["promo_price"],
                        common_values["promo_original_price"],
                        common_values["promo_finish_date"],
                        title,
                        listing_type_id,
                        listing_kind,
                        category_id,
                        common_values["effective_price"],
                        common_values["fee_amount_regular"],
                        common_values["fee_pct_regular"],
                        common_values["fee_amount_effective"],
                        common_values["fee_pct_effective"],
                        common_values["shipping_list_cost"],
                        common_values["shipping_method_id"],
                        common_values["shipping_method_type"],
                        Json(make_json_safe(item)),
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
                    common_values["sku_source"],
                    item.get("available_quantity"),
                    price,
                    base_price,
                    common_values["has_discount"],
                    common_values["discount_value"],
                    common_values["discount_pct"],
                    common_values["promo_active"],
                    common_values["promo_type"],
                    common_values["promo_campaign_type"],
                    common_values["promo_id"],
                    common_values["promo_status"],
                    common_values["promo_price"],
                    common_values["promo_original_price"],
                    common_values["promo_finish_date"],
                    title,
                    listing_type_id,
                    listing_kind,
                    category_id,
                    common_values["effective_price"],
                    common_values["fee_amount_regular"],
                    common_values["fee_pct_regular"],
                    common_values["fee_amount_effective"],
                    common_values["fee_pct_effective"],
                    common_values["shipping_list_cost"],
                    common_values["shipping_method_id"],
                    common_values["shipping_method_type"],
                    Json(make_json_safe(item)),
                )
            )

    if pending_lookup:
        lookup_map: dict[tuple[str, int], str | None] = {}
        with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
            future_map = {
                executor.submit(
                    fetch_variation_detail,
                    session,
                    headers=headers,
                    item_id=mlb,
                    variation_id=variation_id,
                ): (mlb, variation_id)
                for mlb, variation_id in pending_lookup
            }

            for future in as_completed(future_map):
                mlb, variation_id = future_map[future]
                try:
                    detail = future.result()
                    lookup_map[(mlb, variation_id)] = extract_real_variation_sku(detail)
                except Exception as exc:
                    print(f"[INVENTORY] falha ao buscar SKU real da variação | mlb={mlb} variation_id={variation_id} detalhe={exc}")
                    lookup_map[(mlb, variation_id)] = None

        fixed_rows: list[tuple] = []
        for row in rows:
            row_list = list(row)
            mlb = str(row_list[2])
            variation_id = row_list[4]
            if variation_id is not None:
                real_sku = lookup_map.get((mlb, int(variation_id)))
                if real_sku:
                    row_list[5] = real_sku
            fixed_rows.append(tuple(row_list))
        rows = fixed_rows

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
        sku_source,
        stock,
        price,
        base_price,
        has_discount,
        discount_value,
        discount_pct,
        promo_active,
        promo_type,
        promo_campaign_type,
        promo_id,
        promo_status,
        promo_price,
        promo_original_price,
        promo_finish_date,
        title,
        listing_type_id,
        listing_kind,
        category_id,
        effective_price,
        fee_amount_regular,
        fee_pct_regular,
        fee_amount_effective,
        fee_pct_effective,
        shipping_list_cost,
        shipping_method_id,
        shipping_method_type,
        raw_json
    ) VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=250)

    conn.commit()
    return len(rows)


def ensure_columns(conn) -> None:
    sql = """
    ALTER TABLE ml.inventory_snapshot_item
    ADD COLUMN IF NOT EXISTS raw_json JSONB,
    ADD COLUMN IF NOT EXISTS sku_source TEXT,
    ADD COLUMN IF NOT EXISTS has_discount BOOLEAN,
    ADD COLUMN IF NOT EXISTS discount_value NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS discount_pct NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS promo_active TEXT,
    ADD COLUMN IF NOT EXISTS promo_type TEXT,
    ADD COLUMN IF NOT EXISTS promo_campaign_type TEXT,
    ADD COLUMN IF NOT EXISTS promo_id TEXT,
    ADD COLUMN IF NOT EXISTS promo_status TEXT,
    ADD COLUMN IF NOT EXISTS promo_price NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS promo_original_price NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS promo_finish_date TEXT,
    ADD COLUMN IF NOT EXISTS listing_kind TEXT,
    ADD COLUMN IF NOT EXISTS effective_price NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS fee_amount_regular NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS fee_pct_regular NUMERIC(10,6),
    ADD COLUMN IF NOT EXISTS fee_amount_effective NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS fee_pct_effective NUMERIC(10,6),
    ADD COLUMN IF NOT EXISTS shipping_list_cost NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS shipping_method_id TEXT,
    ADD COLUMN IF NOT EXISTS shipping_method_type TEXT
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta snapshot de inventário do seller e grava em ml.inventory_snapshot_item com taxa e frete"
    )
    parser.add_argument("--connected-seller-id", type=int, required=True, help="ID do seller conectado em ml.connected_seller")
    parser.add_argument("--limit-items", type=int, default=None, help="Limita quantidade total de anúncios coletados")
    parser.add_argument("--page-size", type=int, default=100, help="Quantidade por página na busca de item ids. Máximo efetivo: 100")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Quantidade máxima de workers para paralelismo controlado")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Quantidade de itens processados por batch antes do insert")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    connected_seller_id = args.connected_seller_id
    session = requests.Session()

    with db_connect() as conn:
        ensure_columns(conn)
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
                "mode": "basic_scan_enriched",
                "ship_zip": SHIP_ZIP,
                "site_id": SITE_ID,
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

            #item_ids = ["MLB5510338384"]
            print(f"[INVENTORY] IDs encontrados: {len(item_ids)}")

            total_ids = len(item_ids)
            inserted = 0
            batch_items: list[dict[str, Any]] = []
            batch_size = max(1, int(args.batch_size))

            with ThreadPoolExecutor(max_workers=max(1, int(args.max_workers))) as executor:
                future_map = {
                    executor.submit(fetch_item_detail, session, connected_seller_id=connected_seller_id, headers=headers, item_id=item_id): item_id
                    for item_id in item_ids
                }

                for idx, future in enumerate(as_completed(future_map), start=1):
                    item_id = future_map[future]
                    try:
                        item = future.result()
                        item = enrich_item(session, headers=headers, item=item)
                        batch_items.append(item)
                    except Exception as exc:
                        print(f"[INVENTORY] falha ao buscar/enriquecer detalhe | idx={idx}/{total_ids} | mlb={item_id} | detalhe={exc}")
                        continue

                    if should_log_progress(idx, total_ids, step=10):
                        enr = item.get("__inventory_enriched__") or {}
                        ship = (enr.get("shipping") or {}).get("shipping_list_cost")
                        fee = (enr.get("fee_effective") or {}).get("fee_amount")
                        print(f"[INVENTORY] detalhe {idx}/{total_ids} | mlb={item_id} | fee={fee} | ship={ship}")

                    if len(batch_items) >= batch_size or idx == total_ids:
                        rows = build_rows(
                            session=session,
                            headers=headers,
                            connected_seller_id=connected_seller_id,
                            run_id=run_id,
                            items=batch_items,
                            max_workers=max(1, int(args.max_workers)),
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
                    "mode": "basic_scan_enriched",
                    "ship_zip": SHIP_ZIP,
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
                    "mode": "basic_scan_enriched",
                }
            )

        except Exception as exc:
            finish_run(conn, run_id, status="error", totals={}, error=str(exc))
            raise


if __name__ == "__main__":
    main()
