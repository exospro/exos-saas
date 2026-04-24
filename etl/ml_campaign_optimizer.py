#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import requests
from psycopg2.extras import Json, RealDictCursor, execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from etl.inventory.repository import db_connect, create_run, finish_run
from etl.ml_auth_db_multi import get_headers


APP_VERSION = "v2"
DEFAULT_THREADS = int(os.environ.get("ML_CAMPAIGN_OPT_THREADS", "8"))
DEFAULT_RPS = float(os.environ.get("ML_CAMPAIGN_OPT_RPS", "6"))
DEFAULT_TIMEOUT = int(os.environ.get("ML_CAMPAIGN_OPT_TIMEOUT_SEC", "60"))
DEFAULT_MIN_MARGIN_PCT = float(os.environ.get("ML_CAMPAIGN_OPT_MIN_MARGIN_PCT", "0.08"))
DEFAULT_TAX_PCT = float(os.environ.get("ML_CAMPAIGN_OPT_TAX_PCT", "0.09"))
DEFAULT_MAX_MELI_REBATE_PCT = float(os.environ.get("ML_CAMPAIGN_OPT_MAX_MELI_REBATE_PCT", "0.30"))
DEFAULT_FLUSH_EVERY = int(os.environ.get("ML_CAMPAIGN_OPT_FLUSH_EVERY", "500"))
DEFAULT_USE_COST = os.environ.get("ML_CAMPAIGN_OPT_USE_COST", "false").strip().lower() in {"1", "true", "yes", "y", "on"}
DEFAULT_INSERT_PAGE_SIZE = int(os.environ.get("ML_CAMPAIGN_OPT_INSERT_PAGE_SIZE", "1000"))
SITE_ID = os.environ.get("ML_SITE_ID", "MLB")
SHIP_ZIP = os.environ.get("ML_SHIP_ZIP", "04111080")

CSV_HEADERS = {
    "mlb": "MLB",
    "sku": "SKU",
    "title": "Título",
    "status": "Status",
    "listing_type_label": "Tipo anúncio",
    "current_price": "Preço atual",
    "shipping_cost": "Frete",
    "fee_amount_current": "Tarifa atual",
    "current_receive": "Recebimento atual",
    "candidate_promotion_name": "Campanha sugerida",
    "candidate_promotion_type": "Tipo da campanha",
    "candidate_price": "Preço sugerido",
    "candidate_shipping_cost": "Frete sugerido",
    "candidate_rebate_meli_amount": "Rebate ML",
    "fee_amount_candidate": "Tarifa sugerida",
    "candidate_receive_estimated": "Recebimento estimado",
    "sku_min_value_used": "Mínimo do SKU",
    "delta_receive": "Diferença de recebimento",
    "action": "Ação",
    "execution_status": "Status execução",
    "reason": "Motivo",
}


ACTION_LABELS = {
    "SKIP": "Manter como está",
    "SWITCH": "Alterar campanha",
}

EXECUTION_STATUS_LABELS = {
    "not_applicable": "Não aplicável",
    "already_started_or_pending": "Já está em campanha",
    "dry_run": "Simulação",
    "success": "Aplicado com sucesso",
    "error": "Erro",
    "campaign_approved": "Campanha aprovada",
    "campaign_rejected": "Campanha rejeitada",
}

REASON_LABELS = {
    "no_better_candidate": "Nenhuma campanha melhor encontrada",
    "already_in_campaign": "Produto já está nessa campanha",
    "eligible_candidate": "Campanha elegível",
    "candidate_receive_below_sku_minimum": "Recebimento abaixo do mínimo do SKU",
    "candidate_net_result_below_current": "Recebimento menor que o atual",
    "candidate_price_not_lower_than_current": "Preço sugerido não é menor que o atual",
    "same_as_current_promotion": "É a mesma campanha atual",
    "normalize_candidate_promotion_rejected": "Campanha incompatível com as regras",
    "candidate_price_or_margin_missing": "Preço ou margem da campanha ausente",
    "candidate_net_result_missing": "Recebimento estimado ausente",
    "candidate_margin_missing_after_recalc": "Margem da campanha ausente após recálculo",
    "candidate_margin_below_current": "Margem menor que a atual",
    "candidate_margin_below_minimum": "Margem abaixo da mínima",
    "current_price_missing": "Preço atual ausente",
    "current_margin_missing": "Margem atual ausente",
    "current_net_result_missing": "Recebimento atual ausente",
    "missing_cost_mapping": "SKU sem mapeamento de custo",
    "missing_join_key_cost": "Custo do fornecedor ausente",
    "missing_cost_product": "Custo do produto ausente",
    "max_switch_reached": "Limite de alterações atingido",
}


def label_for_csv(mapping: dict[str, str], value: Any) -> Any:
    if value in (None, ""):
        return value
    text = str(value)
    return mapping.get(text, text)


LOG_TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.campaign_optimizer_run_item (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    connected_seller_id bigint NOT NULL,
    run_id bigint,
    mlb text NOT NULL,
    variation_id bigint,
    sku text,
    title text,
    status text,
    current_promotion_id text,
    current_promotion_type text,
    candidate_promotion_id text,
    candidate_promotion_type text,
    candidate_start_date timestamptz,
    current_price numeric(18,2),
    candidate_price numeric(18,2),
    current_margin_pct numeric(12,6),
    candidate_margin_pct numeric(12,6),
    current_mc numeric(18,2),
    candidate_mc numeric(18,2),
    current_rebate_meli_amount numeric(18,2),
    candidate_rebate_meli_amount numeric(18,2),
    cost_product numeric(18,2),
    shipping_cost numeric(18,2),
    fee_amount_current numeric(18,2),
    fee_amount_candidate numeric(18,2),
    fee_pct_effective numeric(12,6),
    tax_pct numeric(12,6),
    tax_amount_current numeric(18,2),
    tax_amount_candidate numeric(18,2),
    action text,
    execution_status text,
    reason text,
    dry_run boolean NOT NULL DEFAULT false,
    raw_current jsonb,
    raw_candidate jsonb,
    raw_decision jsonb,
    raw_api_result jsonb
);

CREATE INDEX IF NOT EXISTS ix_campaign_optimizer_run_item_run_id
    ON ml.campaign_optimizer_run_item (run_id);
CREATE INDEX IF NOT EXISTS ix_campaign_optimizer_run_item_mlb
    ON ml.campaign_optimizer_run_item (mlb);
CREATE INDEX IF NOT EXISTS ix_campaign_optimizer_run_item_action
    ON ml.campaign_optimizer_run_item (action);
CREATE INDEX IF NOT EXISTS ix_campaign_optimizer_run_item_connected_seller_id
    ON ml.campaign_optimizer_run_item (connected_seller_id);
"""


@dataclass
class ScopeItem:
    mlb: str
    variation_id: int | None
    sku: str | None
    title: str | None
    status: str | None
    listing_type_id: str | None
    category_id: str | None
    price: Decimal | None
    effective_price: Decimal | None
    fee_amount_effective: Decimal | None
    fee_pct_effective: Decimal | None
    shipping_list_cost: Decimal | None
    current_promotion_id: str | None
    current_promotion_type: str | None
    regular_amount_current: Decimal | None
    sale_amount_current: Decimal | None
    promo_price: Decimal | None
    original_price: Decimal | None
    rebate_meli_amount: Decimal | None
    meli_percent: Decimal | None
    seller_percent: Decimal | None
    cost_product: Decimal | None
    cost_detail: str | None
    cost_missing_mapping: bool = False
    cost_missing_price: bool = False


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


def normalize_percent(value: Any) -> Decimal | None:
    dec = to_decimal(value)
    if dec is None:
        return None
    return dec / Decimal("100") if dec > 1 else dec


def safe_div(num: Decimal | None, den: Decimal | None) -> Decimal | None:
    if num is None or den in (None, Decimal("0")):
        return None
    return num / den


def first_non_empty(obj: dict, keys: list[str]):
    for key in keys:
        val = obj.get(key)
        if val not in (None, "", []):
            return val
    return None


def first_positive_decimal(obj: dict, keys: list[str]) -> Decimal | None:
    for key in keys:
        val = to_decimal(obj.get(key))
        if val is not None and val > 0:
            return val
    return None


def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return {"_raw": (resp.text or "").strip()[:4000]}




def build_auth_headers(connected_seller_id: int) -> dict[str, str]:
    return get_headers(connected_seller_id)

def _mk_session(insecure: bool = False) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.verify = not insecure
    return s


class RateLimiter:
    def __init__(self, rps: float):
        self.rps = float(rps)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = now + (1.0 / self.rps)


def ensure_log_table(conn):
    with conn.cursor() as cur:
        cur.execute(LOG_TABLE_DDL)


def insert_log_rows(conn, rows: list[dict], *, page_size: int = DEFAULT_INSERT_PAGE_SIZE) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO ml.campaign_optimizer_run_item (
        connected_seller_id,
        run_id,
        mlb,
        variation_id,
        sku,
        title,
        status,
        current_promotion_id,
        current_promotion_type,
        candidate_promotion_id,
        candidate_promotion_type,
        candidate_start_date,
        current_price,
        candidate_price,
        current_margin_pct,
        candidate_margin_pct,
        current_mc,
        candidate_mc,
        current_rebate_meli_amount,
        candidate_rebate_meli_amount,
        cost_product,
        shipping_cost,
        fee_amount_current,
        fee_amount_candidate,
        fee_pct_effective,
        tax_pct,
        tax_amount_current,
        tax_amount_candidate,
        action,
        execution_status,
        reason,
        dry_run,
        raw_current,
        raw_candidate,
        raw_decision,
        raw_api_result
    ) VALUES %s
    """

    values = [
        (
            r.get("connected_seller_id"),
            r.get("run_id"),
            r.get("mlb"),
            r.get("variation_id"),
            r.get("sku"),
            r.get("title"),
            r.get("status"),
            r.get("current_promotion_id"),
            r.get("current_promotion_type"),
            r.get("candidate_promotion_id"),
            r.get("candidate_promotion_type"),
            r.get("candidate_start_date"),
            r.get("current_price"),
            r.get("candidate_price"),
            r.get("current_margin_pct"),
            r.get("candidate_margin_pct"),
            r.get("current_mc"),
            r.get("candidate_mc"),
            r.get("current_rebate_meli_amount"),
            r.get("candidate_rebate_meli_amount"),
            r.get("cost_product"),
            r.get("shipping_cost"),
            r.get("fee_amount_current"),
            r.get("fee_amount_candidate"),
            r.get("fee_pct_effective"),
            r.get("tax_pct"),
            r.get("tax_amount_current"),
            r.get("tax_amount_candidate"),
            r.get("action"),
            r.get("execution_status"),
            r.get("reason"),
            bool(r.get("dry_run", False)),
            Json(r.get("raw_current")) if r.get("raw_current") is not None else None,
            Json(r.get("raw_candidate")) if r.get("raw_candidate") is not None else None,
            Json(r.get("raw_decision")) if r.get("raw_decision") is not None else None,
            Json(r.get("raw_api_result")) if r.get("raw_api_result") is not None else None,
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=page_size)
    return len(rows)


def fetch_scope_rows(conn, connected_seller_id: int, source_run_id: int | None = None, limit: int | None = None) -> list[dict]:
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
    promo_run AS (
        SELECT (
            SELECT max(id)
            FROM ml.run
            WHERE connected_seller_id = %(connected_seller_id)s
              AND run_type = 'item_promo_rebate_snapshot'
        ) AS run_id
    ),
    inv AS (
        SELECT DISTINCT ON (i.mlb, COALESCE(i.variation_id, -1))
            i.run_id,
            i.mlb,
            i.variation_id,
            i.sku,
            i.title,
            i.status,
            i.listing_type_id,
            i.category_id,
            i.price,
            i.effective_price,
            i.fee_amount_effective,
            i.fee_pct_effective,
            i.shipping_list_cost,
            i.promo_id,
            i.promo_campaign_type,
            i.promo_type,
            i.promo_price,
            i.promo_original_price,
            i.collected_at,
            i.id
        FROM ml.inventory_snapshot_item i
        JOIN inv_run r ON r.run_id = i.run_id
        WHERE i.connected_seller_id = %(connected_seller_id)s
          AND COALESCE(i.status, '') = 'active'
          AND i.mlb IS NOT NULL
        ORDER BY i.mlb, COALESCE(i.variation_id, -1), i.collected_at DESC, i.id DESC
    ),
    promo AS (
        SELECT DISTINCT ON (p.mlb, COALESCE(p.variation_id, -1))
            p.mlb,
            p.variation_id,
            p.promotion_id,
            p.promotion_type,
            p.original_price,
            p.promo_price,
            p.meli_percent,
            p.seller_percent,
            p.rebate_meli_amount,
            p.seller_discount_amount,
            p.sale_amount_current,
            p.regular_amount_current,
            p.raw_json
        FROM ml.item_promo_rebate_snapshot p
        JOIN promo_run pr ON pr.run_id = p.run_id
        WHERE p.connected_seller_id = %(connected_seller_id)s
        ORDER BY p.mlb, COALESCE(p.variation_id, -1), p.id DESC
    )
    SELECT
        inv.*,
        promo.promotion_id AS rebate_promotion_id,
        promo.promotion_type AS rebate_promotion_type,
        promo.original_price AS rebate_original_price,
        promo.promo_price AS rebate_promo_price,
        promo.meli_percent,
        promo.seller_percent,
        promo.rebate_meli_amount,
        promo.seller_discount_amount,
        promo.sale_amount_current,
        promo.regular_amount_current,
        promo.raw_json AS rebate_raw_json
    FROM inv
    LEFT JOIN promo
      ON promo.mlb = inv.mlb
     AND COALESCE(promo.variation_id, -1) = COALESCE(inv.variation_id, -1)
    ORDER BY inv.mlb, COALESCE(inv.variation_id, -1)
    """
    if limit and limit > 0:
        sql += "\nLIMIT %(limit)s"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            sql,
            {
                "connected_seller_id": connected_seller_id,
                "source_run_id": source_run_id,
                "limit": limit,
            },
        )
        return list(cur.fetchall())


def fetch_bom_components(conn) -> dict[str, list[tuple[str, Decimal]]]:
    sql = """
    SELECT parent_master_sku, component_master_sku, component_qty
    FROM catalog.master_sku_bom
    """
    out: dict[str, list[tuple[str, Decimal]]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for parent_sku, component_sku, component_qty in cur.fetchall():
            out.setdefault(parent_sku, []).append((component_sku, to_decimal(component_qty) or Decimal("0")))
    return out


def fetch_master_sku_to_join_key(conn) -> dict[str, str]:
    sql = """
    SELECT DISTINCT ON (master_sku) master_sku, join_key
    FROM supplier.join_key_master_sku_map
    ORDER BY master_sku, updated_at DESC
    """
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for master_sku, join_key in cur.fetchall():
            out[master_sku] = join_key
    return out


def fetch_join_key_to_cost(conn) -> dict[str, Decimal]:
    sql = """
    WITH sup_run AS (
        SELECT max(id) AS run_id FROM supplier.scrape_run
    )
    SELECT DISTINCT ON (join_key) join_key, price_discount
    FROM supplier.scrape_item, sup_run
    WHERE supplier.scrape_item.run_id = sup_run.run_id
    ORDER BY join_key, id DESC
    """
    out: dict[str, Decimal] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for join_key, price_discount in cur.fetchall():
            out[join_key] = to_decimal(price_discount) or Decimal("0")
    return out


def build_scope_items(conn, connected_seller_id: int, source_run_id: int | None = None, limit: int | None = None) -> list[ScopeItem]:
    rows = fetch_scope_rows(conn, connected_seller_id=connected_seller_id, source_run_id=source_run_id, limit=limit)
    if not rows:
        return []

    bom_map = fetch_bom_components(conn)
    sku_to_jk = fetch_master_sku_to_join_key(conn)
    jk_to_cost = fetch_join_key_to_cost(conn)

    items: list[ScopeItem] = []
    for row in rows:
        sku = row.get("sku")
        total_cost = Decimal("0")
        details: list[str] = []
        cost_missing_mapping = False
        cost_missing_price = False

        if sku:
            components = bom_map.get(sku, [(sku, Decimal("1"))])
            for component_sku, component_qty in components:
                join_key = sku_to_jk.get(component_sku)
                if not join_key:
                    cost_missing_mapping = True
                    details.append(f"{component_sku}(Sem Mapeamento)")
                    continue
                unit_cost = jk_to_cost.get(join_key)
                if unit_cost is None:
                    cost_missing_price = True
                    details.append(f"{component_sku}(JK sem custo)")
                    continue
                line_cost = unit_cost * component_qty
                total_cost += line_cost
                details.append(f"{component_sku}({component_qty}x{unit_cost})")

        current_promotion_id = row.get("promo_id") or row.get("rebate_promotion_id")
        current_promotion_type = row.get("promo_campaign_type") or row.get("promo_type") or row.get("rebate_promotion_type")

        items.append(
            ScopeItem(
                mlb=row["mlb"],
                variation_id=row.get("variation_id"),
                sku=sku,
                title=row.get("title"),
                status=row.get("status"),
                listing_type_id=row.get("listing_type_id"),
                category_id=row.get("category_id"),
                price=to_decimal(row.get("price")),
                effective_price=to_decimal(row.get("effective_price")),
                fee_amount_effective=to_decimal(row.get("fee_amount_effective")),
                fee_pct_effective=normalize_percent(row.get("fee_pct_effective")) or Decimal("0"),
                shipping_list_cost=to_decimal(row.get("shipping_list_cost")) or Decimal("0"),
                current_promotion_id=current_promotion_id,
                current_promotion_type=current_promotion_type,
                regular_amount_current=to_decimal(row.get("regular_amount_current")),
                sale_amount_current=to_decimal(row.get("sale_amount_current")),
                promo_price=to_decimal(row.get("rebate_promo_price") or row.get("promo_price")),
                original_price=to_decimal(row.get("rebate_original_price") or row.get("promo_original_price")),
                rebate_meli_amount=to_decimal(row.get("rebate_meli_amount")) or Decimal("0"),
                meli_percent=normalize_percent(row.get("meli_percent")),
                seller_percent=normalize_percent(row.get("seller_percent")),
                cost_product=q2(total_cost),
                cost_detail=" + ".join(details) if details else None,
                cost_missing_mapping=cost_missing_mapping,
                cost_missing_price=cost_missing_price,
            )
        )

    return items


def item_promotions(
    session: requests.Session,
    connected_seller_id: int,
    item_id: str,
    *,
    auth_headers: dict[str, str],
):
    url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    resp = session.get(
        url,
        headers=auth_headers,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if 200 <= resp.status_code < 300:
        data = _safe_json(resp)
        return data if isinstance(data, list) else []
    if resp.status_code == 404:
        return []
    raise requests.HTTPError(f"{resp.status_code} {resp.reason} - {_safe_json(resp)}")


def fetch_listing_fee(
    session: requests.Session,
    price: Decimal | None,
    category_id: str | None,
    listing_type_id: str | None,
    *,
    auth_headers: dict[str, str],
) -> dict:
    if price is None or price <= 0 or not category_id or not listing_type_id:
        return {"fee_amount": None, "fee_pct": None}

    params = {
        "price": float(q2(price)),
        "category_id": category_id,
        "listing_type_id": listing_type_id,
    }
    resp = session.get(
        f"https://api.mercadolibre.com/sites/{SITE_ID}/listing_prices",
        headers=auth_headers,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if not (200 <= resp.status_code < 300):
        raise requests.HTTPError(f"{resp.status_code} {resp.reason} - {_safe_json(resp)}")

    data = _safe_json(resp)
    fee_amount = to_decimal(data.get("sale_fee_amount"))
    fee_pct = None
    if fee_amount is not None and price > 0:
        fee_pct = q6(fee_amount / price)
    return {"fee_amount": q2(fee_amount), "fee_pct": fee_pct}


def _shipping_effective_cost(opt: dict) -> Decimal | None:
    for key in ("list_cost", "base_cost", "cost"):
        val = to_decimal(opt.get(key))
        if val is not None and val > 0:
            return val
    return None


def pick_shipping_option(shipping_options: dict) -> dict:
    options = shipping_options.get("options") or []
    if not options:
        return {}

    def effective(opt: dict):
        return _shipping_effective_cost(opt)

    # 1) PRIORIDADE: SLOW
    slow_options = [
        opt for opt in options
        if (opt.get("shipping_method_type") or "").lower() == "slow"
        and effective(opt) is not None
    ]
    if slow_options:
        return sorted(slow_options, key=lambda o: effective(o) or Decimal("999999"))[0]

    # 2) fallback: qualquer opção válida
    valid_options = [opt for opt in options if effective(opt) is not None]
    if valid_options:
        return sorted(valid_options, key=lambda o: effective(o) or Decimal("999999"))[0]

    return {}



def fetch_shipping_option(
    session: requests.Session,
    item_id: str,
    *,
    auth_headers: dict[str, str],
) -> dict:
    if not item_id:
        return {"shipping_list_cost": None, "shipping_method_id": None, "shipping_method_type": None}

    params = {"zip_code": SHIP_ZIP, "quantity": 1}
    resp = session.get(
        f"https://api.mercadolibre.com/items/{item_id}/shipping_options",
        headers=auth_headers,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if not (200 <= resp.status_code < 300):
        raise requests.HTTPError(f"{resp.status_code} {resp.reason} - {_safe_json(resp)}")

    data = _safe_json(resp)
    chosen = pick_shipping_option(data)
    shipping_cost = _shipping_effective_cost(chosen)
    return {
        "shipping_list_cost": q2(shipping_cost),
        "shipping_method_id": chosen.get("shipping_method_id"),
        "shipping_method_type": chosen.get("shipping_method_type"),
    }


def fetch_sku_min_receive_map(conn, connected_seller_id: int) -> dict[str, dict[str, Decimal | None]]:
    sql = """
    SELECT
        smr.sku,
        smr.min_receive_classico,
        smr.min_receive_premium
    FROM app.account_sku_min_receive smr
    JOIN ml.connected_seller cs
      ON cs.account_id = smr.account_id
    WHERE cs.id = %s
    """
    out: dict[str, dict[str, Decimal | None]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (connected_seller_id,))
        for sku, min_classico, min_premium in cur.fetchall():
            if sku in (None, ""):
                continue
            out[str(sku).strip()] = {
                "min_receive_classico": to_decimal(min_classico),
                "min_receive_premium": to_decimal(min_premium),
            }
    return out


def _extract_error_code(err: dict) -> str | None:
    if not isinstance(err, dict):
        return None
    cause = err.get("cause")
    if isinstance(cause, list) and cause and isinstance(cause[0], dict) and cause[0].get("error_code"):
        return str(cause[0]["error_code"])
    msg = err.get("message") or ""
    if isinstance(msg, str) and "Errors:" in msg:
        m = msg.split("Errors:", 1)[1].strip()
        if "-" in m:
            return m.split("-", 1)[0].strip()
    return err.get("error")


def normalize_offer_id(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    text = str(value).strip()
    return text or None


def campaign_item_post(session: requests.Session, connected_seller_id: int, campaign_id: str, item_id: str, deal_price: Decimal,
                       promotion_type: str = "SELLER_CAMPAIGN", offer_id: str | None = None, *,
                       auth_headers: dict[str, str]):
    base_url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    payload = {
        "promotion_id": campaign_id,
        "promotion_type": promotion_type,
        "deal_price": float(q2(deal_price) or Decimal("0")),
    }
    if offer_id:
        payload["offer_id"] = offer_id
    headers = {**auth_headers, "Content-Type": "application/json"}
    resp = session.post(base_url, headers=headers, json=payload, params=params,
                        timeout=DEFAULT_TIMEOUT, verify=session.verify)
    if 200 <= resp.status_code < 300:
        return {"ok": True, "method": "POST", "status_code": resp.status_code, "body": _safe_json(resp)}
    return {"ok": False, "method": "POST", "status_code": resp.status_code, "error": _safe_json(resp)}


def campaign_item_put(session: requests.Session, connected_seller_id: int, campaign_id: str, item_id: str, deal_price: Decimal,
                      promotion_type: str = "SELLER_CAMPAIGN", offer_id: str | None = None, *,
                      auth_headers: dict[str, str]):
    base_url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    payload = {
        "promotion_id": campaign_id,
        "promotion_type": promotion_type,
        "deal_price": float(q2(deal_price) or Decimal("0")),
    }
    if offer_id:
        payload["offer_id"] = offer_id
    headers = {**auth_headers, "Content-Type": "application/json"}
    resp = session.put(base_url, headers=headers, json=payload, params=params,
                       timeout=DEFAULT_TIMEOUT, verify=session.verify)
    if 200 <= resp.status_code < 300:
        return {"ok": True, "method": "PUT", "status_code": resp.status_code, "body": _safe_json(resp)}
    return {"ok": False, "method": "PUT", "status_code": resp.status_code, "error": _safe_json(resp)}


def campaign_item_upsert(session: requests.Session, connected_seller_id: int, campaign_id: str, item_id: str, deal_price: Decimal,
                         promotion_type: str = "SELLER_CAMPAIGN", offer_id: str | None = None, *,
                         auth_headers: dict[str, str]) -> dict:
    if str(promotion_type).upper() == "SMART" and not offer_id:
        return {
            "ok": False,
            "method": "POST",
            "status_code": None,
            "error": {"message": "SMART promotion requires offer_id"},
            "error_code": "smart_missing_offer_id",
        }

    r1 = campaign_item_post(
        session, connected_seller_id, campaign_id, item_id, deal_price,
        promotion_type=promotion_type, offer_id=offer_id, auth_headers=auth_headers
    )
    if r1.get("ok"):
        return {**r1, "error_code": None}
    r2 = campaign_item_put(
        session, connected_seller_id, campaign_id, item_id, deal_price,
        promotion_type=promotion_type, offer_id=offer_id, auth_headers=auth_headers
    )
    if r2.get("ok"):
        return {**r2, "error_code": None}
    err = r2.get("error") or r1.get("error") or {}
    return {
        "ok": False,
        "method": r2.get("method") or r1.get("method") or "PUT",
        "status_code": r2.get("status_code") or r1.get("status_code"),
        "error": err,
        "error_code": _extract_error_code(err),
    }


def parse_start_date(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def current_price_for_comparison(item: ScopeItem) -> Decimal | None:
    for value in (item.sale_amount_current, item.effective_price, item.promo_price, item.price):
        if value is not None and value > 0:
            return q2(value)
    return None


def calculate_financials(price: Decimal, cost_product: Decimal, shipping_cost: Decimal,
                         fee_amount_effective: Decimal, tax_pct: Decimal,
                         rebate_meli_amount: Decimal) -> dict:
    fee_amount = q2(fee_amount_effective)
    tax_amount = q2(price * tax_pct)
    total_costs = q2(cost_product + shipping_cost + (fee_amount or Decimal("0")) + (tax_amount or Decimal("0")) - rebate_meli_amount)
    mc = q2(price - (total_costs or Decimal("0")))
    margin_pct = q6(safe_div(mc, price)) if mc is not None else None
    return {
        "price": q2(price),
        "fee_amount": fee_amount,
        "tax_amount": tax_amount,
        "mc": mc,
        "margin_pct": margin_pct,
        "rebate_meli_amount": q2(rebate_meli_amount),
    }


def calculate_net_result(financials: dict, *, use_cost: bool) -> Decimal | None:
    price = to_decimal(financials.get("price"))
    tax_amount = to_decimal(financials.get("tax_amount")) or Decimal("0")
    rebate_meli_amount = to_decimal(financials.get("rebate_meli_amount")) or Decimal("0")

    if price is None:
        return None

    if use_cost:
        return q2(price - tax_amount + rebate_meli_amount)

    return q2(price + rebate_meli_amount)


def extract_percent(promo: dict, primary_keys: list[str]) -> Decimal | None:
    direct = first_non_empty(promo, primary_keys)
    if direct is not None:
        return normalize_percent(direct)
    for nested_key in ("benefit", "funding", "contribution", "discount", "rebate"):
        nested = promo.get(nested_key)
        if isinstance(nested, dict):
            val = first_non_empty(nested, primary_keys)
            if val is not None:
                return normalize_percent(val)
    return None


def normalize_candidate_promotion(item: ScopeItem, promo: dict, today_utc: datetime,
                                  tax_pct: Decimal, max_meli_rebate_pct: Decimal, effective_cost_product: Decimal) -> dict | None:
    status = str(first_non_empty(promo, ["status", "promotion_status"]) or "").strip().lower()
    if status not in {"started", "active", "candidate"}:
        return None

    start_date = parse_start_date(first_non_empty(promo, ["start_date", "start_time", "date_start"]))
    start_date_utc = None
    if start_date is not None:
        start_date_utc = start_date.astimezone(timezone.utc) if start_date.tzinfo else start_date.replace(tzinfo=timezone.utc)
        if start_date_utc.date() > today_utc.date():
            return None

    meli_percent = extract_percent(promo, ["meli_percent", "meli_percentage", "marketplace_percent", "meli_contribution_percent"])
    if meli_percent is not None and max_meli_rebate_pct > 0 and meli_percent > max_meli_rebate_pct:
        return None

    seller_percent = extract_percent(promo, ["seller_percent", "seller_percentage", "seller_contribution_percent"])
    deal_price = first_positive_decimal(
        promo,
        [
            "price",
            "deal_price",
            "price_to_show",
            "offer_price",
            "suggested_discounted_price",
            "max_discounted_price",
            "min_discounted_price",
        ],
    )
    if deal_price is None:
        return None

    rebate_meli_amount = Decimal("0")
    if meli_percent is not None and meli_percent > 0:
        rebate_meli_amount = q2(deal_price * meli_percent)
        if rebate_meli_amount is None:
            return None
        if max_meli_rebate_pct > 0 and rebate_meli_amount > q2(deal_price * max_meli_rebate_pct):
            return None

    fin = calculate_financials(
        price=deal_price,
        cost_product=effective_cost_product,
        shipping_cost=item.shipping_list_cost or Decimal("0"),
        fee_amount_effective=item.fee_amount_effective or Decimal("0"),
        tax_pct=tax_pct,
        rebate_meli_amount=rebate_meli_amount,
    )

    promotion_type = str(first_non_empty(promo, ["promotion_type", "type", "campaign_type"]) or "SELLER_CAMPAIGN")
    offer_id = normalize_offer_id(first_non_empty(
        promo,
        [
            "offer_id",
            "offerId",
            "promotion_offer_id",
            "candidate_offer_id",
            "smart_offer_id",
            "ref_id",
            "reference_id",
        ],
    ))

    return {
        "promotion_id": str(first_non_empty(promo, ["promotion_id", "id"]) or "") or None,
        "promotion_type": promotion_type,
        "promotion_name": str(first_non_empty(promo, ["name", "promotion_name", "campaign_name"]) or "") or None,
        "offer_id": offer_id,
        "ref_id": str(first_non_empty(promo, ["ref_id", "reference_id"]) or "") or None,
        "status": status,
        "start_date": start_date_utc,
        "deal_price": q2(deal_price),
        "meli_percent": q6(meli_percent),
        "seller_percent": q6(seller_percent),
        "rebate_meli_amount": rebate_meli_amount,
        "financials": fin,
        "raw": promo,
    }


def choose_best_candidate(item: ScopeItem, promotions_payload: list[dict], tax_pct: Decimal,
                          min_margin_pct: Decimal, max_meli_rebate_pct: Decimal, use_cost: bool,
                          sku_min_receive_map: dict[str, dict[str, Decimal | None]] | None = None,
                          fee_cache: dict | None = None,
                          shipping_cache: dict | None = None,
                          session: requests.Session | None = None,
                          auth_headers: dict[str, str] | None = None) -> tuple[dict, str]:
    current_price = current_price_for_comparison(item)
    if current_price is None or current_price <= 0:
        return {}, "current_price_missing"

    if use_cost:
        if item.cost_missing_mapping:
            return {}, "missing_cost_mapping"
        if item.cost_missing_price:
            return {}, "missing_join_key_cost"
        if item.cost_product is None or item.cost_product <= 0:
            return {}, "missing_cost_product"
        effective_cost_product = item.cost_product
    else:
        effective_cost_product = Decimal("0")

    current_fin = calculate_financials(
        price=current_price,
        cost_product=effective_cost_product,
        shipping_cost=item.shipping_list_cost or Decimal("0"),
        fee_amount_effective=item.fee_amount_effective or Decimal("0"),
        tax_pct=tax_pct,
        rebate_meli_amount=item.rebate_meli_amount or Decimal("0"),
    )
    current_margin = current_fin.get("margin_pct")
    if current_margin is None:
        return {}, "current_margin_missing"

    current_net_result = calculate_net_result(current_fin, use_cost=use_cost)
    if current_net_result is None:
        return {}, "current_net_result_missing"

    fee_cache = fee_cache if fee_cache is not None else {}
    shipping_cache = shipping_cache if shipping_cache is not None else {}

    today_utc = datetime.now(timezone.utc)
    candidates: list[dict] = []
    already_started_candidates: list[dict] = []
    evaluated_candidates: list[dict] = []
    for promo in promotions_payload or []:
        if not isinstance(promo, dict):
            continue
        cand = normalize_candidate_promotion(
            item,
            promo,
            today_utc=today_utc,
            tax_pct=tax_pct,
            max_meli_rebate_pct=max_meli_rebate_pct,
            effective_cost_product=effective_cost_product,
        )
        if not cand:
            raw_price = first_positive_decimal(promo, ["price", "deal_price", "price_to_show", "offer_price", "suggested_discounted_price", "max_discounted_price", "min_discounted_price"])
            evaluated_candidates.append({
                "promotion_id": str(first_non_empty(promo, ["promotion_id", "id"]) or "") or None,
                "promotion_name": promo.get("name"),
                "promotion_type": str(first_non_empty(promo, ["promotion_type", "type", "campaign_type"]) or "SELLER_CAMPAIGN"),
                "status": str(first_non_empty(promo, ["status", "promotion_status"]) or "").strip().lower() or None,
                "deal_price": q2(raw_price),
                "raw": promo,
                "analysis_status": "rejected",
                "analysis_reason": "normalize_candidate_promotion_rejected",
                "financials": {},
            })
            continue
        if cand["promotion_id"] == item.current_promotion_id:
            cand["analysis_status"] = "rejected"
            cand["analysis_reason"] = "same_as_current_promotion"
            evaluated_candidates.append(cand)
            continue
        new_price = cand["financials"]["price"]
        new_margin = cand["financials"]["margin_pct"]
        if new_price is None or new_margin is None:
            cand["analysis_status"] = "rejected"
            cand["analysis_reason"] = "candidate_price_or_margin_missing"
            evaluated_candidates.append(cand)
            continue
        if new_price >= current_price:
            cand["analysis_status"] = "rejected"
            cand["analysis_reason"] = "candidate_price_not_lower_than_current"
            evaluated_candidates.append(cand)
            continue

        if session is not None and auth_headers is not None:
            fee_key = (item.category_id or "", item.listing_type_id or "", str(q2(new_price)))
            if fee_key not in fee_cache:
                fee_cache[fee_key] = fetch_listing_fee(
                    session,
                    new_price,
                    item.category_id,
                    item.listing_type_id,
                    auth_headers=auth_headers,
                )
            fee_info = fee_cache[fee_key]
            fee_amount_candidate = to_decimal(fee_info.get("fee_amount")) or Decimal("0")

            ship_key = item.mlb
            if ship_key not in shipping_cache:
                shipping_cache[ship_key] = fetch_shipping_option(
                    session,
                    item.mlb,
                    auth_headers=auth_headers,
                )
            ship_info = shipping_cache[ship_key]
            shipping_cost_candidate = to_decimal(ship_info.get("shipping_list_cost")) or Decimal("0")

            cand["fee_amount_candidate"] = q2(fee_amount_candidate)
            cand["shipping_cost_candidate"] = q2(shipping_cost_candidate)
            cand["financials"] = calculate_financials(
                price=new_price,
                cost_product=effective_cost_product,
                shipping_cost=shipping_cost_candidate,
                fee_amount_effective=fee_amount_candidate,
                tax_pct=tax_pct,
                rebate_meli_amount=cand.get("rebate_meli_amount") or Decimal("0"),
            )
            new_margin = cand["financials"]["margin_pct"]
            if new_margin is None:
                cand["analysis_status"] = "rejected"
                cand["analysis_reason"] = "candidate_margin_missing_after_recalc"
                evaluated_candidates.append(cand)
                continue

        candidate_net_result = calculate_net_result(cand["financials"], use_cost=use_cost)
        cand["candidate_net_result"] = candidate_net_result
        if candidate_net_result is None:
            cand["analysis_status"] = "rejected"
            cand["analysis_reason"] = "candidate_net_result_missing"
            evaluated_candidates.append(cand)
            continue

        if use_cost:
            if new_margin < current_margin:
                cand["analysis_status"] = "rejected"
                cand["analysis_reason"] = "candidate_margin_below_current"
                evaluated_candidates.append(cand)
                continue
            if new_margin < min_margin_pct:
                cand["analysis_status"] = "rejected"
                cand["analysis_reason"] = "candidate_margin_below_minimum"
                evaluated_candidates.append(cand)
                continue
        else:
            sku_rule_applied = False
            sku_rule_passed = False
            candidate_receive_estimated = None
            sku_min_value = None

            if item.sku and sku_min_receive_map:
                sku_limits = sku_min_receive_map.get(item.sku.strip())
                if sku_limits:
                    listing_type = (item.listing_type_id or "").strip().lower()
                    if listing_type == "gold_special":
                        sku_min_value = sku_limits.get("min_receive_classico")
                    elif listing_type == "gold_pro":
                        sku_min_value = sku_limits.get("min_receive_premium")

                    if sku_min_value is not None:
                        sku_rule_applied = True
                        if session is None or auth_headers is None:
                            raise RuntimeError("session/auth_headers required for sku minimum rule")

                        fee_amount_candidate = to_decimal(cand.get("fee_amount_candidate"))
                        shipping_cost_candidate = to_decimal(cand.get("shipping_cost_candidate"))

                        if fee_amount_candidate is None:
                            fee_key = (item.category_id or "", item.listing_type_id or "", str(q2(new_price)))
                            if fee_key not in fee_cache:
                                fee_cache[fee_key] = fetch_listing_fee(
                                    session,
                                    new_price,
                                    item.category_id,
                                    item.listing_type_id,
                                    auth_headers=auth_headers,
                                )
                            fee_info = fee_cache[fee_key]
                            fee_amount_candidate = to_decimal(fee_info.get("fee_amount")) or Decimal("0")
                            cand["fee_amount_candidate"] = q2(fee_amount_candidate)

                        if shipping_cost_candidate is None:
                            ship_key = item.mlb
                            if ship_key not in shipping_cache:
                                shipping_cache[ship_key] = fetch_shipping_option(
                                    session,
                                    item.mlb,
                                    auth_headers=auth_headers,
                                )
                            ship_info = shipping_cache[ship_key]
                            shipping_cost_candidate = to_decimal(ship_info.get("shipping_list_cost")) or Decimal("0")
                            cand["shipping_cost_candidate"] = q2(shipping_cost_candidate)

                        candidate_receive_estimated = q2(new_price - fee_amount_candidate - shipping_cost_candidate)

                        if candidate_receive_estimated is not None and candidate_receive_estimated >= sku_min_value:
                            sku_rule_passed = True

            if sku_rule_applied:
                cand["candidate_receive_estimated"] = candidate_receive_estimated
                cand["sku_min_value_used"] = sku_min_value
                if not sku_rule_passed:
                    cand["analysis_status"] = "rejected"
                    cand["analysis_reason"] = "candidate_receive_below_sku_minimum"
                    evaluated_candidates.append(cand)
                    continue
            else:
                if candidate_net_result < current_net_result:
                    cand["analysis_status"] = "rejected"
                    cand["analysis_reason"] = "candidate_net_result_below_current"
                    evaluated_candidates.append(cand)
                    continue

        if cand.get("status") in {"started", "active", "pending"}:
            cand["analysis_status"] = "approved_started"
            cand["analysis_reason"] = "already_in_campaign"
            already_started_candidates.append(cand)
            evaluated_candidates.append(cand)
            continue
        cand["analysis_status"] = "approved"
        cand["analysis_reason"] = "eligible_candidate"
        candidates.append(cand)
        evaluated_candidates.append(cand)

    def _sort_key(c: dict):
        net_result = calculate_net_result(c["financials"], use_cost=use_cost) or Decimal("0")
        return (
            c["financials"]["price"],
            -net_result,
            -(c["meli_percent"] or Decimal("0")),
            c["promotion_id"] or "",
        )

    if already_started_candidates:
        already_started_candidates.sort(key=_sort_key)
        best_started = already_started_candidates[0]
        return {
            "current": current_fin,
            "current_price": current_price,
            "current_margin_pct": current_margin,
            "current_net_result": current_net_result,
            "candidate": best_started,
            "candidate_net_result": calculate_net_result(best_started["financials"], use_cost=use_cost),
            "use_cost": use_cost,
            "evaluated_candidates": evaluated_candidates,
        }, "already_in_campaign"

    if not candidates:
        return {
            "current": current_fin,
            "current_price": current_price,
            "current_margin_pct": current_margin,
            "current_net_result": current_net_result,
            "use_cost": use_cost,
            "evaluated_candidates": evaluated_candidates,
        }, "no_better_candidate"

    candidates.sort(key=_sort_key)
    best = candidates[0]
    return {
        "current": current_fin,
        "current_price": current_price,
        "current_margin_pct": current_margin,
        "current_net_result": current_net_result,
        "candidate": best,
        "candidate_net_result": calculate_net_result(best["financials"], use_cost=use_cost),
        "use_cost": use_cost,
        "evaluated_candidates": evaluated_candidates,
    }, "switch"


def read_csv_mlbs(path: str) -> set[str]:
    out: set[str] = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            mlb = (row.get("mlb") or row.get("item_id") or "").strip()
            if mlb:
                out.add(mlb)
    return out


def format_decimal_br(value: Any, decimals: int = 2) -> str:
    dec = to_decimal(value)
    if dec is None:
        return ""
    quant = Decimal("1").scaleb(-decimals)
    dec = dec.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{dec:.{decimals}f}".replace(".", ",")


def format_percent_br(value: Any, decimals: int = 3) -> str:
    dec = to_decimal(value)
    if dec is None:
        return ""
    pct = dec * Decimal("100")
    quant = Decimal("1").scaleb(-decimals)
    pct = pct.quantize(quant, rounding=ROUND_HALF_UP)
    return f"{pct:.{decimals}f}".replace(".", ",")


def format_datetime_br(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, datetime):
        dt = value
    else:
        dt = parse_start_date(value)
        if dt is None:
            return str(value)
    return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S") if dt.tzinfo else dt.strftime("%d/%m/%Y %H:%M:%S")



def format_row_for_csv(row: dict) -> dict:
    out = dict(row)

    # Frete sugerido deve acompanhar o frete atual quando não houver valor específico do candidato.
    # Isso evita CSV com a coluna "Frete sugerido" em branco em cenários sem campanha elegível
    # ou quando o frete candidato não foi recalculado separadamente.
    if out.get("candidate_shipping_cost") in (None, ""):
        out["candidate_shipping_cost"] = out.get("shipping_cost")

    numeric_2 = [
        "current_price",
        "shipping_cost",
        "fee_amount_current",
        "current_receive",
        "candidate_price",
        "candidate_shipping_cost",
        "candidate_rebate_meli_amount",
        "fee_amount_candidate",
        "candidate_receive_estimated",
        "sku_min_value_used",
        "delta_receive",
    ]
    for col in numeric_2:
        out[col] = format_decimal_br(out.get(col), 2)

    # Traduções apenas para a experiência do usuário no CSV.
    # Os valores técnicos continuam preservados no banco/raw_decision.
    out["action"] = label_for_csv(ACTION_LABELS, out.get("action"))
    out["execution_status"] = label_for_csv(EXECUTION_STATUS_LABELS, out.get("execution_status"))
    out["reason"] = label_for_csv(REASON_LABELS, out.get("reason"))

    out["dry_run"] = "true" if bool(out.get("dry_run")) else "false"
    return out

def write_audit_csv(path: str, rows: list[dict]):
    fieldnames = list(CSV_HEADERS.keys())
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writerow(CSV_HEADERS)
        for row in rows:
            formatted = format_row_for_csv(row)
            writer.writerow({k: formatted.get(k) for k in fieldnames})


def default_detailed_csv_path(path: str) -> str:
    root, ext = os.path.splitext(path)
    if not ext:
        ext = ".csv"
    return f"{root}_detalhado{ext}"




def build_log_row(connected_seller_id: int, run_id: int, item: ScopeItem, decision: dict, action: str, execution_status: str,
                  reason: str, dry_run: bool, use_cost: bool, api_result: dict | None = None) -> dict:
    current = decision.get("current") or {}
    candidate = decision.get("candidate") or {}
    cand_fin = candidate.get("financials") or {}

    current_price = to_decimal(current.get("price"))
    fee_amount_current = to_decimal(current.get("fee_amount")) or Decimal("0")
    shipping_cost = item.shipping_list_cost if item.shipping_list_cost is not None else Decimal("0")
    current_receive = None
    if current_price is not None:
        current_receive = q2(current_price - fee_amount_current - shipping_cost)

    candidate_price = to_decimal(cand_fin.get("price"))
    fee_amount_candidate = to_decimal(cand_fin.get("fee_amount")) or Decimal("0")
    candidate_shipping_cost = to_decimal(candidate.get("shipping_cost_candidate"))
    if candidate_shipping_cost is None:
        candidate_shipping_cost = shipping_cost

    candidate_receive_estimated = to_decimal(candidate.get("candidate_receive_estimated"))
    if candidate_receive_estimated is None and candidate_price is not None:
        candidate_receive_estimated = q2(
            candidate_price
            - fee_amount_candidate
            - candidate_shipping_cost
            + (to_decimal(cand_fin.get("rebate_meli_amount")) or Decimal("0"))
        )

    delta_receive = None
    if current_receive is not None and candidate_receive_estimated is not None:
        delta_receive = q2(candidate_receive_estimated - current_receive)

    listing_type_label = {
        "gold_special": "Clássico",
        "gold_pro": "Premium",
    }.get((item.listing_type_id or "").strip().lower(), item.listing_type_id)

    return {
        "connected_seller_id": connected_seller_id,
        "run_id": run_id,
        "mlb": item.mlb,
        "variation_id": item.variation_id,
        "sku": item.sku,
        "title": item.title,
        "status": item.status,
        "listing_type_label": listing_type_label,
        "current_promotion_id": item.current_promotion_id,
        "current_promotion_type": item.current_promotion_type,
        "candidate_promotion_id": candidate.get("promotion_id"),
        "candidate_promotion_type": candidate.get("promotion_type"),
        "candidate_promotion_name": candidate.get("promotion_name"),
        "candidate_start_date": candidate.get("start_date"),
        "current_price": current.get("price"),
        "shipping_cost": shipping_cost,
        "fee_amount_current": current.get("fee_amount"),
        "current_receive": current_receive,
        "candidate_price": cand_fin.get("price"),
        "candidate_shipping_cost": candidate_shipping_cost,
        "candidate_rebate_meli_amount": cand_fin.get("rebate_meli_amount"),
        "fee_amount_candidate": candidate.get("fee_amount_candidate") if candidate.get("fee_amount_candidate") is not None else cand_fin.get("fee_amount"),
        "candidate_receive_estimated": candidate_receive_estimated,
        "sku_min_value_used": candidate.get("sku_min_value_used"),
        "delta_receive": delta_receive,
        "current_margin_pct": current.get("margin_pct"),
        "candidate_margin_pct": cand_fin.get("margin_pct"),
        "current_mc": current.get("mc"),
        "candidate_mc": cand_fin.get("mc"),
        "current_rebate_meli_amount": current.get("rebate_meli_amount"),
        "cost_product": item.cost_product,
        "shipping_cost_current": shipping_cost,
        "shipping_cost_candidate": shipping_cost,
        "fee_pct_effective": item.fee_pct_effective,
        "tax_pct": decision.get("tax_pct"),
        "tax_amount_current": current.get("tax_amount"),
        "tax_amount_candidate": cand_fin.get("tax_amount"),
        "action": action,
        "execution_status": execution_status,
        "reason": reason,
        "dry_run": dry_run,
        "raw_current": {
            "current_promotion_id": item.current_promotion_id,
            "current_promotion_type": item.current_promotion_type,
            "sale_amount_current": str(item.sale_amount_current) if item.sale_amount_current is not None else None,
            "effective_price": str(item.effective_price) if item.effective_price is not None else None,
            "price": str(item.price) if item.price is not None else None,
            "meli_percent": str(item.meli_percent) if item.meli_percent is not None else None,
            "rebate_meli_amount": str(item.rebate_meli_amount) if item.rebate_meli_amount is not None else None,
            "cost_detail": item.cost_detail,
            "cost_missing_mapping": item.cost_missing_mapping,
            "cost_missing_price": item.cost_missing_price,
            "listing_type_id": item.listing_type_id,
            "listing_type_label": listing_type_label,
        },
        "raw_candidate": candidate.get("raw"),
        "raw_decision": {
            "current_price_comparison": str(decision.get("current_price")) if decision.get("current_price") is not None else None,
            "current_margin_pct": str(decision.get("current_margin_pct")) if decision.get("current_margin_pct") is not None else None,
            "tax_pct": str(decision.get("tax_pct")) if decision.get("tax_pct") is not None else None,
            "max_meli_rebate_pct": str(decision.get("max_meli_rebate_pct")) if decision.get("max_meli_rebate_pct") is not None else None,
            "candidate_offer_id": candidate.get("offer_id"),
            "candidate_ref_id": candidate.get("ref_id"),
            "candidate_promotion_name": candidate.get("promotion_name"),
            "current_net_result": str(decision.get("current_net_result")) if decision.get("current_net_result") is not None else None,
            "candidate_net_result": str(decision.get("candidate_net_result")) if decision.get("candidate_net_result") is not None else None,
            "candidate_receive_estimated": str(candidate.get("candidate_receive_estimated")) if candidate.get("candidate_receive_estimated") is not None else None,
            "candidate_shipping_cost": str(candidate.get("shipping_cost_candidate")) if candidate.get("shipping_cost_candidate") is not None else None,
            "sku_min_value_used": str(candidate.get("sku_min_value_used")) if candidate.get("sku_min_value_used") is not None else None,
            "fee_amount_candidate_recalculated": str(candidate.get("fee_amount_candidate")) if candidate.get("fee_amount_candidate") is not None else None,
            "shipping_cost_candidate_recalculated": str(candidate.get("shipping_cost_candidate")) if candidate.get("shipping_cost_candidate") is not None else None,
            "listing_type_id": item.listing_type_id,
            "listing_type_label": listing_type_label,
            "use_cost": use_cost,
        },
        "raw_api_result": api_result,
    }


def build_detailed_rows_from_decision(base_row: dict, item: ScopeItem, decision: dict, decision_reason: str, dry_run: bool) -> list[dict]:
    rows: list[dict] = []
    current = decision.get("current") or {}
    current_price = to_decimal(current.get("price"))
    fee_amount_current = to_decimal(current.get("fee_amount")) or Decimal("0")
    shipping_cost = to_decimal(base_row.get("shipping_cost")) or Decimal("0")
    current_receive = to_decimal(base_row.get("current_receive"))
    listing_type_label = base_row.get("listing_type_label")

    for cand in decision.get("evaluated_candidates") or []:
        cand_fin = cand.get("financials") or {}
        candidate_price = to_decimal(cand_fin.get("price") or cand.get("deal_price"))
        fee_amount_candidate = to_decimal(cand.get("fee_amount_candidate"))
        if fee_amount_candidate is None:
            fee_amount_candidate = to_decimal(cand_fin.get("fee_amount")) or Decimal("0")
        candidate_shipping_cost = to_decimal(cand.get("shipping_cost_candidate"))
        if candidate_shipping_cost is None:
            candidate_shipping_cost = shipping_cost
        candidate_receive_estimated = to_decimal(cand.get("candidate_receive_estimated"))
        if candidate_receive_estimated is None and candidate_price is not None:
            candidate_receive_estimated = q2(
                candidate_price
                - fee_amount_candidate
                - candidate_shipping_cost
                + (to_decimal(cand_fin.get("rebate_meli_amount")) or Decimal("0"))
            )
        delta_receive = None
        if current_receive is not None and candidate_receive_estimated is not None:
            delta_receive = q2(candidate_receive_estimated - current_receive)

        row = {
            "connected_seller_id": base_row.get("connected_seller_id"),
            "run_id": base_row.get("run_id"),
            "mlb": item.mlb,
            "variation_id": item.variation_id,
            "sku": item.sku,
            "title": item.title,
            "status": item.status,
            "listing_type_label": listing_type_label,
            "current_price": current_price,
            "shipping_cost": shipping_cost,
            "fee_amount_current": fee_amount_current,
            "current_receive": current_receive,
            "candidate_promotion_name": cand.get("promotion_name"),
            "candidate_promotion_id": cand.get("promotion_id"),
            "candidate_promotion_type": cand.get("promotion_type"),
            "candidate_price": candidate_price,
            "candidate_shipping_cost": candidate_shipping_cost,
            "candidate_rebate_meli_amount": to_decimal(cand_fin.get("rebate_meli_amount")) or to_decimal(cand.get("rebate_meli_amount")),
            "fee_amount_candidate": fee_amount_candidate,
            "candidate_receive_estimated": candidate_receive_estimated,
            "sku_min_value_used": to_decimal(cand.get("sku_min_value_used")),
            "delta_receive": delta_receive,
            "action": "SWITCH" if cand.get("analysis_status") in {"approved", "approved_started"} else "SKIP",
            "execution_status": "campaign_approved" if cand.get("analysis_status") in {"approved", "approved_started"} else "campaign_rejected",
            "reason": cand.get("analysis_reason") or decision_reason,
            "dry_run": dry_run,
            "raw_current": base_row.get("raw_current"),
            "raw_candidate": cand.get("raw"),
            "raw_decision": {
                "analysis_status": cand.get("analysis_status"),
                "analysis_reason": cand.get("analysis_reason"),
                "current_price_comparison": str(decision.get("current_price")) if decision.get("current_price") is not None else None,
                "current_net_result": str(decision.get("current_net_result")) if decision.get("current_net_result") is not None else None,
                "candidate_net_result": str(cand.get("candidate_net_result")) if cand.get("candidate_net_result") is not None else None,
                "candidate_ref_id": cand.get("ref_id"),
                "candidate_offer_id": cand.get("offer_id"),
                "sku_min_value_used": str(cand.get("sku_min_value_used")) if cand.get("sku_min_value_used") is not None else None,
                "candidate_receive_estimated": str(candidate_receive_estimated) if candidate_receive_estimated is not None else None,
            },
            "raw_api_result": None,
        }
        rows.append(row)

    if not rows:
        # fallback line even when no campaign could be normalized
        rows.append(dict(base_row))
    return rows


def process_items(connected_seller_id: int, items: list[ScopeItem], source_run_id: int | None, dry_run: bool, max_switch: int | None,
                  tax_pct: Decimal, min_margin_pct: Decimal, max_meli_rebate_pct: Decimal,
                  threads: int, rps: float, insecure: bool, out_csv: str, out_detailed_csv: str | None, flush_every: int,
                  use_cost: bool, mlb_filter: set[str] | None = None,
                  sku_min_receive_map: dict[str, dict[str, Decimal | None]] | None = None) -> dict:
    session = _mk_session(insecure=insecure)
    limiter = RateLimiter(rps)
    auth_headers = build_auth_headers(connected_seller_id)
    fee_cache: dict = {}
    shipping_cache: dict = {}

    if mlb_filter:
        items = [item for item in items if item.mlb in mlb_filter]

    items = sorted(items, key=lambda x: (x.mlb, x.variation_id or -1))
    total_items = len(items)
    conn = db_connect()
    run_id = None
    audit_rows: list[dict] = []
    detailed_audit_rows: list[dict] = []
    pending_rows: list[dict] = []
    switched = 0
    processed = 0
    errors = 0
    no_action = 0
    switch_success_count = 0
    switch_count_lock = threading.Lock()

    try:
        ensure_log_table(conn)
        conn.commit()

        run_id = create_run(
            conn,
            connected_seller_id=connected_seller_id,
            run_type="campaign_optimizer",
            status="running",
            params={
                "source_run_id": source_run_id,
                "dry_run": dry_run,
                "max_switch": max_switch,
                "tax_pct": str(tax_pct),
                "min_margin_pct": str(min_margin_pct),
                "max_meli_rebate_pct": str(max_meli_rebate_pct),
                "threads": threads,
                "rps": rps,
                "insecure": insecure,
                "out_csv": out_csv,
                "out_detailed_csv": out_detailed_csv,
                "flush_every": flush_every,
                "use_cost": use_cost,
                "mlb_filter_count": len(mlb_filter or set()),
                "scope_items": len(items),
            },
        )
        conn.commit()

        def flush_logs():
            nonlocal pending_rows
            if not pending_rows:
                return
            insert_log_rows(conn, pending_rows, page_size=DEFAULT_INSERT_PAGE_SIZE)
            conn.commit()
            pending_rows = []

        def worker(item: ScopeItem):
            limiter.wait()
            promos = item_promotions(session, connected_seller_id, item.mlb, auth_headers=auth_headers)
            decision, reason = choose_best_candidate(
                item,
                promos,
                tax_pct=tax_pct,
                min_margin_pct=min_margin_pct,
                max_meli_rebate_pct=max_meli_rebate_pct,
                use_cost=use_cost,
                sku_min_receive_map=sku_min_receive_map,
                fee_cache=fee_cache,
                shipping_cache=shipping_cache,
                session=session,
                auth_headers=auth_headers,
            )
            decision["tax_pct"] = tax_pct
            decision["max_meli_rebate_pct"] = max_meli_rebate_pct
            decision["use_cost"] = use_cost

            if reason == "already_in_campaign":
                row = build_log_row(connected_seller_id, run_id, item, decision, action="SKIP", execution_status="already_started_or_pending",
                                     reason=reason, dry_run=dry_run, use_cost=use_cost)
                return row, build_detailed_rows_from_decision(row, item, decision, reason, dry_run)

            if reason != "switch":
                row = build_log_row(connected_seller_id, run_id, item, decision, action="SKIP", execution_status="not_applicable",
                                     reason=reason, dry_run=dry_run, use_cost=use_cost)
                return row, build_detailed_rows_from_decision(row, item, decision, reason, dry_run)

            candidate = decision["candidate"]
            if dry_run:
                row = build_log_row(connected_seller_id, run_id, item, decision, action="SWITCH", execution_status="dry_run",
                                     reason="eligible_candidate", dry_run=True, use_cost=use_cost)
                return row, build_detailed_rows_from_decision(row, item, decision, "eligible_candidate", True)

            with switch_count_lock:
                nonlocal switch_success_count
                if max_switch is not None and switch_success_count >= max_switch:
                    row = build_log_row(connected_seller_id, run_id, item, decision, action="SKIP", execution_status="not_applicable",
                                         reason="max_switch_reached", dry_run=False, use_cost=use_cost)
                    return row, build_detailed_rows_from_decision(row, item, decision, "max_switch_reached", False)

            api_result = campaign_item_upsert(
                session=session,
                connected_seller_id=connected_seller_id,
                campaign_id=candidate["promotion_id"],
                item_id=item.mlb,
                deal_price=candidate["deal_price"],
                promotion_type=candidate["promotion_type"],
                offer_id=candidate.get("offer_id"),
                auth_headers=auth_headers,
            )
            if api_result.get("ok"):
                with switch_count_lock:
                    switch_success_count += 1
                row = build_log_row(connected_seller_id, run_id, item, decision, action="SWITCH", execution_status="success",
                                     reason="eligible_candidate", dry_run=False, use_cost=use_cost, api_result=api_result)
                return row, build_detailed_rows_from_decision(row, item, decision, "eligible_candidate", False)
            row = build_log_row(connected_seller_id, run_id, item, decision, action="SWITCH", execution_status="error",
                                 reason=api_result.get("error_code") or "api_error", dry_run=False,
                                 use_cost=use_cost, api_result=api_result)
            return row, build_detailed_rows_from_decision(row, item, decision, api_result.get("error_code") or "api_error", False)

        with ThreadPoolExecutor(max_workers=threads) as ex:
            futures = {ex.submit(worker, item): item for item in items}

            for fut in as_completed(futures):
                processed += 1
                try:
                    row, detailed_rows = fut.result()
                except Exception as exc:
                    item = futures[fut]
                    row = {
                        "connected_seller_id": connected_seller_id,
                        "run_id": run_id,
                        "mlb": item.mlb,
                        "variation_id": item.variation_id,
                        "sku": item.sku,
                        "title": item.title,
                        "status": item.status,
                        "action": "SKIP",
                        "execution_status": "error",
                        "reason": str(exc),
                        "dry_run": dry_run,
                        "current_promotion_id": item.current_promotion_id,
                        "current_promotion_type": item.current_promotion_type,
                        "candidate_promotion_id": None,
                        "candidate_promotion_type": None,
                        "candidate_promotion_name": None,
                        "candidate_start_date": None,
                        "current_price": current_price_for_comparison(item),
                        "shipping_cost_current": item.shipping_list_cost,
                        "fee_amount_current": item.fee_amount_effective,
                        "current_receive": None,
                        "candidate_price": None,
                        "current_margin_pct": None,
                        "candidate_margin_pct": None,
                        "current_mc": None,
                        "candidate_mc": None,
                        "current_rebate_meli_amount": item.rebate_meli_amount,
                        "candidate_rebate_meli_amount": None,
                        "candidate_shipping_cost": item.shipping_list_cost,
                        "shipping_cost_candidate": None,
                        "candidate_receive_estimated": None,
                        "sku_min_value_used": None,
                        "delta_receive": None,
                        "cost_product": item.cost_product,
                        "shipping_cost": item.shipping_list_cost,
                        "fee_amount_current": item.fee_amount_effective,
                        "fee_amount_candidate": None,
                        "fee_pct_effective": item.fee_pct_effective,
                        "tax_pct": tax_pct,
                        "tax_amount_current": None,
                        "tax_amount_candidate": None,
                        "raw_current": {"cost_detail": item.cost_detail},
                        "raw_candidate": None,
                        "raw_decision": {"use_cost": use_cost},
                        "raw_api_result": None,
                    }
                    detailed_rows = [dict(row)]

                audit_rows.append(row)
                detailed_audit_rows.extend(detailed_rows)
                pending_rows.append(row)

                if row.get("action") == "SWITCH" and row.get("execution_status") in {"success", "dry_run"}:
                    switched += 1
                elif row.get("execution_status") == "error":
                    errors += 1
                else:
                    no_action += 1

                current_mlb = row.get("mlb") or "-"
                current_reason = row.get("reason") or "-"
                current_status = row.get("execution_status") or "-"
                if processed == 1 or processed % 25 == 0 or processed == total_items:
                    print(
                        f"[OPTIMIZER] {processed}/{total_items} | mlb={current_mlb} | status={current_status} | "
                        f"reason={current_reason} | switched={switched} | errors={errors} | no_action={no_action}"
                    )

                if len(pending_rows) >= flush_every:
                    flush_logs()

        flush_logs()
        write_audit_csv(out_csv, audit_rows)
        if out_detailed_csv:
            write_audit_csv(out_detailed_csv, detailed_audit_rows)

        finish_run(
            conn,
            run_id,
            status="finished",
            totals={
                "scope_items": total_items,
                "processed": processed,
                "switched": switched,
                "errors": errors,
                "no_action": no_action,
                "dry_run": dry_run,
                "use_cost": use_cost,
                "out_csv": out_csv,
                "out_detailed_csv": out_detailed_csv,
            },
        )
        conn.commit()
        return {
            "run_id": run_id,
            "scope_items": total_items,
            "processed": processed,
            "switched": switched,
            "errors": errors,
            "no_action": no_action,
            "use_cost": use_cost,
            "out_csv": out_csv,
            "out_detailed_csv": out_detailed_csv,
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        if run_id is not None:
            try:
                finish_run(conn, run_id, status="error", error=str(e))
                conn.commit()
            except Exception:
                pass
        raise
    finally:
        conn.close()


def parse_bool(value: str) -> bool:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError("Use true/false, yes/no, 1/0, on/off")


def parse_args():
    p = argparse.ArgumentParser(description="Otimiza campanhas ML com rebate de tarifa preservando margem")
    p.add_argument("--connected-seller-id", type=int, required=True, help="ID do seller conectado na tabela ml.connected_seller")
    p.add_argument("--source-run-id", type=int, default=None, help="run_id fonte da inventory_snapshot; default = último")
    p.add_argument("--tax-pct", type=float, default=DEFAULT_TAX_PCT, help="Percentual de imposto. Aceita 0.09 ou 9")
    p.add_argument("--min-margin-pct", type=float, default=DEFAULT_MIN_MARGIN_PCT, help="Margem mínima. Aceita 0.08 ou 8")
    p.add_argument("--max-meli-rebate-pct", type=float, default=DEFAULT_MAX_MELI_REBATE_PCT,
                   help="Teto do rebate percentual do ML aceito para homologação/execução. Aceita 0.30 ou 30")
    p.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    p.add_argument("--rps", type=float, default=DEFAULT_RPS)
    p.add_argument("--limit", type=int, default=None, help="Limita quantidade de itens do escopo")
    p.add_argument("--max-switch", type=int, default=None, help="Limita trocas efetivas por execução")
    p.add_argument("--mlb", action="append", default=None, help="Filtra MLB específico; pode repetir")
    p.add_argument("--csv", default=None, help="CSV com coluna mlb ou item_id para filtrar escopo")
    p.add_argument("--dry-run", action="store_true", help="Simula decisões sem alterar campanhas")
    p.add_argument("--insecure", action="store_true", help="Desativa validação SSL (debug)")
    p.add_argument("--out", default=None, help="CSV de auditoria")
    p.add_argument("--out-detailed", default=None, help="CSV detalhado por campanha analisada")
    p.add_argument("--flush-every", type=int, default=DEFAULT_FLUSH_EVERY)
    p.add_argument("--use-cost", type=parse_bool, default=DEFAULT_USE_COST,
                   help="Usa custo do produto nas regras (true/false). Default atual: false para onboarding inicial")
    return p.parse_args()


def main():
    args = parse_args()
    connected_seller_id = args.connected_seller_id
    tax_pct = normalize_percent(args.tax_pct) or Decimal("0")
    min_margin_pct = normalize_percent(args.min_margin_pct) or Decimal("0.08")
    max_meli_rebate_pct = normalize_percent(args.max_meli_rebate_pct) or Decimal("0.30")
    use_cost = bool(args.use_cost)

    mlb_filter: set[str] = set(args.mlb or [])
    if args.csv:
        mlb_filter.update(read_csv_mlbs(args.csv))

    conn = db_connect()
    try:
        items = build_scope_items(
            conn,
            connected_seller_id=connected_seller_id,
            source_run_id=args.source_run_id,
            limit=args.limit,
        )
        sku_min_receive_map = fetch_sku_min_receive_map(conn, connected_seller_id)
    finally:
        conn.close()

    out_csv = args.out or f"campaign_optimizer_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_detailed_csv = args.out_detailed or default_detailed_csv_path(out_csv)

    print(
        f"[OPTIMIZER] Iniciando | scope_items={len(items)} | dry_run={args.dry_run} | use_cost={use_cost} | tax_pct={tax_pct} | "
        f"min_margin_pct={min_margin_pct} | max_meli_rebate_pct={max_meli_rebate_pct} | mlb_filter={len(mlb_filter)}"
    )

    result = process_items(
        connected_seller_id=connected_seller_id,
        items=items,
        source_run_id=args.source_run_id,
        dry_run=args.dry_run,
        max_switch=args.max_switch,
        tax_pct=tax_pct,
        min_margin_pct=min_margin_pct,
        max_meli_rebate_pct=max_meli_rebate_pct,
        threads=args.threads,
        rps=args.rps,
        insecure=args.insecure,
        out_csv=out_csv,
        out_detailed_csv=out_detailed_csv,
        flush_every=args.flush_every,
        use_cost=use_cost,
        mlb_filter=mlb_filter or None,
        sku_min_receive_map=sku_min_receive_map,
    )
    print(f"[OPTIMIZER] Finalizado | processed={result.get('processed')} | switched={result.get('switched')} | errors={result.get('errors')} | no_action={result.get('no_action')}")
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
