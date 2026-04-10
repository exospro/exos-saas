#!/usr/bin/env python3
import os
import csv
import time
import argparse
import threading
from decimal import Decimal, ROUND_HALF_UP
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#from core import config  # mantém o padrão do projeto
from etl.ml_auth_db_multi import get_valid_access_token, get_headers


APP_VERSION = "v2"

DEFAULT_THREADS = 8
DEFAULT_RPS = 8
DEFAULT_TIMEOUT = 60


# --- helpers -----------------------------------------------------------------

def _parse_price_to_float(raw: str) -> float:
    """Parse price strings robustly (pt-BR / en-US), handling thousand separators."""
    s = (raw or "").strip()
    if not s:
        raise ValueError("preço vazio")
    s = s.replace("R$", "").replace(" ", "")

    # pt-BR decimal
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
        return float(s)

    # 1.234.56 -> 1234.56
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
        return float(s)

    return float(s)


def _q2(x: float) -> float:
    """Round to 2 decimals (half up) as ML expects monetary values."""
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _mk_session(insecure: bool = False) -> requests.Session:
    """Cria uma Session com retry + pool."""
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
    s.verify = (not insecure)
    return s

class RateLimiter:
    """Rate limiter global simples (token bucket por tempo)."""
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


def _safe_json(resp: requests.Response):
    try:
        return resp.json()
    except Exception:
        return {"_raw": (resp.text or "").strip()[:4000]}


def _extract_error_code(err: dict) -> str | None:
    """Tenta extrair error_code (cause[0].error_code) ou a partir de message."""
    if not isinstance(err, dict):
        return None

    cause = err.get("cause")
    if isinstance(cause, list) and cause:
        if isinstance(cause[0], dict) and cause[0].get("error_code"):
            return str(cause[0]["error_code"])

    msg = err.get("message") or ""
    if isinstance(msg, str) and "Errors:" in msg:
        # Ex.: Errors: MINIMUM_DISCOUNT_PERCENT - ...
        m = msg.split("Errors:", 1)[1].strip()
        if "-" in m:
            return m.split("-", 1)[0].strip()

    return err.get("error")


def _is_terminal_price_rule(code: str | None) -> bool:
    return code in {
        "MINIMUM_DISCOUNT_PERCENT",
        "ERROR_CREDIBILITY_DISCOUNTED_PRICE",
    }


def _fmt_price(x) -> str:
    try:
        return f"{float(x):.2f}" if x is not None else "NA"
    except Exception:
        return "NA"


def _response_applied_price(body: dict, fallback: float) -> float:
    applied = body.get("price")
    if applied is None:
        applied = body.get("deal_price")
    if applied is None:
        applied = fallback
    return _q2(applied)


# --- Mercado Livre APIs -------------------------------------------------------

def get_item_current_price(session: requests.Session, item_id: str) -> float | None:
    """Busca o preço atual do anúncio (público)."""
    url = f"https://api.mercadolibre.com/items/{item_id}"
    resp = session.get(url, timeout=DEFAULT_TIMEOUT, verify=session.verify)
    if not (200 <= resp.status_code < 300):
        return None
    data = _safe_json(resp)
    price = data.get("price")
    try:
        return float(price)
    except Exception:
        return None


def item_promotions(session: requests.Session, connected_seller_id: int, item_id: str):
    """Lista promoções/campanhas disponíveis para um item."""
    url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    resp = session.get(
        url,
        headers=get_headers(connected_seller_id),
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if 200 <= resp.status_code < 300:
        return _safe_json(resp)
    raise requests.HTTPError(f"{resp.status_code} {resp.reason} - {_safe_json(resp)}")

def campaign_promotion_items_page(
    session,
    connected_seller_id: int,
    campaign_id: str,
    promotion_type: str = "SELLER_CAMPAIGN",
    search_after: str | None = None,
    limit: int = 50,
):

    """Lista uma página de itens de uma campanha de seller promotions."""
    url = f"https://api.mercadolibre.com/seller-promotions/promotions/{campaign_id}/items"
    params = {
        "app_version": APP_VERSION,
        "promotion_type": promotion_type,
        "limit": int(limit),
    }
    if search_after:
        params["searchAfter"] = search_after

    resp = session.get(
        url,
        headers=get_headers(connected_seller_id),
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    
    if 200 <= resp.status_code < 300:
        return _safe_json(resp)
    raise requests.HTTPError(f"{resp.status_code} {resp.reason} - {_safe_json(resp)}")


def campaign_promotion_items_all(
    session: requests.Session,
    campaign_id: str,
    promotion_type: str = "SELLER_CAMPAIGN",
    limit: int = 50,
):
    """Percorre todas as páginas de itens de uma campanha usando paging.searchAfter."""
    results = []
    search_after = None

    while True:
        data = campaign_promotion_items_page(
            session,
            campaign_id,
            promotion_type=promotion_type,
            search_after=search_after,
            limit=limit,
        )
        page_results = data.get("results") or []
        if isinstance(page_results, list):
            results.extend(page_results)

        paging = data.get("paging") or {}
        search_after = paging.get("searchAfter")
        if not search_after:
            break

    return results


def campaign_promotion_price_map(
    session: requests.Session,
    campaign_id: str,
    promotion_type: str = "SELLER_CAMPAIGN",
    limit: int = 50,
):
    """Retorna {item_id: price} apenas para itens que já possuem preço aplicado na campanha."""
    items = campaign_promotion_items_all(
        session,
        campaign_id,
        promotion_type=promotion_type,
        limit=limit,
    )
    out: dict[str, float] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        item_id = (it.get("id") or "").strip() if isinstance(it.get("id"), str) else (it.get("id") or "")
        price = it.get("price")
        if not item_id or price is None:
            continue
        try:
            out[str(item_id)] = _q2(float(price))
        except Exception:
            continue
    return out


def clone_campaign_prices(
    session: requests.Session,
    source_campaign_id: str,
    destination_campaign_id: str,
    threads: int = DEFAULT_THREADS,
    rps: int = DEFAULT_RPS,
    promotion_type: str = "SELLER_CAMPAIGN",
    min_discount_percent: float = 5.0,
    auto_fix_min_discount: bool = True,
    out_csv: str | None = None,
    fetch_limit: int = 50,
):
    """Clona os preços (price -> deal_price) de uma campanha origem para uma campanha destino."""
    items_price = campaign_promotion_price_map(
        session,
        source_campaign_id,
        promotion_type=promotion_type,
        limit=fetch_limit,
    )
    return campaign_items_upsert_threaded(
        session,
        destination_campaign_id,
        items_price,
        threads=threads,
        rps=rps,
        promotion_type=promotion_type,
        min_discount_percent=min_discount_percent,
        auto_fix_min_discount=auto_fix_min_discount,
        out_csv=out_csv,
    )


def is_item_in_campaign(
    session: requests.Session,
    connected_seller_id: int,
    item_id: str,
    campaign_id: str,
) -> bool:
    try:
        promos = item_promotions(session, connected_seller_id, item_id)
        if isinstance(promos, list):
            for p in promos:
                if isinstance(p, dict) and p.get("id") == campaign_id:
                    return True
    except Exception:
        pass
    return False


def print_item_promotions(session: requests.Session, item_id: str):
    promos = item_promotions(session, item_id)
    for p in promos:
        print({
            "id": p.get("id"),
            "type": p.get("type"),
            "sub_type": p.get("sub_type"),
            "status": p.get("status"),
            "name": p.get("name"),
            "price": p.get("price") or p.get("deal_price"),
            "original_price": p.get("original_price"),
            "start_date": p.get("start_date"),
            "finish_date": p.get("finish_date"),
        })


def campaign_item_post(
    session,
    connected_seller_id: int,
    campaign_id: str,
    item_id: str,
    deal_price: float,
    promotion_type: str = "SELLER_CAMPAIGN",
):
    base_url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    payload = {
        "promotion_id": campaign_id,
        "promotion_type": promotion_type,
        "deal_price": _q2(deal_price),
    }
    headers = {**get_headers(connected_seller_id), "Content-Type": "application/json"}

    resp = session.post(
        base_url,
        headers=headers,
        json=payload,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if 200 <= resp.status_code < 300:
        return {"ok": True, "method": "POST", "status_code": resp.status_code, "body": _safe_json(resp)}
    return {"ok": False, "method": "POST", "status_code": resp.status_code, "error": _safe_json(resp)}


def campaign_item_put(
    session,
    connected_seller_id: int,
    campaign_id: str,
    item_id: str,
    deal_price: float,
    promotion_type: str = "SELLER_CAMPAIGN",
):
    base_url = f"https://api.mercadolibre.com/seller-promotions/items/{item_id}"
    params = {"app_version": APP_VERSION}
    payload = {
        "promotion_id": campaign_id,
        "promotion_type": promotion_type,
        "deal_price": _q2(deal_price),
    }
    headers = {**get_headers(connected_seller_id), "Content-Type": "application/json"}

    resp = session.put(
        base_url,
        headers=headers,
        json=payload,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        verify=session.verify,
    )
    if 200 <= resp.status_code < 300:
        return {"ok": True, "method": "PUT", "status_code": resp.status_code, "body": _safe_json(resp)}
    return {"ok": False, "method": "PUT", "status_code": resp.status_code, "error": _safe_json(resp)}


def _success_result(
    *,
    method: str,
    source: str,
    status_code: int,
    requested_deal_price: float,
    attempted_deal_price: float,
    body: dict,
    current_price_ref: float | None,
):
    applied = _response_applied_price(body, fallback=attempted_deal_price)
    return {
        "ok": True,
        "method": method,
        "source": source,
        "status_code": status_code,
        "requested_deal_price": _q2(requested_deal_price),
        "attempted_deal_price": _q2(attempted_deal_price),
        "final_deal_price": applied,
        "original_price": body.get("original_price") or current_price_ref,
        "adjusted_by_ml": abs(float(applied) - float(requested_deal_price)) > 0.01,
        "error_code": None,
        "message": None,
    }


def _error_result(
    *,
    method: str,
    source: str,
    status_code: int | None,
    requested_deal_price: float,
    attempted_deal_price: float,
    current_price_ref: float | None,
    err: dict | str | None,
):
    err_dict = err if isinstance(err, dict) else {}
    return {
        "ok": False,
        "method": method,
        "source": source,
        "status_code": status_code,
        "requested_deal_price": _q2(requested_deal_price),
        "attempted_deal_price": _q2(attempted_deal_price),
        "final_deal_price": None,
        "original_price": current_price_ref,
        "adjusted_by_ml": False,
        "error_code": _extract_error_code(err_dict),
        "message": (err_dict.get("message") if isinstance(err_dict, dict) else str(err)),
    }


def campaign_item_upsert_smart(
    session: requests.Session,
    connected_seller_id: int,
    campaign_id: str,
    item_id: str,
    requested_deal_price: float,
    promotion_type: str = "SELLER_CAMPAIGN",
    min_discount_percent: float = 5.0,
    auto_fix_min_discount: bool = True,
):
    """Upsert inteligente com precheck de campanha.

    Regras:
      - se o item já está na campanha alvo, prioriza PUT
      - se não está, tenta POST
      - se houver MINIMUM_DISCOUNT_PERCENT e auto_fix estiver ativo,
        recalcula o preço mínimo aceito e tenta novamente no mesmo método
      - em falhas não-terminais, tenta fallback entre POST/PUT
      - registra se o ML ajustou o preço solicitado
    """
    attempt_price = float(requested_deal_price)
    current_price_ref = get_item_current_price(session, item_id)
    already_in_campaign = is_item_in_campaign(
        session,
        connected_seller_id,
        item_id,
        campaign_id,
    )

    preferred_method = "PUT" if already_in_campaign else "POST"

    def run(method: str, price: float):
        if method == "PUT":
            return campaign_item_put(
                session,
                connected_seller_id,
                campaign_id,
                item_id,
                price,
                promotion_type=promotion_type,
            )
        return campaign_item_post(
            session,
            connected_seller_id,
            campaign_id,
            item_id,
            price,
            promotion_type=promotion_type,
        )

    def try_with_optional_min_discount_fix(method: str, price: float, source_prefix: str):
        r = run(method, price)
        if r.get("ok"):
            body = r.get("body") or {}
            return _success_result(
                method=method,
                source=source_prefix,
                status_code=r["status_code"],
                requested_deal_price=requested_deal_price,
                attempted_deal_price=price,
                body=body,
                current_price_ref=current_price_ref,
            )

        err = r.get("error") or {}
        code = _extract_error_code(err)

        if code == "MINIMUM_DISCOUNT_PERCENT" and auto_fix_min_discount:
            current_price = current_price_ref or get_item_current_price(session, item_id)
            if current_price:
                factor = (100.0 - float(min_discount_percent)) / 100.0
                fixed = _q2(current_price * factor)

                # Tenta ficar estritamente abaixo do limite quando necessário
                if fixed >= _q2(current_price * factor):
                    fixed = _q2(fixed - 0.01)

                r2 = run(method, fixed)
                if r2.get("ok"):
                    body2 = r2.get("body") or {}
                    out = _success_result(
                        method=method,
                        source=f"{source_prefix}_auto_fix_min_discount",
                        status_code=r2["status_code"],
                        requested_deal_price=requested_deal_price,
                        attempted_deal_price=fixed,
                        body=body2,
                        current_price_ref=current_price,
                    )
                    out["message"] = "AUTO_FIXED_MIN_DISCOUNT"
                    return out

                err2 = r2.get("error") or {}
                return _error_result(
                    method=method,
                    source=f"{source_prefix}_auto_fix_failed",
                    status_code=r2.get("status_code"),
                    requested_deal_price=requested_deal_price,
                    attempted_deal_price=fixed,
                    current_price_ref=current_price,
                    err=err2,
                )

        return _error_result(
            method=method,
            source=f"{source_prefix}_failed",
            status_code=r.get("status_code"),
            requested_deal_price=requested_deal_price,
            attempted_deal_price=price,
            current_price_ref=current_price_ref,
            err=err,
        )

    # 1) método preferido
    if preferred_method == "PUT":
        first = try_with_optional_min_discount_fix("PUT", attempt_price, "precheck_put")
        if first.get("ok"):
            return first

        # se falha por regra terminal, devolve
        if _is_terminal_price_rule(first.get("error_code")):
            return first

        # fallback POST
        second = try_with_optional_min_discount_fix("POST", attempt_price, "put_failed_then_post")
        if second.get("ok"):
            return second

        # se ambos falharam, prioriza o erro do fallback se houver mais contexto
        return second if second.get("message") else first

    # 2) POST preferido
    first = try_with_optional_min_discount_fix("POST", attempt_price, "post")
    if first.get("ok"):
        return first

    if _is_terminal_price_rule(first.get("error_code")):
        return first

    # fallback PUT
    second = try_with_optional_min_discount_fix("PUT", attempt_price, "post_failed_then_put")
    if second.get("ok"):
        return second

    return second if second.get("message") else first


def campaign_items_upsert_threaded(
    session: requests.Session,
    campaign_id: str,
    items_price: dict[str, float],
    threads: int = DEFAULT_THREADS,
    rps: int = DEFAULT_RPS,
    promotion_type: str = "SELLER_CAMPAIGN",
    min_discount_percent: float = 5.0,
    auto_fix_min_discount: bool = True,
    out_csv: str | None = None,
):
    limiter = RateLimiter(rps)

    rows = []
    lock = threading.Lock()

    def worker(item_id: str, price: float):
        limiter.wait()
        res = campaign_item_upsert_smart(
            session,
            campaign_id,
            item_id,
            price,
            promotion_type=promotion_type,
            min_discount_percent=min_discount_percent,
            auto_fix_min_discount=auto_fix_min_discount,
        )
        with lock:
            rows.append({"item_id": item_id, **res})
        return (item_id, res)

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker, iid, pr) for iid, pr in items_price.items()]
        done = 0
        total = len(items_price)
        print_lock = threading.Lock()

        for fut in as_completed(futures):
            iid, res = fut.result()
            results[iid] = res
            done += 1

            orig = res.get("original_price")
            requested = res.get("requested_deal_price")
            applied = res.get("final_deal_price")
            source = res.get("source", "na")

            if not res.get("ok"):
                status = f"ERRO: {res.get('error_code') or 'UNKNOWN'}"
            elif res.get("adjusted_by_ml"):
                status = "AJUSTADO_PELO_ML"
            else:
                status = "OK"

            orig_s = _fmt_price(orig)
            req_s = _fmt_price(requested)
            applied_s = _fmt_price(applied)

            with print_lock:
                print(
                    f"[{done}/{total}] {iid} | original: {orig_s} | "
                    f"requested: {req_s} | applied: {applied_s} | via: {source} | {status}"
                )

            if done % 25 == 0:
                print(f"itens processados: {done}/{total}")

    if out_csv:
        fieldnames = [
            "item_id",
            "ok",
            "method",
            "source",
            "status_code",
            "requested_deal_price",
            "attempted_deal_price",
            "final_deal_price",
            "original_price",
            "adjusted_by_ml",
            "error_code",
            "message",
        ]
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k) for k in fieldnames})

    return {
        "ok": True,
        "campaign_id": campaign_id,
        "items": len(items_price),
        "results": results,
        "out_csv": out_csv,
    }


# --- input parsers ------------------------------------------------------------

def parse_items_kv(items_list: list[str]) -> dict[str, float]:
    out = {}
    for s in items_list:
        if "=" not in s:
            raise ValueError(f"Item inválido '{s}'. Use MLB=preco (ex: MLB3929538765=295.40)")
        item_id, price = s.split("=", 1)
        out[item_id.strip()] = _parse_price_to_float(price.strip())
    return out


def read_csv_items_price(path: str) -> dict[str, float]:
    """CSV com colunas: item_id,deal_price (ou mlb,deal_price) / aceita também 'price'."""
    out: dict[str, float] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delim)
        for line_no, row in enumerate(reader, start=2):
            item_id = (row.get("item_id") or row.get("mlb") or "").strip()
            price_raw = (row.get("deal_price") or row.get("price") or "").strip()
            if not item_id or not price_raw:
                continue
            try:
                out[item_id] = _parse_price_to_float(price_raw)
            except Exception as e:
                print(f"[WARN] linha {line_no}: item_id={item_id} price='{price_raw}' inválido ({e}). Pulando.")
    return out


# --- CLI ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gestão de campanhas seller-promotions (Mercado Livre) - v4")
    parser.add_argument("--insecure", action="store_true", help="Desativa validação SSL (use só para debug)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("item-promos", help="Mostra promoções/campanhas disponíveis para um item")
    p1.add_argument("item_id", help="MLBxxxxx")

    p2 = sub.add_parser("upsert-items", help="Inclui/atualiza itens na campanha (deal_price)")
    parser.add_argument(
        "--connected-seller-id",
        type=int,
        required=True,
        help="ID do seller conectado na tabela ml.connected_seller"
    )
    p2.add_argument("campaign_id", help="Ex: C-MLB3295913")
    p2.add_argument("--items", nargs="+", required=True, help="Formato: MLB=deal_price (ex: MLB3929538765=295.40)")
    p2.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    p2.add_argument("--rps", type=int, default=DEFAULT_RPS)
    p2.add_argument("--min-discount", type=float, default=5.0, help="Desconto mínimo (%%). Default: 5")
    p2.add_argument("--no-auto-fix", action="store_true", help="Desativa correção automática do MINIMUM_DISCOUNT_PERCENT")
    p2.add_argument("--out", default=None, help="Caminho para salvar CSV de resultados (ex: results.csv)")

    p3 = sub.add_parser("upsert-items-csv", help="Inclui/atualiza itens na campanha a partir de CSV (item_id,deal_price)")
    p3.add_argument("campaign_id", help="Ex: C-MLB3295913")
    p3.add_argument("--csv", required=True, help="Caminho do CSV com colunas item_id,deal_price (ou mlb,deal_price)")
    p3.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    p3.add_argument("--rps", type=int, default=DEFAULT_RPS)
    p3.add_argument("--min-discount", type=float, default=5.0, help="Desconto mínimo (%%). Default: 5")
    p3.add_argument("--no-auto-fix", action="store_true", help="Desativa correção automática do MINIMUM_DISCOUNT_PERCENT")
    p3.add_argument("--out", default=None, help="Caminho para salvar CSV de resultados (ex: results.csv)")

    p4 = sub.add_parser("clone-campaign-prices", help="Clona os preços (price -> deal_price) da campanha origem para a campanha destino")
    p4.add_argument("destination_campaign_id", help="Campanha destino. Ex: C-MLB3295913")
    p4.add_argument("source_campaign_id", help="Campanha origem. Ex: C-MLB3295914")
    p4.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    p4.add_argument("--rps", type=int, default=DEFAULT_RPS)
    p4.add_argument("--min-discount", type=float, default=5.0, help="Desconto mínimo (%%). Default: 5")
    p4.add_argument("--no-auto-fix", action="store_true", help="Desativa correção automática do MINIMUM_DISCOUNT_PERCENT")
    p4.add_argument("--out", default=None, help="Caminho para salvar CSV de resultados (ex: results.csv)")
    p4.add_argument("--fetch-limit", type=int, default=50, help="Quantidade por página ao listar itens da campanha origem")

    args = parser.parse_args()
    connected_seller_id = args.connected_seller_id
    session = _mk_session(insecure=args.insecure)

    if args.cmd == "item-promos":
        print_item_promotions(session, args.item_id)
        return

    if args.cmd == "upsert-items":
        items_price = parse_items_kv(args.items)
        out = args.out or f"campaign_{args.campaign_id}_results.csv"
        campaign_items_upsert_threaded(
            session,
            args.campaign_id,
            items_price,
            threads=args.threads,
            rps=args.rps,
            min_discount_percent=args.min_discount,
            auto_fix_min_discount=(not args.no_auto_fix),
            out_csv=out,
        )
        return

    if args.cmd == "upsert-items-csv":
        items_price = read_csv_items_price(args.csv)
        out = args.out or f"campaign_{args.campaign_id}_results.csv"
        campaign_items_upsert_threaded(
            session,
            args.campaign_id,
            items_price,
            threads=args.threads,
            rps=args.rps,
            min_discount_percent=args.min_discount,
            auto_fix_min_discount=(not args.no_auto_fix),
            out_csv=out,
        )
        return

    if args.cmd == "clone-campaign-prices":
        out = args.out or f"campaign_clone_{args.source_campaign_id}_to_{args.destination_campaign_id}_results.csv"
        clone_campaign_prices(
            session,
            args.source_campaign_id,
            args.destination_campaign_id,
            threads=args.threads,
            rps=args.rps,
            min_discount_percent=args.min_discount,
            auto_fix_min_discount=(not args.no_auto_fix),
            out_csv=out,
            fetch_limit=args.fetch_limit,
        )
        return


if __name__ == "__main__":
    main()
