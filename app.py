from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse, PlainTextResponse
import os
import re
import time
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests
from psycopg2.extras import Json, RealDictCursor

from etl.inventory.repository import db_connect

app = FastAPI()

ML_CLIENT_ID = os.environ["ML_CLIENT_ID"]
ML_CLIENT_SECRET = os.environ["ML_CLIENT_SECRET"]
ML_REDIRECT_URI = os.environ["ML_REDIRECT_URI"]

AUTH_URL = "https://auth.mercadolivre.com.br/authorization"
TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ME_URL = "https://api.mercadolibre.com/users/me"

BASE_DIR = Path("/opt/render/project/src")
CSV_DIR = BASE_DIR / "runtime_csv"
LOG_DIR = BASE_DIR / "runtime_logs"
CSV_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)


def build_csv_path(prefix: str = "campaign_optimizer_audit") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return CSV_DIR / f"{prefix}_{timestamp}.csv"


def build_log_path(prefix: str = "pipeline_run") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{prefix}_{timestamp}.log"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_connected_seller_summary(connected_seller_id: int) -> dict:
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    cs.id,
                    cs.account_id,
                    cs.ml_user_id,
                    cs.seller_nickname,
                    cs.site_id,
                    cs.status,
                    cs.authorized_at,
                    ot.expires_at,
                    ot.updated_at AS token_updated_at
                FROM ml.connected_seller cs
                LEFT JOIN ml.oauth_token ot
                  ON ot.connected_seller_id = cs.id
                WHERE cs.id = %s
                """,
                (connected_seller_id,),
            )
            row = cur.fetchone()

    if not row:
        return {
            "exists": False,
            "connected": False,
            "connected_seller_id": connected_seller_id,
        }

    connected = bool(row["ml_user_id"]) and row["status"] == "active"

    return {
        "exists": True,
        "connected": connected,
        "connected_seller_id": row["id"],
        "account_id": row["account_id"],
        "ml_user_id": row["ml_user_id"],
        "seller_nickname": row["seller_nickname"],
        "site_id": row["site_id"],
        "status": row["status"],
        "authorized_at": row["authorized_at"],
        "expires_at": row["expires_at"],
        "token_updated_at": row["token_updated_at"],
    }


def insert_job(
    *,
    job_type: str,
    connected_seller_id: int,
    limit_items: int,
    dry_run: bool | None,
    use_cost: bool | None,
    payload: dict,
    log_file: str | None,
    csv_file: str | None,
) -> str:
    run_id = os.urandom(16).hex()
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app.async_job (
                    run_id,
                    job_type,
                    status,
                    step,
                    connected_seller_id,
                    limit_items,
                    dry_run,
                    use_cost,
                    payload_json,
                    log_file,
                    csv_file,
                    result_json,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, 'queued', NULL, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                """,
                (
                    run_id,
                    job_type,
                    connected_seller_id,
                    limit_items,
                    dry_run,
                    use_cost,
                    Json(payload),
                    log_file,
                    csv_file,
                    Json({}),
                ),
            )
        conn.commit()
    return run_id


def get_job(run_id: str) -> dict:
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    run_id,
                    job_type,
                    status,
                    step,
                    connected_seller_id,
                    limit_items,
                    dry_run,
                    use_cost,
                    payload_json,
                    result_json,
                    log_file,
                    csv_file,
                    error,
                    created_at,
                    started_at,
                    finished_at,
                    updated_at
                FROM app.async_job
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"run_id {run_id} não encontrado")

    out = dict(row)
    if out.get("payload_json") is None:
        out["payload_json"] = {}
    if out.get("result_json") is None:
        out["result_json"] = {}
    return out


def build_inventory_cmd(connected_seller_id: int, limit_items: int) -> list[str]:
    cmd = [
        "python3",
        "-m",
        "etl.ml_inventory_snapshot_basic",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit_items and limit_items > 0:
        cmd.extend(["--limit-items", str(limit_items)])
    return cmd


def build_rebate_cmd(connected_seller_id: int, limit_items: int) -> list[str]:
    cmd = [
        "python3",
        "-m",
        "etl.ml_item_promo_rebate_snapshot",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit_items and limit_items > 0:
        cmd.extend(["--limit-items", str(limit_items)])
    return cmd


def build_optimizer_cmd(
    connected_seller_id: int,
    limit_items: int,
    dry_run: bool,
    use_cost: bool,
    csv_path: Path,
) -> list[str]:
    cmd = [
        "python3",
        "-m",
        "etl.ml_campaign_optimizer",
        "--connected-seller-id",
        str(connected_seller_id),
        "--use-cost",
        "true" if use_cost else "false",
        "--out",
        str(csv_path),
    ]
    if limit_items and limit_items > 0:
        cmd.extend(["--limit", str(limit_items)])
    if dry_run:
        cmd.append("--dry-run")
    return cmd


@app.get("/")
def root():
    return RedirectResponse(url="/painel")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ml/oauth/start")
def start_oauth(
    connected_seller_id: int | None = None,
    account_id: int | None = None,
):
    if connected_seller_id:
        state = f"seller:{connected_seller_id}"
    else:
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id é obrigatório para conectar uma nova conta.")
        state = f"seller:new:{account_id}"

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
    }
    return RedirectResponse(f"{AUTH_URL}?{urlencode(params)}")


@app.get("/ml/oauth/callback")
async def oauth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")

    if not code:
        raise HTTPException(status_code=400, detail="Missing code")
    if not state or not state.startswith("seller:"):
        raise HTTPException(status_code=400, detail="Invalid state")

    connected_seller_id: int | None = None
    account_id: int | None = None
    is_new = False

    parts = state.split(":")
    if len(parts) == 3 and parts[1] == "new":
        is_new = True
        account_id = int(parts[2])
    elif len(parts) == 2:
        connected_seller_id = int(parts[1])
    else:
        raise HTTPException(status_code=400, detail="Invalid state format")

    token_resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": ML_CLIENT_ID,
            "client_secret": ML_CLIENT_SECRET,
            "code": code,
            "redirect_uri": ML_REDIRECT_URI,
        },
        timeout=30,
    )
    if token_resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Token error: {token_resp.text}")

    tokens = token_resp.json()
    access_token = tokens["access_token"]
    refresh_token = tokens["refresh_token"]
    token_type = tokens.get("token_type", "Bearer")
    scope = tokens.get("scope")
    expires_at = utc_now() + timedelta(seconds=int(tokens["expires_in"]))

    me_resp = requests.get(ME_URL, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if me_resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to fetch user: {me_resp.text}")

    me = me_resp.json()
    ml_user_id = me["id"]
    seller_nickname = me.get("nickname")
    site_id = me.get("site_id")

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if is_new:
                cur.execute("SELECT id, account_id FROM ml.connected_seller WHERE ml_user_id = %s", (ml_user_id,))
                existing = cur.fetchone()
                if existing:
                    connected_seller_id = int(existing["id"])
                    account_id = int(existing["account_id"])
                    cur.execute(
                        """
                        UPDATE ml.connected_seller
                           SET seller_nickname = %s,
                               site_id = %s,
                               status = 'active',
                               authorized_at = now(),
                               updated_at = now()
                         WHERE id = %s
                        """,
                        (seller_nickname, site_id, connected_seller_id),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO ml.connected_seller (
                            account_id, ml_user_id, seller_nickname, site_id, status, authorized_at, created_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, 'active', now(), now(), now())
                        RETURNING id
                        """,
                        (account_id, ml_user_id, seller_nickname, site_id),
                    )
                    connected_seller_id = int(cur.fetchone()["id"])
            else:
                cur.execute(
                    """
                    UPDATE ml.connected_seller
                       SET ml_user_id = %s,
                           seller_nickname = %s,
                           site_id = %s,
                           status = 'active',
                           authorized_at = now(),
                           updated_at = now()
                     WHERE id = %s
                    RETURNING id, account_id
                    """,
                    (ml_user_id, seller_nickname, site_id, connected_seller_id),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"connected_seller_id {connected_seller_id} não encontrado")
                account_id = row["account_id"]

            cur.execute(
                """
                INSERT INTO ml.oauth_token (
                    connected_seller_id, access_token, refresh_token, token_type, scope, expires_at, last_refresh_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (connected_seller_id)
                DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    token_type = EXCLUDED.token_type,
                    scope = EXCLUDED.scope,
                    expires_at = EXCLUDED.expires_at,
                    last_refresh_at = now(),
                    updated_at = now()
                """,
                (connected_seller_id, access_token, refresh_token, token_type, scope, expires_at),
            )
        conn.commit()

    redirect_url = f"/painel?connected_seller_id={connected_seller_id}&connected=1"
    if account_id:
        redirect_url += f"&account_id={account_id}"
    return RedirectResponse(url=redirect_url)


@app.get("/run/inventory")
def run_inventory(connected_seller_id: int = 1, limit: int = 0):
    started = time.time()
    cmd = build_inventory_cmd(connected_seller_id, limit)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "status": "inventory executado",
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "elapsed_seconds": round(time.time() - started, 2),
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "csv_file": None,
    }


@app.get("/run/rebate")
def run_rebate(connected_seller_id: int = 1, limit: int = 0):
    started = time.time()
    cmd = build_rebate_cmd(connected_seller_id, limit)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "status": "rebate executado",
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "elapsed_seconds": round(time.time() - started, 2),
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "csv_file": None,
    }


@app.get("/run/optimizer")
def run_optimizer(connected_seller_id: int = 1, limit: int = 0, dry_run: bool = True, use_cost: bool = False):
    started = time.time()
    csv_path = build_csv_path()
    cmd = build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "status": "optimizer executado",
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "elapsed_seconds": round(time.time() - started, 2),
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "csv_file": csv_path.name if csv_path.exists() else None,
    }


@app.get("/run/full")
def run_full(connected_seller_id: int = 1, limit: int = 0, dry_run: bool = True, use_cost: bool = False):
    started = time.time()
    results = {}
    r1 = subprocess.run(build_inventory_cmd(connected_seller_id, limit), capture_output=True, text=True)
    results["inventory"] = r1.stdout or r1.stderr
    r2 = subprocess.run(build_rebate_cmd(connected_seller_id, limit), capture_output=True, text=True)
    results["rebate"] = r2.stdout or r2.stderr
    csv_path = build_csv_path()
    r3 = subprocess.run(build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path), capture_output=True, text=True)
    results["optimizer"] = r3.stdout or r3.stderr
    return {
        "status": "full run executado",
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "elapsed_seconds": round(time.time() - started, 2),
        "csv_file": csv_path.name if csv_path.exists() else None,
        "results": results,
    }


@app.get("/run/inventory_async")
def run_inventory_async(connected_seller_id: int = 1, limit: int = 0):
    log_path = build_log_path("inventory_run")
    run_id = insert_job(
        job_type="inventory",
        connected_seller_id=connected_seller_id,
        limit_items=limit,
        dry_run=None,
        use_cost=None,
        payload={"cmd": build_inventory_cmd(connected_seller_id, limit)},
        log_file=log_path.name,
        csv_file=None,
    )
    return {"status": "inventory enfileirado", "run_id": run_id, "connected_seller_id": connected_seller_id, "limit": limit, "log_file": log_path.name}


@app.get("/run/rebate_async")
def run_rebate_async(connected_seller_id: int = 1, limit: int = 0):
    log_path = build_log_path("rebate_run")
    run_id = insert_job(
        job_type="rebate",
        connected_seller_id=connected_seller_id,
        limit_items=limit,
        dry_run=None,
        use_cost=None,
        payload={"cmd": build_rebate_cmd(connected_seller_id, limit)},
        log_file=log_path.name,
        csv_file=None,
    )
    return {"status": "rebate enfileirado", "run_id": run_id, "connected_seller_id": connected_seller_id, "limit": limit, "log_file": log_path.name}


@app.get("/run/optimizer_async")
def run_optimizer_async(connected_seller_id: int = 1, limit: int = 0, dry_run: bool = True, use_cost: bool = False):
    log_path = build_log_path("optimizer_run")
    csv_path = build_csv_path()
    run_id = insert_job(
        job_type="optimizer",
        connected_seller_id=connected_seller_id,
        limit_items=limit,
        dry_run=dry_run,
        use_cost=use_cost,
        payload={"cmd": build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path)},
        log_file=log_path.name,
        csv_file=csv_path.name,
    )
    return {
        "status": "optimizer enfileirado",
        "run_id": run_id,
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "log_file": log_path.name,
        "csv_file": csv_path.name,
    }


@app.get("/run/full_async")
def run_full_async(connected_seller_id: int = 1, limit: int = 0, dry_run: bool = True, use_cost: bool = False):
    log_path = build_log_path("full_pipeline")
    csv_path = build_csv_path()
    run_id = insert_job(
        job_type="full",
        connected_seller_id=connected_seller_id,
        limit_items=limit,
        dry_run=dry_run,
        use_cost=use_cost,
        payload={
            "inventory_cmd": build_inventory_cmd(connected_seller_id, limit),
            "rebate_cmd": build_rebate_cmd(connected_seller_id, limit),
            "optimizer_cmd": build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path),
        },
        log_file=log_path.name,
        csv_file=csv_path.name,
    )
    return {
        "status": "pipeline completo enfileirado",
        "run_id": run_id,
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "log_file": log_path.name,
        "csv_file": csv_path.name,
    }


@app.get("/run/status")
def run_status(run_id: str):
    return get_job(run_id)


@app.get("/run/log")
def run_log(run_id: str):
    job = get_job(run_id)
    log_file = job.get("log_file")
    if not log_file:
        raise HTTPException(status_code=404, detail="Log não encontrado para este run_id")
    path = LOG_DIR / log_file
    if not path.exists():
        return PlainTextResponse("", status_code=200)
    return PlainTextResponse(path.read_text(encoding="utf-8"))


@app.get("/download/csv")
def download_csv(filename: str):
    file_path = CSV_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="CSV não encontrado")
    return FileResponse(path=str(file_path), filename=filename, media_type="text/csv")


@app.get("/download/log")
def download_log(filename: str):
    file_path = LOG_DIR / filename
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Log não encontrado")
    return FileResponse(path=str(file_path), filename=filename, media_type="text/plain")


@app.get("/jobs/recent")
def recent_jobs(limit: int = 20, connected_seller_id: int | None = None):
    sql = """
    SELECT
        run_id, job_type, status, step, connected_seller_id,
        limit_items, dry_run, use_cost, log_file, csv_file,
        error, created_at, started_at, finished_at, updated_at
    FROM app.async_job
    """
    params = []
    if connected_seller_id is not None:
        sql += " WHERE connected_seller_id = %s"
        params.append(connected_seller_id)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return {"jobs": rows}


@app.get("/painel", response_class=HTMLResponse)
def painel(connected_seller_id: int = 1, connected: int = 0, account_id: int | None = None):
    seller = get_connected_seller_summary(connected_seller_id)

    if seller["connected"]:
        status_html = f"""
        <div class="status-card connected">
            <div class="status-title">✅ Conectado</div>
            <div class="status-line"><strong>Conta:</strong> {seller.get("seller_nickname") or "-"}</div>
            <div class="status-line"><strong>ML User ID:</strong> {seller.get("ml_user_id") or "-"}</div>
            <div class="status-line"><strong>Site:</strong> {seller.get("site_id") or "-"}</div>
            <div class="status-line"><strong>Connected Seller ID:</strong> {seller.get("connected_seller_id") or "-"}</div>
        </div>
        """
    else:
        status_html = """
        <div class="status-card disconnected">
            <div class="status-title">❌ Não conectado</div>
            <div class="status-line">Conecte sua conta do Mercado Livre para liberar a execução.</div>
        </div>
        """

    connected_banner = '<div class="flash success">Conta conectada com sucesso.</div>' if connected == 1 else ''
    account_hint = (
        f'<div class="muted">Contexto da conta carregado. Account ID interno: {account_id}</div>'
        if account_id else '<div class="muted" style="color:#ffd7d7;">Para conectar nova conta, abra este painel com ?account_id=SEU_ID.</div>'
    )
    new_connect_href = f"/ml/oauth/start?account_id={account_id}" if account_id else "#"
    reconnect_href = f"/ml/oauth/start?connected_seller_id={connected_seller_id}"

    return f"""
    <html>
    <head>
        <title>Exos SaaS</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            * {{ box-sizing: border-box; }}
            body {{ margin: 0; font-family: Arial, sans-serif; background: linear-gradient(180deg, #06122b 0%, #091a3f 100%); color: #ffffff; }}
            .container {{ max-width: 1120px; margin: 0 auto; padding: 40px 20px 60px; }}
            .hero {{ text-align: center; margin-bottom: 28px; }}
            .hero h1 {{ font-size: 56px; margin: 0 0 10px; font-weight: 800; }}
            .hero p {{ font-size: 20px; color: #d8e3ff; margin: 0; }}
            .flash {{ max-width: 760px; margin: 0 auto 20px; padding: 14px 18px; border-radius: 14px; text-align: center; font-weight: 700; }}
            .flash.success {{ background: rgba(34, 197, 94, 0.18); border: 1px solid rgba(34, 197, 94, 0.35); color: #d8ffe5; }}
            .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; align-items: start; }}
            .card {{ background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.08); border-radius: 20px; padding: 24px; box-shadow: 0 10px 30px rgba(0,0,0,0.22); }}
            .card h2 {{ margin: 0 0 18px; font-size: 28px; }}
            .status-card {{ border-radius: 16px; padding: 18px; }}
            .status-card.connected {{ background: rgba(34, 197, 94, 0.14); border: 1px solid rgba(34, 197, 94, 0.28); }}
            .status-card.disconnected {{ background: rgba(239, 68, 68, 0.12); border: 1px solid rgba(239, 68, 68, 0.25); }}
            .status-title {{ font-size: 24px; font-weight: 800; margin-bottom: 10px; }}
            .status-line {{ font-size: 16px; color: #e8eeff; margin-top: 6px; }}
            .actions {{ display: flex; flex-direction: column; gap: 12px; margin-top: 16px; }}
            .btn {{ width: 100%; border: none; border-radius: 14px; padding: 16px 18px; font-size: 18px; font-weight: 700; cursor: pointer; transition: transform .05s ease, opacity .2s ease; text-decoration: none; }}
            .btn:hover {{ opacity: .94; }}
            .btn:active {{ transform: scale(0.99); }}
            .btn-connect {{ background: #22c55e; color: #051b0d; }}
            .btn-primary {{ background: #3b82f6; color: #ffffff; }}
            .btn-secondary {{ background: #0f172a; color: #ffffff; border: 1px solid rgba(255,255,255,0.15); }}
            .btn-warn {{ background: #f59e0b; color: #1a1200; }}
            .form-row {{ margin-bottom: 14px; text-align: left; }}
            .form-row label {{ display: block; margin-bottom: 8px; font-weight: 700; color: #dbe6ff; }}
            input[type="number"], select {{ width: 220px; padding: 12px 14px; border-radius: 12px; border: none; font-size: 18px; }}
            .check {{ display: flex; align-items: center; gap: 10px; font-size: 18px; margin: 10px 0; }}
            .output-wrap {{ margin-top: 24px; }}
            .output-head {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 10px; }}
            .download-area {{ display: none; gap: 10px; flex-wrap: wrap; }}
            .download-area.show {{ display: flex; }}
            pre {{ white-space: pre-wrap; word-break: break-word; text-align: left; background: #020817; color: #dbeafe; border: 1px solid rgba(255,255,255,0.08); padding: 18px; border-radius: 16px; min-height: 240px; max-height: 620px; overflow: auto; font-size: 13px; line-height: 1.45; }}
            .muted {{ color: #9fb0d9; font-size: 14px; margin-top: 8px; }}
            .small-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
            .job-list {{ margin-top: 16px; font-size: 13px; color: #d8e3ff; }}
            .job-item {{ padding: 10px 12px; border-radius: 10px; background: rgba(2,8,23,0.55); margin-bottom: 8px; }}
            #customLimitRow {{ display: none; }}
            a.button-link {{ text-decoration: none; display: block; }}
            @media (max-width: 820px) {{ .grid {{ grid-template-columns: 1fr; }} .small-grid {{ grid-template-columns: 1fr; }} .hero h1 {{ font-size: 42px; }} }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="hero">
                <h1>🚀 Exos SaaS</h1>
                <p>Otimize suas campanhas automaticamente com segurança</p>
            </div>
            {connected_banner}
            <div class="grid">
                <div class="card">
                    <h2>Conta Mercado Livre</h2>
                    {status_html}
                    {account_hint}
                    <div class="actions">
                        <a class="button-link" href="{new_connect_href}" onclick="return validarNovaConta();"><button class="btn btn-connect" type="button">Conectar nova conta Mercado Livre</button></a>
                        <a class="button-link" href="{reconnect_href}"><button class="btn btn-secondary" type="button">Reconectar seller atual</button></a>
                    </div>
                    <div class="job-list">
                        <div><strong>Últimos jobs</strong></div>
                        <div id="recentJobs">Carregando...</div>
                    </div>
                </div>
                <div class="card">
                    <h2>Executar otimização</h2>
                    <div class="form-row"><label for="connectedSellerId">Connected Seller ID</label><input type="number" id="connectedSellerId" value="{connected_seller_id}" /></div>
                    <div class="form-row"><label for="limitMode">Escopo</label><select id="limitMode" onchange="toggleLimitInput()"><option value="50">50 anúncios</option><option value="100">100 anúncios</option><option value="custom">Quantidade personalizada</option><option value="all">Todos os anúncios</option></select></div>
                    <div class="form-row" id="customLimitRow"><label for="limit">Quantidade personalizada</label><input type="number" id="limit" value="50" /></div>
                    <div class="check"><input type="checkbox" id="dryrun" checked /><label for="dryrun">Simulação (não altera campanhas)</label></div>
                    <div class="check"><input type="checkbox" id="usecost" /><label for="usecost">Usar custo do produto</label></div>
                    <div class="actions">
                        <div class="small-grid">
                            <button class="btn btn-secondary" onclick="rodarInventorySync()">Inventory sync</button>
                            <button class="btn btn-secondary" onclick="rodarInventoryAsync()">Inventory async</button>
                            <button class="btn btn-secondary" onclick="rodarRebateSync()">Rebate sync</button>
                            <button class="btn btn-secondary" onclick="rodarRebateAsync()">Rebate async</button>
                            <button class="btn btn-primary" onclick="rodarOptimizerSync()">Optimizer sync</button>
                            <button class="btn btn-primary" onclick="rodarOptimizerAsync()">Optimizer async</button>
                        </div>
                        <button class="btn btn-warn" onclick="rodarFullSync()">Pipeline completo sync</button>
                        <button class="btn btn-connect" onclick="rodarFullAsync()">Pipeline completo async</button>
                    </div>
                    <div class="muted">Para todos os anúncios, prefira os modos async.</div>
                    <div class="muted" id="jobInfo"></div>
                </div>
            </div>
            <div class="output-wrap">
                <div class="output-head">
                    <h2>Resultado / Log</h2>
                    <div id="downloadArea" class="download-area">
                        <a id="downloadCsvLink" href="#" target="_blank"><button class="btn btn-secondary" type="button">Baixar CSV</button></a>
                        <a id="downloadLogLink" href="#" target="_blank"><button class="btn btn-secondary" type="button">Baixar LOG</button></a>
                    </div>
                </div>
                <pre id="output">Nenhuma execução ainda.</pre>
            </div>
        </div>
        <script>
            let pollingTimer = null;
            function validarNovaConta() {{
                const hasAccount = {str(bool(account_id)).lower()};
                if (!hasAccount) {{ alert("Abra o painel com ?account_id=SEU_ID antes de conectar uma nova conta."); return false; }}
                return true;
            }}
            function toggleLimitInput() {{
                const mode = document.getElementById("limitMode").value;
                document.getElementById("customLimitRow").style.display = mode === "custom" ? "block" : "none";
            }}
            function getLimitValue() {{
                const mode = document.getElementById("limitMode").value;
                if (mode === "all") return 0;
                if (mode === "custom") return document.getElementById("limit").value || 0;
                return mode;
            }}
            function getParams() {{
                return {{ connectedSellerId: document.getElementById("connectedSellerId").value, limit: getLimitValue(), dryRun: document.getElementById("dryrun").checked, useCost: document.getElementById("usecost").checked }};
            }}
            function setOutput(text) {{ document.getElementById("output").innerText = text; }}
            function setJobInfo(text) {{ document.getElementById("jobInfo").innerText = text || ""; }}
            function setDownloads(csvFile, logFile) {{
                const area = document.getElementById("downloadArea");
                const csvLink = document.getElementById("downloadCsvLink");
                const logLink = document.getElementById("downloadLogLink");
                let show = false;
                if (csvFile) {{ csvLink.href = `/download/csv?filename=${{encodeURIComponent(csvFile)}}`; csvLink.style.display = "inline-block"; show = true; }} else {{ csvLink.style.display = "none"; }}
                if (logFile) {{ logLink.href = `/download/log?filename=${{encodeURIComponent(logFile)}}`; logLink.style.display = "inline-block"; show = true; }} else {{ logLink.style.display = "none"; }}
                area.classList.toggle("show", show);
            }}
            async function fetchJson(url) {{ const res = await fetch(url); const text = await res.text(); try {{ return JSON.parse(text); }} catch (e) {{ throw new Error(`HTTP ${{res.status}}\n\n${{text}}`); }} }}
            async function fetchText(url) {{ const res = await fetch(url); return await res.text(); }}
            function stopPolling() {{ if (pollingTimer) {{ clearInterval(pollingTimer); pollingTimer = null; }} }}
            async function refreshRecentJobs() {{
                try {{
                    const sellerId = document.getElementById("connectedSellerId").value;
                    const data = await fetchJson(`/jobs/recent?connected_seller_id=${{sellerId}}&limit=8`);
                    const root = document.getElementById("recentJobs");
                    const jobs = data.jobs || [];
                    if (!jobs.length) {{ root.innerHTML = "Nenhum job recente."; return; }}
                    root.innerHTML = jobs.map(j => `<div class="job-item"><div><strong>${{j.job_type}}</strong> | status=${{j.status}} | step=${{j.step || "-"}}</div><div>run_id=${{j.run_id}}</div><div>criado_em=${{j.created_at}}</div></div>`).join("");
                }} catch (e) {{ document.getElementById("recentJobs").innerText = String(e); }}
            }}
            async function pollRun(runId) {{
                try {{
                    const status = await fetchJson(`/run/status?run_id=${{encodeURIComponent(runId)}}`);
                    const logText = await fetchText(`/run/log?run_id=${{encodeURIComponent(runId)}}`);
                    setOutput(logText || JSON.stringify(status, null, 2));
                    setJobInfo(`run_id=${{status.run_id}} | tipo=${{status.job_type}} | status=${{status.status}} | etapa=${{status.step || "-"}}`);
                    setDownloads(status.csv_file, status.log_file);
                    await refreshRecentJobs();
                    if (status.status === "finished" || status.status === "error") stopPolling();
                }} catch (err) {{ setOutput(String(err)); stopPolling(); }}
            }}
            function startPolling(runId) {{ stopPolling(); pollRun(runId); pollingTimer = setInterval(() => pollRun(runId), 3000); }}
            async function runSync(url, message) {{ stopPolling(); setJobInfo(""); setOutput(message); try {{ const data = await fetchJson(url); setOutput(JSON.stringify(data, null, 2)); setDownloads(data.csv_file || null, null); await refreshRecentJobs(); }} catch (err) {{ setOutput(String(err)); }} }}
            async function runAsync(url, message) {{ stopPolling(); setOutput(message); try {{ const data = await fetchJson(url); setOutput(JSON.stringify(data, null, 2)); setJobInfo(`run_id=${{data.run_id}} | status=${{data.status}}`); setDownloads(data.csv_file || null, data.log_file || null); await refreshRecentJobs(); if (data.run_id) startPolling(data.run_id); }} catch (err) {{ setOutput(String(err)); }} }}
            async function rodarInventorySync() {{ const p = getParams(); await runSync(`/run/inventory?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`, "Executando inventory..."); }}
            async function rodarInventoryAsync() {{ const p = getParams(); await runAsync(`/run/inventory_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`, "Enfileirando inventory..."); }}
            async function rodarRebateSync() {{ const p = getParams(); await runSync(`/run/rebate?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`, "Executando rebate..."); }}
            async function rodarRebateAsync() {{ const p = getParams(); await runAsync(`/run/rebate_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`, "Enfileirando rebate..."); }}
            async function rodarOptimizerSync() {{ const p = getParams(); await runSync(`/run/optimizer?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`, "Executando optimizer..."); }}
            async function rodarOptimizerAsync() {{ const p = getParams(); await runAsync(`/run/optimizer_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`, "Enfileirando optimizer..."); }}
            async function rodarFullSync() {{ const p = getParams(); await runSync(`/run/full?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`, "Executando pipeline completo..."); }}
            async function rodarFullAsync() {{ const p = getParams(); await runAsync(`/run/full_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`, "Enfileirando pipeline completo..."); }}
            toggleLimitInput();
            refreshRecentJobs();
        </script>
    </body>
    </html>
    """
