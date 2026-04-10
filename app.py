from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, FileResponse, PlainTextResponse
import os
import re
import time
import uuid
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

import requests
from psycopg2.extras import RealDictCursor

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

JOB_STORE_LOCK = threading.Lock()
JOB_STORE: dict[str, dict] = {}


def build_csv_path(prefix: str = "campaign_optimizer_audit") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return CSV_DIR / f"{prefix}_{timestamp}.csv"


def build_log_path(prefix: str = "pipeline_run") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{prefix}_{timestamp}.log"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_csv_name(text: str) -> str | None:
    match = re.search(r"campaign_optimizer_audit_[0-9_]+\.csv", text or "")
    return match.group(0) if match else None


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


def create_job(
    *,
    job_type: str,
    connected_seller_id: int,
    limit: int,
    dry_run: bool | None,
    use_cost: bool | None,
    log_file: str | None = None,
    csv_file: str | None = None,
) -> dict:
    run_id = uuid.uuid4().hex
    job = {
        "run_id": run_id,
        "job_type": job_type,
        "status": "queued",
        "step": None,
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "created_at": utc_now_iso(),
        "started_at": None,
        "finished_at": None,
        "elapsed_seconds": None,
        "log_file": log_file,
        "csv_file": csv_file,
        "error": None,
        "result": None,
    }
    with JOB_STORE_LOCK:
        JOB_STORE[run_id] = job
    return job


def update_job(run_id: str, **kwargs) -> None:
    with JOB_STORE_LOCK:
        job = JOB_STORE.get(run_id)
        if not job:
            return
        job.update(kwargs)


def get_job(run_id: str) -> dict:
    with JOB_STORE_LOCK:
        job = JOB_STORE.get(run_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"run_id {run_id} não encontrado")
        return dict(job)


def append_log(log_path: Path, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message.rstrip()}\n"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line)


def run_command_sync(cmd: list[str], log_path: Path, step_name: str) -> dict:
    started = time.time()
    append_log(log_path, f"[{step_name}] CMD: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)

    if result.stdout:
        for line in result.stdout.splitlines():
            append_log(log_path, f"[{step_name}][STDOUT] {line}")

    if result.stderr:
        for line in result.stderr.splitlines():
            append_log(log_path, f"[{step_name}][STDERR] {line}")

    append_log(
        log_path,
        f"[{step_name}] returncode={result.returncode} elapsed_seconds={elapsed}",
    )

    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "elapsed_seconds": elapsed,
    }


def build_inventory_cmd(connected_seller_id: int, limit: int) -> list[str]:
    cmd = [
        "python3",
        "-m",
        "etl.ml_inventory_snapshot_basic",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit and limit > 0:
        cmd.extend(["--limit-items", str(limit)])
    return cmd


def build_rebate_cmd(connected_seller_id: int, limit: int) -> list[str]:
    cmd = [
        "python3",
        "-m",
        "etl.ml_item_promo_rebate_snapshot",
        "--connected-seller-id",
        str(connected_seller_id),
    ]
    if limit and limit > 0:
        cmd.extend(["--limit-items", str(limit)])
    return cmd


def build_optimizer_cmd(
    connected_seller_id: int,
    limit: int,
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
    if limit and limit > 0:
        cmd.extend(["--limit", str(limit)])
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def worker_inventory_async(run_id: str, connected_seller_id: int, limit: int, log_path: Path):
    started = time.time()
    try:
        update_job(run_id, status="running", step="inventory", started_at=utc_now_iso())
        append_log(log_path, "[PIPELINE] Etapa única - Inventory: iniciando")

        result = run_command_sync(
            build_inventory_cmd(connected_seller_id, limit),
            log_path,
            "INVENTORY",
        )

        final_status = "finished" if result["returncode"] == 0 else "error"
        update_job(
            run_id,
            status=final_status,
            step="inventory",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            result={"inventory": result},
            error=result["stderr"] if result["returncode"] != 0 else None,
        )
        append_log(log_path, f"[PIPELINE] Inventory finalizado | status={final_status}")
    except Exception as e:
        append_log(log_path, f"[PIPELINE][ERRO] Inventory async falhou: {e}")
        update_job(
            run_id,
            status="error",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            error=str(e),
        )


def worker_rebate_async(run_id: str, connected_seller_id: int, limit: int, log_path: Path):
    started = time.time()
    try:
        update_job(run_id, status="running", step="rebate", started_at=utc_now_iso())
        append_log(log_path, "[PIPELINE] Etapa única - Rebate: iniciando")

        result = run_command_sync(
            build_rebate_cmd(connected_seller_id, limit),
            log_path,
            "REBATE",
        )

        final_status = "finished" if result["returncode"] == 0 else "error"
        update_job(
            run_id,
            status=final_status,
            step="rebate",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            result={"rebate": result},
            error=result["stderr"] if result["returncode"] != 0 else None,
        )
        append_log(log_path, f"[PIPELINE] Rebate finalizado | status={final_status}")
    except Exception as e:
        append_log(log_path, f"[PIPELINE][ERRO] Rebate async falhou: {e}")
        update_job(
            run_id,
            status="error",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            error=str(e),
        )


def worker_optimizer_async(
    run_id: str,
    connected_seller_id: int,
    limit: int,
    dry_run: bool,
    use_cost: bool,
    log_path: Path,
    csv_path: Path,
):
    started = time.time()
    try:
        update_job(run_id, status="running", step="optimizer", started_at=utc_now_iso())
        append_log(log_path, "[PIPELINE] Etapa única - Optimizer: iniciando")

        result = run_command_sync(
            build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path),
            log_path,
            "OPTIMIZER",
        )

        final_status = "finished" if result["returncode"] == 0 else "error"
        update_job(
            run_id,
            status=final_status,
            step="optimizer",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            result={"optimizer": result},
            error=result["stderr"] if result["returncode"] != 0 else None,
            csv_file=csv_path.name if csv_path.exists() else None,
        )
        append_log(log_path, f"[PIPELINE] Optimizer finalizado | status={final_status}")
    except Exception as e:
        append_log(log_path, f"[PIPELINE][ERRO] Optimizer async falhou: {e}")
        update_job(
            run_id,
            status="error",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            error=str(e),
        )


def worker_full_async(
    run_id: str,
    connected_seller_id: int,
    limit: int,
    dry_run: bool,
    use_cost: bool,
    log_path: Path,
    csv_path: Path,
):
    started = time.time()
    results = {}

    try:
        update_job(
            run_id,
            status="running",
            step="inventory",
            started_at=utc_now_iso(),
        )
        append_log(log_path, "[PIPELINE] Etapa 1/3 - Inventory: iniciando")
        inventory_result = run_command_sync(
            build_inventory_cmd(connected_seller_id, limit),
            log_path,
            "INVENTORY",
        )
        results["inventory"] = inventory_result
        if inventory_result["returncode"] != 0:
            raise RuntimeError("Inventory falhou")
        append_log(log_path, "[PIPELINE] Etapa 1/3 - Inventory: concluída")

        update_job(run_id, step="rebate", result=results)
        append_log(log_path, "[PIPELINE] Etapa 2/3 - Rebate: iniciando")
        rebate_result = run_command_sync(
            build_rebate_cmd(connected_seller_id, limit),
            log_path,
            "REBATE",
        )
        results["rebate"] = rebate_result
        if rebate_result["returncode"] != 0:
            raise RuntimeError("Rebate falhou")
        append_log(log_path, "[PIPELINE] Etapa 2/3 - Rebate: concluída")

        update_job(run_id, step="optimizer", result=results)
        append_log(log_path, "[PIPELINE] Etapa 3/3 - Optimizer: iniciando")
        optimizer_result = run_command_sync(
            build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path),
            log_path,
            "OPTIMIZER",
        )
        results["optimizer"] = optimizer_result
        if optimizer_result["returncode"] != 0:
            raise RuntimeError("Optimizer falhou")
        append_log(log_path, "[PIPELINE] Etapa 3/3 - Optimizer: concluída")

        update_job(
            run_id,
            status="finished",
            step="done",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            result=results,
            csv_file=csv_path.name if csv_path.exists() else None,
        )
        append_log(log_path, "[PIPELINE] Pipeline completo finalizado com sucesso")
    except Exception as e:
        append_log(log_path, f"[PIPELINE][ERRO] Pipeline falhou: {e}")
        update_job(
            run_id,
            status="error",
            finished_at=utc_now_iso(),
            elapsed_seconds=round(time.time() - started, 2),
            result=results,
            error=str(e),
            csv_file=csv_path.name if csv_path.exists() else None,
        )


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
            raise HTTPException(
                status_code=400,
                detail="account_id é obrigatório para conectar uma nova conta.",
            )
        state = f"seller:new:{account_id}"

    params = {
        "response_type": "code",
        "client_id": ML_CLIENT_ID,
        "redirect_uri": ML_REDIRECT_URI,
        "state": state,
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url)


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
        try:
            account_id = int(parts[2])
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid account_id in state")
    elif len(parts) == 2:
        try:
            connected_seller_id = int(parts[1])
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid connected_seller_id in state")
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
    expires_in = int(tokens["expires_in"])
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    me_resp = requests.get(
        ME_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )

    if me_resp.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Failed to fetch user: {me_resp.text}")

    me = me_resp.json()

    ml_user_id = me["id"]
    seller_nickname = me.get("nickname")
    site_id = me.get("site_id")

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if is_new:
                cur.execute(
                    """
                    SELECT id, account_id
                    FROM ml.connected_seller
                    WHERE ml_user_id = %s
                    """,
                    (ml_user_id,),
                )
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
                            account_id,
                            ml_user_id,
                            seller_nickname,
                            site_id,
                            status,
                            authorized_at,
                            created_at,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, 'active', now(), now(), now())
                        RETURNING id
                        """,
                        (account_id, ml_user_id, seller_nickname, site_id),
                    )
                    row = cur.fetchone()
                    connected_seller_id = int(row["id"])
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
                    raise HTTPException(
                        status_code=404,
                        detail=f"connected_seller_id {connected_seller_id} não encontrado",
                    )

                account_id = row["account_id"]

            cur.execute(
                """
                INSERT INTO ml.oauth_token (
                    connected_seller_id,
                    access_token,
                    refresh_token,
                    token_type,
                    scope,
                    expires_at,
                    last_refresh_at,
                    updated_at
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
                (
                    connected_seller_id,
                    access_token,
                    refresh_token,
                    token_type,
                    scope,
                    expires_at,
                ),
            )

        conn.commit()

    redirect_url = f"/painel?connected_seller_id={connected_seller_id}&connected=1"
    if account_id:
        redirect_url += f"&account_id={account_id}"

    return RedirectResponse(url=redirect_url)


@app.get("/run/inventory")
def run_inventory(
    connected_seller_id: int = 1,
    limit: int = 0,
):
    started = time.time()
    cmd = build_inventory_cmd(connected_seller_id, limit)
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "inventory executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "elapsed_seconds": elapsed,
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_file": None,
        }
    )


@app.get("/run/rebate")
def run_rebate(
    connected_seller_id: int = 1,
    limit: int = 0,
):
    started = time.time()
    cmd = build_rebate_cmd(connected_seller_id, limit)
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "rebate executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": None,
            "use_cost": None,
            "elapsed_seconds": elapsed,
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_file": None,
        }
    )


@app.get("/run/optimizer")
def run_optimizer(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    csv_path = build_csv_path()

    cmd = build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path)

    started = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": elapsed,
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_file": csv_path.name if csv_path.exists() else None,
        }
    )


@app.get("/run/full")
def run_full(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    started = time.time()
    results = {}

    inventory_result = subprocess.run(
        build_inventory_cmd(connected_seller_id, limit),
        capture_output=True,
        text=True,
    )
    results["inventory"] = inventory_result.stdout or inventory_result.stderr

    rebate_result = subprocess.run(
        build_rebate_cmd(connected_seller_id, limit),
        capture_output=True,
        text=True,
    )
    results["rebate"] = rebate_result.stdout or rebate_result.stderr

    csv_path = build_csv_path()
    optimizer_result = subprocess.run(
        build_optimizer_cmd(connected_seller_id, limit, dry_run, use_cost, csv_path),
        capture_output=True,
        text=True,
    )
    results["optimizer"] = optimizer_result.stdout or optimizer_result.stderr

    elapsed = round(time.time() - started, 2)

    return JSONResponse(
        {
            "status": "full run executado",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": elapsed,
            "csv_file": csv_path.name if csv_path.exists() else None,
            "results": results,
        }
    )


@app.get("/run/inventory_async")
def run_inventory_async(
    connected_seller_id: int = 1,
    limit: int = 0,
):
    log_path = build_log_path("inventory_run")
    job = create_job(
        job_type="inventory",
        connected_seller_id=connected_seller_id,
        limit=limit,
        dry_run=None,
        use_cost=None,
        log_file=log_path.name,
        csv_file=None,
    )

    thread = threading.Thread(
        target=worker_inventory_async,
        args=(job["run_id"], connected_seller_id, limit, log_path),
        daemon=True,
    )
    thread.start()

    return JSONResponse(
        {
            "status": "inventory iniciado em background",
            "run_id": job["run_id"],
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "log_file": log_path.name,
        }
    )


@app.get("/run/rebate_async")
def run_rebate_async(
    connected_seller_id: int = 1,
    limit: int = 0,
):
    log_path = build_log_path("rebate_run")
    job = create_job(
        job_type="rebate",
        connected_seller_id=connected_seller_id,
        limit=limit,
        dry_run=None,
        use_cost=None,
        log_file=log_path.name,
        csv_file=None,
    )

    thread = threading.Thread(
        target=worker_rebate_async,
        args=(job["run_id"], connected_seller_id, limit, log_path),
        daemon=True,
    )
    thread.start()

    return JSONResponse(
        {
            "status": "rebate iniciado em background",
            "run_id": job["run_id"],
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "log_file": log_path.name,
        }
    )


@app.get("/run/optimizer_async")
def run_optimizer_async(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    log_path = build_log_path("optimizer_run")
    csv_path = build_csv_path()

    job = create_job(
        job_type="optimizer",
        connected_seller_id=connected_seller_id,
        limit=limit,
        dry_run=dry_run,
        use_cost=use_cost,
        log_file=log_path.name,
        csv_file=csv_path.name,
    )

    thread = threading.Thread(
        target=worker_optimizer_async,
        args=(job["run_id"], connected_seller_id, limit, dry_run, use_cost, log_path, csv_path),
        daemon=True,
    )
    thread.start()

    return JSONResponse(
        {
            "status": "optimizer iniciado em background",
            "run_id": job["run_id"],
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "log_file": log_path.name,
            "csv_file": csv_path.name,
        }
    )


@app.get("/run/full_async")
def run_full_async(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    log_path = build_log_path("full_pipeline")
    csv_path = build_csv_path()

    job = create_job(
        job_type="full",
        connected_seller_id=connected_seller_id,
        limit=limit,
        dry_run=dry_run,
        use_cost=use_cost,
        log_file=log_path.name,
        csv_file=csv_path.name,
    )

    thread = threading.Thread(
        target=worker_full_async,
        args=(job["run_id"], connected_seller_id, limit, dry_run, use_cost, log_path, csv_path),
        daemon=True,
    )
    thread.start()

    return JSONResponse(
        {
            "status": "pipeline completo iniciado em background",
            "run_id": job["run_id"],
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "log_file": log_path.name,
            "csv_file": csv_path.name,
            "message": "Acompanhe em /run/status?run_id=... e /run/log?run_id=...",
        }
    )


@app.get("/run/status")
def run_status(run_id: str):
    job = get_job(run_id)
    return JSONResponse(job)


@app.get("/run/log")
def run_log(run_id: str):
    job = get_job(run_id)
    log_file = job.get("log_file")
    if not log_file:
        raise HTTPException(status_code=404, detail="Log não encontrado para este run_id")

    path = LOG_DIR / log_file
    if not path.exists():
        return PlainTextResponse("", status_code=200)

    text = path.read_text(encoding="utf-8")
    return PlainTextResponse(text)


@app.get("/download/csv")
def download_csv(filename: str):
    file_path = CSV_DIR / filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="CSV não encontrado")

    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="text/csv",
    )


@app.get("/download/log")
def download_log(filename: str):
    file_path = LOG_DIR / filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Log não encontrado")

    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="text/plain",
    )


@app.get("/painel", response_class=HTMLResponse)
def painel(
    connected_seller_id: int = 1,
    connected: int = 0,
    account_id: int | None = None,
):
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

    connected_banner = ""
    if connected == 1:
        connected_banner = """
        <div class="flash success">
            Conta conectada com sucesso.
        </div>
        """

    account_hint = ""
    if account_id:
        account_hint = f"""
        <div class="muted">
            Contexto da conta carregado. Account ID interno: {account_id}
        </div>
        """
    else:
        account_hint = """
        <div class="muted" style="color:#ffd7d7;">
            Para conectar nova conta, abra este painel com ?account_id=SEU_ID.
        </div>
        """

    new_connect_href = f"/ml/oauth/start?account_id={account_id}" if account_id else "#"
    reconnect_href = f"/ml/oauth/start?connected_seller_id={connected_seller_id}"

    return f"""
    <html>
    <head>
        <title>Exos SaaS</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            * {{
                box-sizing: border-box;
            }}
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: linear-gradient(180deg, #06122b 0%, #091a3f 100%);
                color: #ffffff;
            }}
            .container {{
                max-width: 1100px;
                margin: 0 auto;
                padding: 40px 20px 60px;
            }}
            .hero {{
                text-align: center;
                margin-bottom: 28px;
            }}
            .hero h1 {{
                font-size: 56px;
                margin: 0 0 10px;
                font-weight: 800;
            }}
            .hero p {{
                font-size: 20px;
                color: #d8e3ff;
                margin: 0;
            }}
            .flash {{
                max-width: 760px;
                margin: 0 auto 20px;
                padding: 14px 18px;
                border-radius: 14px;
                text-align: center;
                font-weight: 700;
            }}
            .flash.success {{
                background: rgba(34, 197, 94, 0.18);
                border: 1px solid rgba(34, 197, 94, 0.35);
                color: #d8ffe5;
            }}
            .grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
                align-items: start;
            }}
            .card {{
                background: rgba(255,255,255,0.08);
                border: 1px solid rgba(255,255,255,0.08);
                border-radius: 20px;
                padding: 24px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.22);
            }}
            .card h2 {{
                margin: 0 0 18px;
                font-size: 28px;
            }}
            .status-card {{
                border-radius: 16px;
                padding: 18px;
            }}
            .status-card.connected {{
                background: rgba(34, 197, 94, 0.14);
                border: 1px solid rgba(34, 197, 94, 0.28);
            }}
            .status-card.disconnected {{
                background: rgba(239, 68, 68, 0.12);
                border: 1px solid rgba(239, 68, 68, 0.25);
            }}
            .status-title {{
                font-size: 24px;
                font-weight: 800;
                margin-bottom: 10px;
            }}
            .status-line {{
                font-size: 16px;
                color: #e8eeff;
                margin-top: 6px;
            }}
            .actions {{
                display: flex;
                flex-direction: column;
                gap: 12px;
                margin-top: 16px;
            }}
            .btn {{
                width: 100%;
                border: none;
                border-radius: 14px;
                padding: 16px 18px;
                font-size: 18px;
                font-weight: 700;
                cursor: pointer;
                transition: transform .05s ease, opacity .2s ease;
                text-decoration: none;
            }}
            .btn:hover {{
                opacity: .94;
            }}
            .btn:active {{
                transform: scale(0.99);
            }}
            .btn-connect {{
                background: #22c55e;
                color: #051b0d;
            }}
            .btn-primary {{
                background: #3b82f6;
                color: #ffffff;
            }}
            .btn-secondary {{
                background: #0f172a;
                color: #ffffff;
                border: 1px solid rgba(255,255,255,0.15);
            }}
            .btn-warn {{
                background: #f59e0b;
                color: #1a1200;
            }}
            .form-row {{
                margin-bottom: 14px;
                text-align: left;
            }}
            .form-row label {{
                display: block;
                margin-bottom: 8px;
                font-weight: 700;
                color: #dbe6ff;
            }}
            input[type="number"], select {{
                width: 220px;
                padding: 12px 14px;
                border-radius: 12px;
                border: none;
                font-size: 18px;
            }}
            .check {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 18px;
                margin: 10px 0;
            }}
            .output-wrap {{
                margin-top: 24px;
            }}
            .output-head {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 12px;
                flex-wrap: wrap;
                margin-bottom: 10px;
            }}
            .download-area {{
                display: none;
                gap: 10px;
                flex-wrap: wrap;
            }}
            .download-area.show {{
                display: flex;
            }}
            pre {{
                white-space: pre-wrap;
                word-break: break-word;
                text-align: left;
                background: #020817;
                color: #dbeafe;
                border: 1px solid rgba(255,255,255,0.08);
                padding: 18px;
                border-radius: 16px;
                min-height: 220px;
                max-height: 600px;
                overflow: auto;
                font-size: 13px;
                line-height: 1.45;
            }}
            .muted {{
                color: #9fb0d9;
                font-size: 14px;
                margin-top: 8px;
            }}
            .small-grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 10px;
            }}
            #customLimitRow {{
                display: none;
            }}
            a.button-link {{
                text-decoration: none;
                display: block;
            }}
            @media (max-width: 820px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}
                .small-grid {{
                    grid-template-columns: 1fr;
                }}
                .hero h1 {{
                    font-size: 42px;
                }}
            }}
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
                        <a class="button-link" href="{new_connect_href}" onclick="return validarNovaConta();">
                            <button class="btn btn-connect" type="button">Conectar nova conta Mercado Livre</button>
                        </a>

                        <a class="button-link" href="{reconnect_href}">
                            <button class="btn btn-secondary" type="button">Reconectar seller atual</button>
                        </a>
                    </div>
                </div>

                <div class="card">
                    <h2>Executar otimização</h2>

                    <div class="form-row">
                        <label for="connectedSellerId">Connected Seller ID</label>
                        <input type="number" id="connectedSellerId" value="{connected_seller_id}" />
                    </div>

                    <div class="form-row">
                        <label for="limitMode">Escopo</label>
                        <select id="limitMode" onchange="toggleLimitInput()">
                            <option value="50">50 anúncios</option>
                            <option value="100">100 anúncios</option>
                            <option value="custom">Quantidade personalizada</option>
                            <option value="all">Todos os anúncios</option>
                        </select>
                    </div>

                    <div class="form-row" id="customLimitRow">
                        <label for="limit">Quantidade personalizada</label>
                        <input type="number" id="limit" value="50" />
                    </div>

                    <div class="check">
                        <input type="checkbox" id="dryrun" checked />
                        <label for="dryrun">Simulação (não altera campanhas)</label>
                    </div>

                    <div class="check">
                        <input type="checkbox" id="usecost" />
                        <label for="usecost">Usar custo do produto</label>
                    </div>

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

                    <div class="muted">
                        Para todos os anúncios, prefira os modos async.
                    </div>

                    <div class="muted" id="jobInfo"></div>
                </div>
            </div>

            <div class="output-wrap">
                <div class="output-head">
                    <h2>Resultado / Log</h2>
                    <div id="downloadArea" class="download-area">
                        <a id="downloadCsvLink" href="#" target="_blank">
                            <button class="btn btn-secondary" type="button">Baixar CSV</button>
                        </a>
                        <a id="downloadLogLink" href="#" target="_blank">
                            <button class="btn btn-secondary" type="button">Baixar LOG</button>
                        </a>
                    </div>
                </div>

                <pre id="output">Nenhuma execução ainda.</pre>
            </div>
        </div>

        <script>
            let pollingTimer = null;
            let currentRunId = null;

            function validarNovaConta() {{
                const hasAccount = {str(bool(account_id)).lower()};
                if (!hasAccount) {{
                    alert("Abra o painel com ?account_id=SEU_ID antes de conectar uma nova conta.");
                    return false;
                }}
                return true;
            }}

            function toggleLimitInput() {{
                const mode = document.getElementById("limitMode").value;
                const row = document.getElementById("customLimitRow");
                row.style.display = mode === "custom" ? "block" : "none";
            }}

            function getLimitValue() {{
                const mode = document.getElementById("limitMode").value;
                if (mode === "all") return 0;
                if (mode === "custom") return document.getElementById("limit").value || 0;
                return mode;
            }}

            function getParams() {{
                return {{
                    connectedSellerId: document.getElementById("connectedSellerId").value,
                    limit: getLimitValue(),
                    dryRun: document.getElementById("dryrun").checked,
                    useCost: document.getElementById("usecost").checked
                }};
            }}

            function setDownloads(csvFile, logFile) {{
                const area = document.getElementById("downloadArea");
                const csvLink = document.getElementById("downloadCsvLink");
                const logLink = document.getElementById("downloadLogLink");

                let show = false;

                if (csvFile) {{
                    csvLink.href = `/download/csv?filename=${{encodeURIComponent(csvFile)}}`;
                    csvLink.style.display = "inline-block";
                    show = true;
                }} else {{
                    csvLink.href = "#";
                    csvLink.style.display = "none";
                }}

                if (logFile) {{
                    logLink.href = `/download/log?filename=${{encodeURIComponent(logFile)}}`;
                    logLink.style.display = "inline-block";
                    show = true;
                }} else {{
                    logLink.href = "#";
                    logLink.style.display = "none";
                }}

                area.classList.toggle("show", show);
            }}

            function setOutput(text) {{
                document.getElementById("output").innerText = text;
            }}

            function setJobInfo(text) {{
                document.getElementById("jobInfo").innerText = text || "";
            }}

            async function fetchJson(url) {{
                const res = await fetch(url);
                const text = await res.text();
                try {{
                    return JSON.parse(text);
                }} catch (e) {{
                    throw new Error(`HTTP ${{res.status}}\\n\\n${{text}}`);
                }}
            }}

            async function fetchText(url) {{
                const res = await fetch(url);
                return await res.text();
            }}

            function stopPolling() {{
                if (pollingTimer) {{
                    clearInterval(pollingTimer);
                    pollingTimer = null;
                }}
            }}

            async function pollRun(runId) {{
                try {{
                    const status = await fetchJson(`/run/status?run_id=${{encodeURIComponent(runId)}}`);
                    const logText = await fetchText(`/run/log?run_id=${{encodeURIComponent(runId)}}`);

                    setOutput(logText || JSON.stringify(status, null, 2));
                    setJobInfo(
                        `run_id=${{status.run_id}} | tipo=${{status.job_type}} | status=${{status.status}} | etapa=${{status.step || "-"}}`
                    );
                    setDownloads(status.csv_file, status.log_file);

                    if (status.status === "finished" || status.status === "error") {{
                        stopPolling();
                    }}
                }} catch (err) {{
                    setOutput(String(err));
                    stopPolling();
                }}
            }}

            function startPolling(runId) {{
                currentRunId = runId;
                stopPolling();
                pollRun(runId);
                pollingTimer = setInterval(() => pollRun(runId), 3000);
            }}

            async function runSync(url, startMessage) {{
                stopPolling();
                setJobInfo("");
                setOutput(startMessage);
                try {{
                    const data = await fetchJson(url);
                    setOutput(JSON.stringify(data, null, 2));
                    setDownloads(data.csv_file || null, null);
                }} catch (err) {{
                    setOutput(String(err));
                }}
            }}

            async function runAsync(url, startMessage) {{
                stopPolling();
                setOutput(startMessage);
                try {{
                    const data = await fetchJson(url);
                    setJobInfo(`run_id=${{data.run_id}} | status=${{data.status}}`);
                    setDownloads(data.csv_file || null, data.log_file || null);
                    setOutput(JSON.stringify(data, null, 2));
                    if (data.run_id) {{
                        startPolling(data.run_id);
                    }}
                }} catch (err) {{
                    setOutput(String(err));
                }}
            }}

            async function rodarInventorySync() {{
                const p = getParams();
                await runSync(
                    `/run/inventory?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`,
                    "Executando inventory..."
                );
            }}

            async function rodarInventoryAsync() {{
                const p = getParams();
                await runAsync(
                    `/run/inventory_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`,
                    "Disparando inventory em background..."
                );
            }}

            async function rodarRebateSync() {{
                const p = getParams();
                await runSync(
                    `/run/rebate?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`,
                    "Executando rebate..."
                );
            }}

            async function rodarRebateAsync() {{
                const p = getParams();
                await runAsync(
                    `/run/rebate_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`,
                    "Disparando rebate em background..."
                );
            }}

            async function rodarOptimizerSync() {{
                const p = getParams();
                await runSync(
                    `/run/optimizer?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`,
                    "Executando optimizer..."
                );
            }}

            async function rodarOptimizerAsync() {{
                const p = getParams();
                await runAsync(
                    `/run/optimizer_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`,
                    "Disparando optimizer em background..."
                );
            }}

            async function rodarFullSync() {{
                const p = getParams();
                await runSync(
                    `/run/full?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`,
                    "Executando pipeline completo..."
                );
            }}

            async function rodarFullAsync() {{
                const p = getParams();
                await runAsync(
                    `/run/full_async?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`,
                    "Disparando pipeline completo em background..."
                );
            }}

            toggleLimitInput();
        </script>
    </body>
    </html>
    """