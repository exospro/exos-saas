from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, FileResponse
import json
import os
import re
import time
import subprocess
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

BACKGROUND_RUNS: dict[str, dict] = {}


def build_csv_path(prefix: str = "campaign_optimizer_audit") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return CSV_DIR / f"{prefix}_{timestamp}.csv"


def build_log_path(prefix: str = "pipeline_run") -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"{prefix}_{timestamp}.log"


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"


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


def extract_csv_name(text: str) -> str | None:
    match = re.search(r"campaign_optimizer_audit_[0-9_]+\.csv", text or "")
    return match.group(0) if match else None


def tail_text_file(path: Path, max_lines: int = 200) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:])
    except Exception as exc:
        return f"[erro ao ler log] {exc}"


def build_step_cmd(module_name: str, connected_seller_id: int, limit: int = 0, dry_run: bool = True, use_cost: bool = False, csv_path: Path | None = None) -> list[str]:
    if module_name == "etl.ml_inventory_snapshot_basic":
        cmd = ["python3", "-m", module_name, "--connected-seller-id", str(connected_seller_id)]
        if limit and limit > 0:
            cmd.extend(["--limit-items", str(limit)])
        return cmd

    if module_name == "etl.ml_item_promo_rebate_snapshot":
        cmd = ["python3", "-m", module_name, "--connected-seller-id", str(connected_seller_id)]
        if limit and limit > 0:
            cmd.extend(["--limit-items", str(limit)])
        return cmd

    if module_name == "etl.ml_campaign_optimizer":
        if csv_path is None:
            raise ValueError("csv_path é obrigatório para optimizer")
        cmd = [
            "python3",
            "-m",
            module_name,
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

    raise ValueError(f"Módulo não suportado: {module_name}")


def run_step_sync(module_name: str, status_label: str, connected_seller_id: int, limit: int = 0, dry_run: bool = True, use_cost: bool = False):
    csv_path = build_csv_path() if module_name == "etl.ml_campaign_optimizer" else None
    cmd = build_step_cmd(module_name, connected_seller_id, limit=limit, dry_run=dry_run, use_cost=use_cost, csv_path=csv_path)
    started = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)
    return JSONResponse(
        {
            "status": status_label,
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run if module_name == "etl.ml_campaign_optimizer" else None,
            "use_cost": use_cost if module_name == "etl.ml_campaign_optimizer" else None,
            "elapsed_seconds": elapsed,
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "csv_file": csv_path.name if csv_path and csv_path.exists() else extract_csv_name(result.stdout or result.stderr),
        }
    )


def start_background_full_run(connected_seller_id: int, limit: int, dry_run: bool, use_cost: bool) -> dict:
    run_id = new_run_id("pipeline")
    log_path = build_log_path(f"pipeline_{run_id}")
    csv_path = build_csv_path()

    inventory_cmd = build_step_cmd("etl.ml_inventory_snapshot_basic", connected_seller_id, limit=limit)
    rebate_cmd = build_step_cmd("etl.ml_item_promo_rebate_snapshot", connected_seller_id, limit=limit)
    optimizer_cmd = build_step_cmd("etl.ml_campaign_optimizer", connected_seller_id, limit=limit, dry_run=dry_run, use_cost=use_cost, csv_path=csv_path)

    log_file = log_path.open("w", encoding="utf-8")
    log_file.write(f"[PIPELINE] run_id={run_id}\n")
    log_file.write(f"[PIPELINE] started_at={datetime.now().isoformat()}\n")
    log_file.write(f"[PIPELINE] connected_seller_id={connected_seller_id} | limit={limit} | dry_run={dry_run} | use_cost={use_cost}\n\n")
    log_file.flush()

    script = "\n".join(
        [
            "set -euo pipefail",
            f"echo '[PIPELINE] Etapa 1/3 - Inventory: iniciando'",
            " ".join(subprocess.list2cmdline([arg]) if ' ' in arg else arg for arg in inventory_cmd),
            f"echo '[PIPELINE] Etapa 1/3 - Inventory: finalizada'",
            f"echo '[PIPELINE] Etapa 2/3 - Rebate: iniciando'",
            " ".join(subprocess.list2cmdline([arg]) if ' ' in arg else arg for arg in rebate_cmd),
            f"echo '[PIPELINE] Etapa 2/3 - Rebate: finalizada'",
            f"echo '[PIPELINE] Etapa 3/3 - Optimizer: iniciando'",
            " ".join(subprocess.list2cmdline([arg]) if ' ' in arg else arg for arg in optimizer_cmd),
            f"echo '[PIPELINE] Etapa 3/3 - Optimizer: finalizada'",
            f"echo '[PIPELINE] CSV esperado: {csv_path.name}'",
            f"echo '[PIPELINE] finished_at='$(date -Iseconds)",
        ]
    )

    proc = subprocess.Popen(
        ["bash", "-lc", script],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(BASE_DIR),
    )

    BACKGROUND_RUNS[run_id] = {
        "run_id": run_id,
        "process": proc,
        "log_path": str(log_path),
        "csv_path": str(csv_path),
        "csv_file": csv_path.name,
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "started_at": datetime.now().isoformat(),
        "log_file_handle": log_file,
    }

    return {
        "status": "execução iniciada em background",
        "run_id": run_id,
        "connected_seller_id": connected_seller_id,
        "limit": limit,
        "dry_run": dry_run,
        "use_cost": use_cost,
        "csv_file": csv_path.name,
        "log_file": log_path.name,
        "message": "Pipeline completo iniciado em sequência com log em tempo real.",
    }


def get_background_run_payload(run_id: str) -> dict:
    meta = BACKGROUND_RUNS.get(run_id)
    if not meta:
        raise HTTPException(status_code=404, detail="run_id não encontrado")

    proc = meta["process"]
    returncode = proc.poll()
    is_running = returncode is None

    if not is_running and meta.get("log_file_handle"):
        try:
            meta["log_file_handle"].flush()
            meta["log_file_handle"].close()
        except Exception:
            pass
        meta["log_file_handle"] = None

    csv_path = Path(meta["csv_path"])
    log_path = Path(meta["log_path"])

    return {
        "run_id": run_id,
        "is_running": is_running,
        "returncode": returncode,
        "connected_seller_id": meta["connected_seller_id"],
        "limit": meta["limit"],
        "dry_run": meta["dry_run"],
        "use_cost": meta["use_cost"],
        "csv_file": meta["csv_file"] if csv_path.exists() else None,
        "log_file": log_path.name,
        "started_at": meta["started_at"],
    }


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
def run_inventory(connected_seller_id: int = 1, limit: int = 0):
    return run_step_sync(
        "etl.ml_inventory_snapshot_basic",
        "inventory executado",
        connected_seller_id=connected_seller_id,
        limit=limit,
    )


@app.get("/run/rebate")
def run_rebate(connected_seller_id: int = 1, limit: int = 0):
    return run_step_sync(
        "etl.ml_item_promo_rebate_snapshot",
        "rebate executado",
        connected_seller_id=connected_seller_id,
        limit=limit,
    )


@app.get("/run/optimizer")
def run_optimizer(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    return run_step_sync(
        "etl.ml_campaign_optimizer",
        "executado",
        connected_seller_id=connected_seller_id,
        limit=limit,
        dry_run=dry_run,
        use_cost=use_cost,
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

    inventory = subprocess.run(
        build_step_cmd("etl.ml_inventory_snapshot_basic", connected_seller_id, limit=limit),
        capture_output=True,
        text=True,
    )
    results["inventory"] = inventory.stdout or inventory.stderr
    if inventory.returncode != 0:
        return JSONResponse({
            "status": "full run falhou no inventory",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": round(time.time() - started, 2),
            "results": results,
        })

    rebate = subprocess.run(
        build_step_cmd("etl.ml_item_promo_rebate_snapshot", connected_seller_id, limit=limit),
        capture_output=True,
        text=True,
    )
    results["rebate"] = rebate.stdout or rebate.stderr
    if rebate.returncode != 0:
        return JSONResponse({
            "status": "full run falhou no rebate",
            "connected_seller_id": connected_seller_id,
            "limit": limit,
            "dry_run": dry_run,
            "use_cost": use_cost,
            "elapsed_seconds": round(time.time() - started, 2),
            "results": results,
        })

    csv_path = build_csv_path()
    optimizer = subprocess.run(
        build_step_cmd("etl.ml_campaign_optimizer", connected_seller_id, limit=limit, dry_run=dry_run, use_cost=use_cost, csv_path=csv_path),
        capture_output=True,
        text=True,
    )
    results["optimizer"] = optimizer.stdout or optimizer.stderr

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


@app.get("/run/full_async")
def run_full_async(
    connected_seller_id: int = 1,
    limit: int = 0,
    dry_run: bool = True,
    use_cost: bool = False,
):
    return JSONResponse(start_background_full_run(connected_seller_id, limit, dry_run, use_cost))


@app.get("/run/status")
def run_status(run_id: str):
    return JSONResponse(get_background_run_payload(run_id))


@app.get("/run/log")
def run_log(run_id: str, lines: int = 200):
    meta = BACKGROUND_RUNS.get(run_id)
    if not meta:
        raise HTTPException(status_code=404, detail="run_id não encontrado")
    payload = get_background_run_payload(run_id)
    payload["log_tail"] = tail_text_file(Path(meta["log_path"]), max_lines=lines)
    return JSONResponse(payload)


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
            Para conectar uma nova conta, abra este painel com ?account_id=SEU_ID.
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
            * {{ box-sizing: border-box; }}
            body {{ margin: 0; font-family: Arial, sans-serif; background: linear-gradient(180deg, #06122b 0%, #091a3f 100%); color: #ffffff; }}
            .container {{ max-width: 1100px; margin: 0 auto; padding: 40px 20px 60px; }}
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
            .btn {{ width: 100%; border: none; border-radius: 14px; padding: 16px 18px; font-size: 17px; font-weight: 700; cursor: pointer; transition: transform .05s ease, opacity .2s ease; text-decoration: none; }}
            .btn:hover {{ opacity: .94; }}
            .btn:active {{ transform: scale(0.99); }}
            .btn-connect {{ background: #22c55e; color: #051b0d; }}
            .btn-primary {{ background: #3b82f6; color: #ffffff; }}
            .btn-secondary {{ background: #0f172a; color: #ffffff; border: 1px solid rgba(255,255,255,0.15); }}
            .btn-warning {{ background: #7c3aed; color: #ffffff; }}
            .form-row {{ margin-bottom: 14px; text-align: left; }}
            .form-row label {{ display: block; margin-bottom: 8px; font-weight: 700; color: #dbe6ff; }}
            input[type="number"], select {{ width: 220px; padding: 12px 14px; border-radius: 12px; border: none; font-size: 18px; }}
            .check {{ display: flex; align-items: center; gap: 10px; font-size: 18px; margin: 10px 0; }}
            .output-wrap {{ margin-top: 24px; }}
            .output-head {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 10px; }}
            .download-area {{ display: none; gap: 10px; flex-wrap: wrap; }}
            .download-area.show {{ display: flex; }}
            pre {{ white-space: pre-wrap; word-break: break-word; text-align: left; background: #020817; color: #dbeafe; border: 1px solid rgba(255,255,255,0.08); padding: 18px; border-radius: 16px; min-height: 220px; max-height: 560px; overflow: auto; font-size: 13px; line-height: 1.45; }}
            .muted {{ color: #9fb0d9; font-size: 14px; margin-top: 8px; }}
            #customLimitRow {{ display: none; }}
            a.button-link {{ text-decoration: none; display: block; }}
            @media (max-width: 820px) {{ .grid {{ grid-template-columns: 1fr; }} .hero h1 {{ font-size: 42px; }} }}
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
                        <button class="btn btn-secondary" onclick="rodarInventory()">Rodar inventory</button>
                        <button class="btn btn-secondary" onclick="rodarRebate()">Rodar rebate</button>
                        <button class="btn btn-primary" onclick="rodarOptimizer()">Rodar apenas optimizer</button>
                        <button class="btn btn-warning" onclick="rodarFull()">Rodar pipeline completo</button>
                    </div>

                    <div class="muted">
                        Em “Todos os anúncios”, o pipeline completo roda em background e o painel atualiza o log automaticamente.
                    </div>
                </div>
            </div>

            <div class="output-wrap">
                <div class="output-head">
                    <h2>Resultado / Log</h2>
                    <div id="downloadArea" class="download-area">
                        <a id="downloadLink" href="#" target="_blank">
                            <button class="btn btn-secondary" type="button">Baixar CSV de auditoria</button>
                        </a>
                        <a id="downloadLogLink" href="#" target="_blank">
                            <button class="btn btn-secondary" type="button">Baixar log</button>
                        </a>
                    </div>
                </div>

                <pre id="output">Nenhuma execução ainda.</pre>
            </div>
        </div>

        <script>
            let activeRunId = null;
            let pollTimer = null;

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
                    useCost: document.getElementById("usecost").checked,
                }};
            }}

            function setDownloads(data) {{
                const downloadArea = document.getElementById("downloadArea");
                const downloadLink = document.getElementById("downloadLink");
                const downloadLogLink = document.getElementById("downloadLogLink");
                let show = false;

                if (data && data.csv_file) {{
                    downloadLink.href = `/download/csv?filename=${{encodeURIComponent(data.csv_file)}}`;
                    show = true;
                }} else {{
                    downloadLink.href = "#";
                }}

                if (data && data.log_file) {{
                    downloadLogLink.href = `/download/log?filename=${{encodeURIComponent(data.log_file)}}`;
                    show = true;
                }} else {{
                    downloadLogLink.href = "#";
                }}

                if (show) downloadArea.classList.add("show");
                else downloadArea.classList.remove("show");
            }}

            function updateOutput(data) {{
                document.getElementById("output").innerText = JSON.stringify(data, null, 2);
                setDownloads(data);
            }}

            function updateLogOutput(data) {{
                const lines = [];
                lines.push(`run_id: ${{data.run_id}}`);
                lines.push(`running: ${{data.is_running}}`);
                if (data.returncode !== null && data.returncode !== undefined) lines.push(`returncode: ${{data.returncode}}`);
                if (data.csv_file) lines.push(`csv_file: ${{data.csv_file}}`);
                if (data.log_file) lines.push(`log_file: ${{data.log_file}}`);
                lines.push("\n--- LOG ---\n");
                lines.push(data.log_tail || "(sem log ainda)");
                document.getElementById("output").innerText = lines.join("\n");
                setDownloads(data);
            }}

            async function fetchAsTextOrJson(url) {{
                const res = await fetch(url);
                const text = await res.text();
                try {{
                    return JSON.parse(text);
                }} catch (e) {{
                    return {{ http_status: res.status, raw_response: text }};
                }}
            }}

            function stopPolling() {{
                if (pollTimer) {{
                    clearInterval(pollTimer);
                    pollTimer = null;
                }}
            }}

            async function pollRunLog() {{
                if (!activeRunId) return;
                const data = await fetchAsTextOrJson(`/run/log?run_id=${{encodeURIComponent(activeRunId)}}&lines=250`);
                updateLogOutput(data);
                if (!data.is_running) stopPolling();
            }}

            function startPolling(runId) {{
                activeRunId = runId;
                stopPolling();
                pollRunLog();
                pollTimer = setInterval(pollRunLog, 2500);
            }}

            async function rodarInventory() {{
                stopPolling();
                const p = getParams();
                document.getElementById("output").innerText = "Executando inventory...";
                const url = `/run/inventory?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`;
                const data = await fetchAsTextOrJson(url);
                updateOutput(data);
            }}

            async function rodarRebate() {{
                stopPolling();
                const p = getParams();
                document.getElementById("output").innerText = "Executando rebate...";
                const url = `/run/rebate?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}`;
                const data = await fetchAsTextOrJson(url);
                updateOutput(data);
            }}

            async function rodarOptimizer() {{
                stopPolling();
                const p = getParams();
                document.getElementById("output").innerText = "Executando optimizer...";
                const url = `/run/optimizer?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`;
                const data = await fetchAsTextOrJson(url);
                updateOutput(data);
            }}

            async function rodarFull() {{
                const p = getParams();
                const isAll = Number(p.limit) === 0;
                const endpoint = isAll ? "/run/full_async" : "/run/full";
                document.getElementById("output").innerText = isAll
                    ? "Iniciando pipeline completo em background..."
                    : "Executando pipeline completo...";
                const url = `${{endpoint}}?connected_seller_id=${{p.connectedSellerId}}&limit=${{p.limit}}&dry_run=${{p.dryRun}}&use_cost=${{p.useCost}}`;
                const data = await fetchAsTextOrJson(url);
                if (data.run_id) startPolling(data.run_id);
                else updateOutput(data);
            }}

            toggleLimitInput();
        </script>
    </body>
    </html>
    """
