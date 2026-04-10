#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import Json, RealDictCursor

from etl.inventory.repository import db_connect

BASE_DIR = Path("/opt/render/project/src")
CSV_DIR = BASE_DIR / "runtime_csv"
LOG_DIR = BASE_DIR / "runtime_logs"
CSV_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

POLL_INTERVAL_SEC = float(os.environ.get("ASYNC_JOB_POLL_INTERVAL_SEC", "5"))
WORKER_NAME = os.environ.get("ASYNC_JOB_WORKER_NAME", "worker-1")


def append_log(log_path: Path, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message.rstrip()}\n")


def claim_next_job() -> dict | None:
    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                WITH next_job AS (
                    SELECT run_id
                    FROM app.async_job
                    WHERE status = 'queued'
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE app.async_job j
                   SET status = 'running',
                       started_at = now(),
                       updated_at = now(),
                       step = COALESCE(step, 'starting')
                  FROM next_job
                 WHERE j.run_id = next_job.run_id
                RETURNING j.run_id, j.job_type, j.payload_json, j.log_file, j.csv_file
                """
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row) if row else None


def update_job(
    run_id: str,
    *,
    status: str | None = None,
    step: str | None = None,
    error: str | None = None,
    result_json: dict | None = None,
    csv_file: str | None = None,
    finished: bool = False,
) -> None:
    sets = ["updated_at = now()"]
    params = []
    if status is not None:
        sets.append("status = %s")
        params.append(status)
    if step is not None:
        sets.append("step = %s")
        params.append(step)
    if error is not None:
        sets.append("error = %s")
        params.append(error)
    if result_json is not None:
        sets.append("result_json = %s")
        params.append(Json(result_json))
    if csv_file is not None:
        sets.append("csv_file = %s")
        params.append(csv_file)
    if finished:
        sets.append("finished_at = now()")
    params.append(run_id)
    sql = f"UPDATE app.async_job SET {', '.join(sets)} WHERE run_id = %s"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def run_command(cmd: list[str], log_path: Path, label: str) -> dict:
    started = time.time()
    append_log(log_path, f"[{label}] CMD: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = round(time.time() - started, 2)
    if result.stdout:
        for line in result.stdout.splitlines():
            append_log(log_path, f"[{label}][STDOUT] {line}")
    if result.stderr:
        for line in result.stderr.splitlines():
            append_log(log_path, f"[{label}][STDERR] {line}")
    append_log(log_path, f"[{label}] returncode={result.returncode} elapsed_seconds={elapsed}")
    return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr, "elapsed_seconds": elapsed}


def run_single_job(job: dict) -> None:
    run_id = job["run_id"]
    job_type = job["job_type"]
    payload = job.get("payload_json") or {}
    log_path = LOG_DIR / (job.get("log_file") or f"{run_id}.log")
    append_log(log_path, f"[WORKER] worker={WORKER_NAME} | run_id={run_id} | job_type={job_type} | iniciando")
    result_json: dict = {}
    try:
        if job_type == "inventory":
            update_job(run_id, step="inventory")
            result = run_command(payload["cmd"], log_path, "INVENTORY")
            result_json["inventory"] = result
            if result["returncode"] != 0:
                raise RuntimeError("Inventory falhou")
        elif job_type == "rebate":
            update_job(run_id, step="rebate")
            result = run_command(payload["cmd"], log_path, "REBATE")
            result_json["rebate"] = result
            if result["returncode"] != 0:
                raise RuntimeError("Rebate falhou")
        elif job_type == "optimizer":
            update_job(run_id, step="optimizer")
            result = run_command(payload["cmd"], log_path, "OPTIMIZER")
            result_json["optimizer"] = result
            if result["returncode"] != 0:
                raise RuntimeError("Optimizer falhou")
        elif job_type == "full":
            update_job(run_id, step="inventory", result_json=result_json)
            append_log(log_path, "[PIPELINE] Etapa 1/3 - Inventory: iniciando")
            r1 = run_command(payload["inventory_cmd"], log_path, "INVENTORY")
            result_json["inventory"] = r1
            if r1["returncode"] != 0:
                raise RuntimeError("Inventory falhou")
            update_job(run_id, step="rebate", result_json=result_json)
            append_log(log_path, "[PIPELINE] Etapa 2/3 - Rebate: iniciando")
            r2 = run_command(payload["rebate_cmd"], log_path, "REBATE")
            result_json["rebate"] = r2
            if r2["returncode"] != 0:
                raise RuntimeError("Rebate falhou")
            update_job(run_id, step="optimizer", result_json=result_json)
            append_log(log_path, "[PIPELINE] Etapa 3/3 - Optimizer: iniciando")
            r3 = run_command(payload["optimizer_cmd"], log_path, "OPTIMIZER")
            result_json["optimizer"] = r3
            if r3["returncode"] != 0:
                raise RuntimeError("Optimizer falhou")
        else:
            raise RuntimeError(f"job_type inválido: {job_type}")

        update_job(run_id, status="finished", step="done", result_json=result_json, csv_file=job.get("csv_file"), finished=True)
        append_log(log_path, f"[WORKER] run_id={run_id} finalizado com sucesso")
    except Exception as e:
        append_log(log_path, f"[WORKER][ERRO] run_id={run_id} | {e}")
        update_job(run_id, status="error", error=str(e), result_json=result_json, finished=True)


def main() -> None:
    print(f"[WORKER] iniciado | nome={WORKER_NAME} | poll_interval={POLL_INTERVAL_SEC}s", flush=True)
    while True:
        try:
            job = claim_next_job()
            if not job:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            run_single_job(job)
        except Exception as e:
            print(f"[WORKER][ERRO] loop principal: {e}", flush=True)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
