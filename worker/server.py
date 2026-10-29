"""Single-job HTTP runner. Airflow submits work; Spark runs in this container.

Requests accept stage names, never arbitrary shell commands. Job state and logs
survive service restarts. Only one job can run at a time on a small laptop.
"""

import hmac
import json
import os
import subprocess
import threading
import time
import uuid
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

STATE = Path("/state/jobs")
LOCK = threading.Lock()
REALTIME_MODE = False
REALTIME_STATE = Path("/state/realtime.json")
REALTIME_STOP = Path("/state/realtime.stop")
STAGES = {
    "setup",
    "seed",
    "ingest",
    "transform",
    "load",
    "dbt",
    "quality",
    "publish",
    "change",
    "verify",
}


def commands(stage, day, fail):
    if fail and stage == "ingest":
        return [["python", "-c", 'raise RuntimeError("Deliberate ingestion failure")']]
    if stage == "setup":
        return [["python", "-m", "jobs.setup"]]
    if stage in {"seed", "change"}:
        return [["python", "-m", "jobs.source", stage, "--date", day]]
    if stage in {"ingest", "transform", "load", "verify"}:
        command = ["spark-submit", "--driver-memory", "1g", f"/app/jobs/{stage}.py"]
        return [command + (["--date", day] if stage != "ingest" else [])]
    if stage == "dbt":
        common = ["--project-dir", "/app/transformation", "--profiles-dir", "/app/transformation"]
        return [["dbt", "run", *common], ["dbt", "test", *common]]
    command = ["python", "-m", "jobs.control", stage, "--date", day]
    return [command + (["--fail"] if fail and stage == "quality" else [])]


def save(job):
    target = STATE / f"{job['id']}.json"
    temp = target.with_suffix(".tmp")
    temp.write_text(json.dumps(job))
    temp.replace(target)


def execute(job):
    try:
        with (STATE / f"{job['id']}.log").open("w") as log:
            log.write(f"start stage={job['stage']} date={job['date']}\n")
            log.flush()
            deadline = time.monotonic() + 1200
            for command in commands(job["stage"], job["date"], job["fail"]):
                process = subprocess.Popen(
                    command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
                )
                try:
                    code = process.wait(timeout=max(1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    import signal

                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                    raise TimeoutError("Job exceeded 20 minutes") from None
                if code:
                    raise RuntimeError(f"Process failed with exit code {code}")
            log.write(f"end stage={job['stage']} status=success\n")
        job["status"] = "success"
    except Exception as error:
        job["status"] = "failed"
        job["error"] = str(error)
    finally:
        save(job)
        LOCK.release()


def realtime_status():
    state = (
        json.loads(REALTIME_STATE.read_text()) if REALTIME_STATE.exists() else {"status": "stopped"}
    )
    return {**state, "active": REALTIME_MODE, "stop_requested": REALTIME_STOP.exists()}


def execute_realtime(job):
    global REALTIME_MODE
    try:
        with (STATE / f"{job['id']}.log").open("w") as log:
            process = subprocess.Popen(
                ["spark-submit", "--driver-memory", "1g", "/app/jobs/realtime.py"],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            code = process.wait()
            if code:
                raise RuntimeError(
                    f"Realtime process exited with code {code}; inspect realtime logs"
                )
        job["status"] = "success"
    except Exception as error:
        job.update(status="failed", error=str(error))
    finally:
        save(job)
        state = json.loads(REALTIME_STATE.read_text()) if REALTIME_STATE.exists() else {}
        state.update(
            status="stopped" if job["status"] == "success" else "failed",
            error=job.get("error"),
            job_id=job["id"],
        )
        temp = REALTIME_STATE.with_suffix(".tmp")
        temp.write_text(json.dumps(state, indent=2))
        temp.replace(REALTIME_STATE)
        REALTIME_MODE = False
        LOCK.release()


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, value, content_type="application/json"):
        payload = (
            json.dumps(value).encode() if content_type == "application/json" else value.encode()
        )
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        self.wfile.write(payload)

    def authorized(self):
        return hmac.compare_digest(
            self.headers.get("Authorization", ""), "Bearer " + os.environ["WORKER_TOKEN"]
        )

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self.reply(200, {"status": "ok"})
            return
        if path == "/metrics":
            jobs = [json.loads(p.read_text()) for p in STATE.glob("*.json")]
            lines = ["# TYPE bank_jobs_total gauge"]
            for status in ["running", "success", "failed"]:
                lines.append(
                    f'bank_jobs_total{{status="{status}"}} {sum(j["status"] == status for j in jobs)}'
                )
            realtime = realtime_status()
            lines.extend(
                [
                    "# TYPE bank_realtime_active gauge",
                    f"bank_realtime_active {int(realtime['active'])}",
                ]
            )
            if realtime.get("last_completed_at"):
                completed = datetime.fromisoformat(realtime["last_completed_at"]).timestamp()
                lines.extend(
                    [
                        "# TYPE bank_realtime_last_completed_seconds gauge",
                        f"bank_realtime_last_completed_seconds {completed}",
                        "# TYPE bank_realtime_last_batch_rows gauge",
                        f"bank_realtime_last_batch_rows {realtime.get('input_rows', 0)}",
                    ]
                )
            self.reply(200, "\n".join(lines) + "\n", "text/plain")
            return
        if not self.authorized():
            self.reply(401, {"error": "Unauthorized"})
            return
        if path == "/realtime":
            self.reply(200, realtime_status())
            return
        job_id = path.removeprefix("/jobs/")
        if (
            path.startswith("/jobs/")
            and len(job_id) == 32
            and all(c in "0123456789abcdef" for c in job_id)
        ):
            target = STATE / f"{job_id}.json"
            if target.exists():
                result = json.loads(target.read_text())
                logfile = STATE / f"{job_id}.log"
                result["log"] = (
                    logfile.read_text(errors="replace")[-24000:] if logfile.exists() else ""
                )
                self.reply(200, result)
                return
        self.reply(404, {"error": "Not found"})

    def do_POST(self):
        global REALTIME_MODE
        if not self.authorized():
            self.reply(401, {"error": "Unauthorized"})
            return
        if self.path == "/realtime/stop":
            if REALTIME_MODE:
                REALTIME_STOP.touch()
            self.reply(200, realtime_status())
            return
        if self.path == "/realtime/start":
            if not LOCK.acquire(blocking=False):
                self.reply(409, {"error": "A batch job or realtime is already running"})
                return
            REALTIME_STOP.unlink(missing_ok=True)
            REALTIME_MODE = True
            job = {
                "id": uuid.uuid4().hex,
                "stage": "realtime",
                "date": "all",
                "fail": False,
                "status": "running",
            }
            save(job)
            temporary = REALTIME_STATE.with_suffix(".tmp")
            temporary.write_text(json.dumps({"status": "starting", "job_id": job["id"]}))
            temporary.replace(REALTIME_STATE)
            threading.Thread(target=execute_realtime, args=(job,), daemon=True).start()
            self.reply(202, job)
            return
        if self.path != "/jobs":
            self.reply(404, {"error": "Not found"})
            return
        try:
            size = int(self.headers.get("Content-Length", 0))
            if not 0 < size <= 4096:
                raise ValueError("Invalid body length")
            body = json.loads(self.rfile.read(size))
            stage = body["stage"]
            day = date.fromisoformat(body["date"]).isoformat()
            if stage not in STAGES or not isinstance(body.get("fail", False), bool):
                raise ValueError("Invalid stage or failure flag")
        except (ValueError, KeyError, TypeError) as error:
            self.reply(400, {"error": str(error)})
            return
        if not LOCK.acquire(blocking=False):
            self.reply(
                409,
                {
                    "error": "Stop realtime with make realtime-stop before running batch jobs"
                    if REALTIME_MODE
                    else "Another job is running; retry later",
                    "realtime": REALTIME_MODE,
                },
            )
            return
        job = {
            "id": uuid.uuid4().hex,
            "stage": stage,
            "date": day,
            "fail": body.get("fail", False),
            "status": "running",
        }
        save(job)
        threading.Thread(target=execute, args=(job,), daemon=True).start()
        self.reply(202, job)


if __name__ == "__main__":
    STATE.mkdir(parents=True, exist_ok=True)
    for path in STATE.glob("*.json"):
        previous = json.loads(path.read_text())
        if previous["status"] == "running":
            previous.update(
                status="failed", error="Worker restarted during job; retry from Airflow"
            )
            save(previous)
    if REALTIME_STATE.exists():
        previous = json.loads(REALTIME_STATE.read_text())
        if previous.get("status") in {"running", "starting"}:
            previous.update(
                status="interrupted",
                error="Worker restarted; use make realtime-start to resume checkpoint",
            )
            REALTIME_STATE.write_text(json.dumps(previous))
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
