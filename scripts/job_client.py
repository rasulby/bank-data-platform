"""Submit an external job and wait. Standard library only; no Spark imports."""

import argparse
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def request(path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    req = Request(
        os.environ["WORKER_URL"] + path,
        data=body,
        headers={
            "Authorization": "Bearer " + os.environ["WORKER_TOKEN"],
            "Content-Type": "application/json",
        },
    )
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def run(stage, day, fail=False):
    deadline = time.monotonic() + 1500
    while True:
        try:
            job = request("/jobs", {"stage": stage, "date": day, "fail": fail})
            break
        except HTTPError as error:
            if error.code == 409:
                reason = json.load(error)
                if reason.get("realtime"):
                    raise RuntimeError(reason["error"]) from None
            if error.code != 409 or time.monotonic() > deadline:
                raise
            time.sleep(5)
    deadline = time.monotonic() + 1500
    print(f"external job={job['id']} stage={stage} date={day}", flush=True)
    while time.monotonic() < deadline:
        job = request("/jobs/" + job["id"])
        if job["status"] != "running":
            print(job.get("log", ""), flush=True)
            if job["status"] != "success":
                raise RuntimeError(job.get("error", "External job failed"))
            return job
        time.sleep(3)
    raise TimeoutError("External job exceeded client timeout; inspect worker logs before retrying")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage")
    parser.add_argument("--date", required=True)
    parser.add_argument("--fail", choices=["true", "false"], default="false")
    args = parser.parse_args()
    run(args.stage, args.date, args.fail == "true")
