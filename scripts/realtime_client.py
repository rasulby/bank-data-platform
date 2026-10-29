"""Control the external continuous Spark process; no Spark dependency in Airflow."""

import json
import sys
import time
from urllib.error import HTTPError

from job_client import request


def main(action):
    if action == "start":
        job = request("/realtime/start", {})
        print("Realtime accepted. Startup/catch-up may take a minute. job_id=" + job["id"])
        print("Use make realtime-status and make realtime-logs; wait for a completed microbatch.")
    elif action == "stop":
        state = request("/realtime/stop", {})
        deadline = time.monotonic() + 300
        while state.get("active") and time.monotonic() < deadline:
            time.sleep(2)
            state = request("/realtime")
        if state.get("active"):
            raise TimeoutError(
                "Stop requested; a microbatch is still active. Check realtime-status; do not start a second writer."
            )
        print(json.dumps(state, indent=2))
        if state.get("status") == "failed":
            raise RuntimeError("Realtime failed; inspect logs before restarting")
    else:
        print(json.dumps(request("/realtime"), indent=2))


if __name__ == "__main__":
    try:
        main(sys.argv[1])
    except HTTPError as error:
        detail = json.load(error)
        raise SystemExit(detail.get("error", f"Worker HTTP {error.code}")) from None
