"""Host-side sampler for this Compose project's database and tool containers."""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import signal
import subprocess
from threading import Event
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--project", required=True, help="Compose project name (docker compose -p)")
    parser.add_argument("--seconds", type=float, default=400)
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error("Duration must be finite and positive")
    stopped = Event()
    signal.signal(signal.SIGINT, lambda signum, frame: stopped.set())
    signal.signal(signal.SIGTERM, lambda signum, frame: stopped.set())
    started = time.monotonic()
    with args.output.open("x") as output:
        while not stopped.is_set() and time.monotonic() - started < args.seconds:
            timestamp = datetime.now(timezone.utc).isoformat()
            listing = subprocess.run(
                ["docker", "ps", "--filter", f"label=com.docker.compose.project={args.project}",
                 "--format", "{{.ID}} {{.Label \"com.docker.compose.service\"}}"],
                capture_output=True, text=True, timeout=10,
            )
            ids = [line.split()[0] for line in listing.stdout.splitlines()
                   if len(line.split()) == 2 and line.split()[1] in ("db", "chaos")]
            result = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                                    capture_output=True, text=True, timeout=15) if ids else None
            records = [json.loads(line) for line in result.stdout.splitlines()] if result else []
            error = (listing.stderr or "docker ps failed") if listing.returncode else (
                (result.stderr or "docker stats failed") if result and result.returncode else (
                    "No matching db/chaos containers found" if not ids else ""))
            output.write(json.dumps({"timestamp": timestamp, "elapsed": time.monotonic() - started,
                                     "project": args.project, "containers": records, "error": error}) + "\n")
            output.flush()
            stopped.wait(5)
    print(f"Resource samples: {args.output}", flush=True)


if __name__ == "__main__":
    main()
