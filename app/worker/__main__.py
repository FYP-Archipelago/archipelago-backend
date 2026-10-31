import logging
import os
import socket
import sys
import threading
import time

import redis

from app.redis_names import GROUP, JOB_CONFIG, JOB_RUN_ID, JOB_STREAM, PENDING_RUNS
from app.worker.jobs import run_job, write_error

CLAIM_IDLE_MS = 60000
HEARTBEAT_SECONDS = 15
MAX_DELIVERIES = 3
ERROR_BACKOFF_SECONDS = 5

log = logging.getLogger("app.worker")


def heartbeat(r: redis.Redis, name: str, job_id: bytes, stop: threading.Event) -> None:
    while not stop.wait(HEARTBEAT_SECONDS):
        try:
            # JUSTID: a plain XCLAIM would add 1 to the delivery count on every beat
            r.xclaim(PENDING_RUNS, GROUP, name, 0, [job_id], justid=True)
        except redis.RedisError:
            log.exception("heartbeat failed")


# Takes one abandoned or new job, if there is one, and runs it to the end.
def process_next(r: redis.Redis, name: str) -> None:
    claimed = r.xautoclaim(PENDING_RUNS, GROUP, name, CLAIM_IDLE_MS, "0-0", count=1)[1]
    if not claimed:
        read = r.xreadgroup(GROUP, name, {PENDING_RUNS: ">"}, count=1, block=5000)  # ms
        claimed = read[0][1] if read else []
    if not claimed:
        return
    job_id, fields = claimed[0]
    run_id = fields[JOB_RUN_ID].decode()

    stop = threading.Event()
    try:
        # XAUTOCLAIM counts as a delivery, so a job re-claimed once is at 2
        deliveries = r.xpending_range(PENDING_RUNS, GROUP, job_id, job_id, 1)[0]["times_delivered"]
        if deliveries > MAX_DELIVERIES:
            write_error(r, run_id, "internal", f"failed after {MAX_DELIVERIES} attempts")
        else:
            log.info("run %s: starting (delivery %d)", run_id, deliveries)
            threading.Thread(target=heartbeat, args=(r, name, job_id, stop), daemon=True).start()
            run_job(r, run_id, fields[JOB_STREAM] == b"true", fields[JOB_CONFIG])
        # ack last: until then a crash leaves the job pending for another worker to claim
        r.xack(PENDING_RUNS, GROUP, job_id)
        r.xdel(PENDING_RUNS, job_id)
        log.info("run %s: done", run_id)
    finally:
        stop.set()


def main() -> None:
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    url = os.environ.get("ARCHIPELAGO_REDIS_URL")
    if url is None:
        sys.exit("missing required environment variable ARCHIPELAGO_REDIS_URL")
    name = os.environ.get("ARCHIPELAGO_WORKER_NAME", f"{socket.gethostname()}-{os.getpid()}")
    # redis-py 8 defaults to a 5 s socket timeout, which would cut the 5 s blocking read
    r = redis.Redis.from_url(url, decode_responses=False, socket_timeout=30)
    try:
        r.xgroup_create(PENDING_RUNS, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    log.info("worker %s started", name)

    while True:
        try:
            process_next(r, name)
        except Exception:
            log.exception("worker loop failed; a held job stays unacked for re-claim")
            time.sleep(ERROR_BACKOFF_SECONDS)


if __name__ == "__main__":
    main()
