PENDING_RUNS = "pending_runs"
GROUP = "workers"

# pending_runs entry fields
JOB_RUN_ID = b"run_id"
JOB_STREAM = b"stream"
JOB_CONFIG = b"config"

# runner_logs entry fields; KIND is one of ROW, EVENT (runner) or ABORT (backend only)
KIND = b"kind"
DATA = b"data"
ROW = b"row"
EVENT = b"event"
ABORT = b"abort"

# clustered entry fields: COVERS + UPDATE, FINAL, or PHASE + ERROR
COVERS = b"covers"
UPDATE = b"update"
FINAL = b"final"
PHASE = b"phase"
ERROR = b"error"


def runner_logs_key(run_id: str) -> str:
    return f"run:{run_id}:runner_logs"


def clustered_key(run_id: str) -> str:
    return f"run:{run_id}:clustered"
