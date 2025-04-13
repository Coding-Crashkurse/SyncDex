# runs.py
import time
import uuid

ACTIVE_RUNS = {}
CHUNK_ACTIVE_RUNS = {}


def start_new_run() -> str:
    run_id = str(uuid.uuid4())
    ACTIVE_RUNS[run_id] = time.time()
    return run_id


def get_run_start_time(run_id: str) -> float:
    return ACTIVE_RUNS[run_id]


def start_new_chunk_run() -> str:
    run_id = str(uuid.uuid4())
    CHUNK_ACTIVE_RUNS[run_id] = time.time()
    return run_id


def get_chunk_run_start_time(run_id: str) -> float:
    return CHUNK_ACTIVE_RUNS[run_id]
