from fastapi import APIRouter, HTTPException, Query
from typing import List
from app.data_structures import DocumentDTO, dto_to_document
from app.runs import ACTIVE_RUNS, start_new_run, get_run_start_time
from app.doc_level_sql_record_manager import DocLevelSQLRecordManager
from app.doc_level_sync_manager import DocLevelSyncManager

DOC_RECORD_MANAGER = DocLevelSQLRecordManager(
    namespace="doc_sync_ns", db_url="sqlite:///doclevel.sqlite"
)
DOC_RECORD_MANAGER.create_schema()
DOC_SYNC_MANAGER = DocLevelSyncManager(DOC_RECORD_MANAGER)

doc_router = APIRouter(prefix="/docs", tags=["Document Sync"])


@doc_router.post("/start")
def start_doc_sync():
    run_id = start_new_run()
    return {"run_id": run_id}


@doc_router.post("/upload")
def upload_doc_batch(run_id: str = Query(...), docs: List[DocumentDTO] = []):
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(400, "Invalid run_id")
    doc_objs = [dto_to_document(d) for d in docs]
    partial_stats = DOC_SYNC_MANAGER.upsert_docs(doc_objs)
    return {
        "batch_added": partial_stats["added"],
        "batch_skipped": partial_stats["skipped"],
        "batch_updated": partial_stats["updated"],
    }


@doc_router.post("/finish")
def finish_doc_sync(run_id: str = Query(...)):
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(400, "Invalid run_id")
    run_start = get_run_start_time(run_id)
    num_deleted = DOC_SYNC_MANAGER.leftover_cleanup(run_start)
    del ACTIVE_RUNS[run_id]
    return {"num_deleted": num_deleted, "message": "Doc-level sync finished!"}
