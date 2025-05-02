from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
from app.data_structures import DocumentDTO, dto_to_document
from app.runs import ACTIVE_RUNS, start_new_run, get_run_start_time
from app.doc_level_sql_record_manager import DocLevelSQLRecordManager
from app.doc_level_sync_manager import DocLevelSyncManager


DOC_RECORD_MANAGER = DocLevelSQLRecordManager(
    namespace="doc_sync_ns",
    db_url="sqlite:///doclevel.sqlite",
)
DOC_RECORD_MANAGER.create_schema()
DOC_SYNC_MANAGER = DocLevelSyncManager(DOC_RECORD_MANAGER)

doc_router = APIRouter(prefix="/docs", tags=["Document Sync"])


@doc_router.post("/start")
def start_doc_sync() -> Dict[str, str]:
    """Begin a new document-level sync run and return its run-id."""
    run_id = start_new_run()
    return {"run_id": run_id}


@doc_router.post("/upload")
def upload_doc_batch(
    run_id: str = Query(...),
    docs: List[DocumentDTO] = [],
) -> Dict[str, Any]:
    """
    Upsert **one batch** of documents.

    Returns usual per-batch counters *plus* the list of doc-ids
    that were actually created / updated. The client can use that list
    to decide which files must later be chunked.
    """
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(400, "Invalid run_id")

    doc_objs = [dto_to_document(d) for d in docs]
    result = DOC_SYNC_MANAGER.upsert_docs(doc_objs)

    stats = result["stats"]
    return {
        "batch_added": stats["added"],
        "batch_skipped": stats["skipped"],
        "batch_updated": stats["updated"],
        "changed_ids": result["changed_ids"],
    }


@doc_router.post("/finish")
def finish_doc_sync(run_id: str = Query(...)) -> Dict[str, Any]:
    """
    Conclude the document-level run, remove records that didn’t show up
    this time (i.e. deleted upstream) and tell the client *which* docs
    were removed so their chunks can be removed, too.
    """
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(400, "Invalid run_id")

    run_start = get_run_start_time(run_id)
    deleted_ids = DOC_SYNC_MANAGER.leftover_cleanup(run_start)
    del ACTIVE_RUNS[run_id]

    return {
        "deleted_ids": deleted_ids,
        "num_deleted": len(deleted_ids),
        "message": "Doc-level sync finished!",
    }
