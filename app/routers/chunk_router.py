from fastapi import APIRouter, HTTPException, Query
from typing import List
from app.data_structures import DocumentDTO, dto_to_document
from app.runs import CHUNK_ACTIVE_RUNS, start_new_chunk_run, get_chunk_run_start_time
from app.chunk_level_sync_manager import ChunkLevelSyncManager
from langchain.text_splitter import RecursiveCharacterTextSplitter

import os
from langchain.indexes import SQLRecordManager
from langchain_community.embeddings import FakeEmbeddings
from langchain_chroma import Chroma

EMBEDDINGS = FakeEmbeddings(size=1536)

VECTORSTORE = Chroma(
    collection_name="my_demo_collection",
    embedding_function=EMBEDDINGS,
    persist_directory=os.path.join(os.getcwd(), "chroma_data"),
)

NAMESPACE = "chroma/my_demo_collection"
RECORD_MANAGER = SQLRecordManager(
    namespace=NAMESPACE,
    db_url="sqlite:///demo_record_manager.sqlite",
)


text_splitter = RecursiveCharacterTextSplitter(chunk_size=100, chunk_overlap=50)
RECORD_MANAGER.create_schema()
CHUNK_SYNC_MANAGER = ChunkLevelSyncManager(
    record_manager=RECORD_MANAGER,
    vectorstore=VECTORSTORE,
    text_splitter=text_splitter,
)

chunk_router = APIRouter(prefix="/chunks", tags=["Chunk Sync"])


@chunk_router.post("/start")
def start_chunk_sync():
    chunk_run_id = start_new_chunk_run()
    return {"chunk_run_id": chunk_run_id}


@chunk_router.post("/upload")
def upload_chunk_batch(chunk_run_id: str = Query(...), docs: List[DocumentDTO] = []):
    if chunk_run_id not in CHUNK_ACTIVE_RUNS:
        raise HTTPException(400, "Invalid chunk_run_id")
    doc_objs = [dto_to_document(d) for d in docs]
    partial_result = CHUNK_SYNC_MANAGER.sync_one_batch(doc_objs)
    return partial_result


@chunk_router.post("/finish")
def finish_chunk_sync(chunk_run_id: str = Query(...)):
    if chunk_run_id not in CHUNK_ACTIVE_RUNS:
        raise HTTPException(400, "Invalid chunk_run_id")
    run_start = get_chunk_run_start_time(chunk_run_id)
    num_deleted = CHUNK_SYNC_MANAGER.leftover_cleanup(run_start)
    del CHUNK_ACTIVE_RUNS[chunk_run_id]
    return {"num_deleted": num_deleted, "message": "Chunk-level sync finished!"}
