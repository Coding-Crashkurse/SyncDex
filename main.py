from fastapi import FastAPI
import uvicorn
from app.routers.document_router import doc_router
from app.routers.chunk_router import chunk_router

tags_metadata = [
    {
        "name": "Document Sync",
        "description": "Endpoints to manage document-level synchronization, including starting a sync run, uploading document batches, and finalizing the sync.",
    },
    {
        "name": "Chunk Sync",
        "description": "Endpoints to manage chunk-level synchronization, including starting a sync run, uploading chunk batches, and finishing the sync.",
    },
]

app = FastAPI(
    title="Sync API Documentation",
    description="""This API provides endpoints for coordinating both document-level and chunk-level synchronization tasks.
    Use the Document Sync endpoints to upload and manage full documents, and the Chunk Sync endpoints to handle document chunks
    for further processing and indexing.""",
    version="0.0.1",
    openapi_tags=tags_metadata,
)

app.include_router(doc_router)
app.include_router(chunk_router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
