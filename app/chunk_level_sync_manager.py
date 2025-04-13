from typing import List, Dict
from langchain_core.documents import Document
from langchain_core.indexing import RecordManager
from langchain_core.vectorstores import VectorStore
from langchain.text_splitter import TextSplitter
from langchain.indexes import index


class ChunkLevelSyncManager:
    def __init__(
        self,
        record_manager: RecordManager,
        vectorstore: VectorStore,
        text_splitter: TextSplitter,
    ):
        self.record_manager = record_manager
        self.vectorstore = vectorstore
        self.text_splitter = text_splitter

    def sync_one_batch(self, docs: List[Document]) -> Dict[str, int]:
        chunked = self.text_splitter.split_documents(docs)
        print(f"Chunk-level: {len(docs)} input docs => produced {len(chunked)} chunks.")
        result = index(
            docs_source=chunked,
            record_manager=self.record_manager,
            vector_store=self.vectorstore,
            cleanup=None,
            source_id_key="source",
            batch_size=10,
            force_update=False,
        )
        print(f"Chunk-level partial stats => {result}")
        return result

    def leftover_cleanup(self, run_start_time: float, batch_size: int = 100) -> int:
        total_deleted = 0
        while True:
            old_keys = self.record_manager.list_keys(
                before=run_start_time, limit=batch_size
            )
            if not old_keys:
                break
            self.vectorstore.delete(ids=old_keys)
            self.record_manager.delete_keys(old_keys)
            total_deleted += len(old_keys)
            print(f"Deleted {len(old_keys)} leftover docs in a chunk (chunk-level).")
        return total_deleted
