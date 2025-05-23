from typing import List, Dict, Any
from langchain_core.documents import Document
from app.doc_level_sql_record_manager import DocLevelSQLRecordManager


class DocLevelSyncManager:
    def __init__(self, manager: DocLevelSQLRecordManager):
        self.manager = manager

    def upsert_docs(self, docs: List[Document]) -> Dict[str, Any]:
        stats = {"added": 0, "skipped": 0, "updated": 0}
        changed_ids: List[str] = []

        for doc in docs:
            doc_id = doc.id or doc.metadata.get("source")
            outcome = self.manager.upsert_doc(
                doc_id=doc_id,
                page_content=doc.page_content,
                metadata=doc.metadata,
            )
            stats[outcome] += 1
            if outcome in ("added", "updated"):
                changed_ids.append(doc_id)

        return {"stats": stats, "changed_ids": changed_ids}



    def leftover_cleanup(self, run_start_time: float) -> List[str]:
        return self.manager.leftover_cleanup(run_start_time)
