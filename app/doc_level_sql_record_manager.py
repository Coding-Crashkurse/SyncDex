import hashlib
import json
from typing import Optional
from langchain.indexes import SQLRecordManager
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Float, Index, String, UniqueConstraint

Base = declarative_base()


class UpsertionRecord(Base):
    __tablename__ = "upsertion_record"
    uuid = Column(String, primary_key=True, nullable=False)
    key = Column(String, index=True)
    namespace = Column(String, index=True, nullable=False)
    group_id = Column(String, index=True, nullable=True)
    updated_at = Column(Float, index=True)
    __table_args__ = (
        UniqueConstraint("key", "namespace", name="uix_key_namespace"),
        Index("ix_key_namespace", "key", "namespace"),
    )


class DocLevelSQLRecordManager(SQLRecordManager):
    @staticmethod
    def compute_doc_hash(page_content: str, metadata: dict) -> str:
        sorted_meta = json.dumps(metadata, sort_keys=True)
        raw = page_content + "\n" + sorted_meta
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get_doc_hash(self, doc_id: str) -> Optional[str]:
        with self._make_session() as session:
            record = (
                session.query(UpsertionRecord)
                .filter_by(key=doc_id, namespace=self.namespace)
                .first()
            )
            return record.group_id if record else None

    def set_doc_hash(self, doc_id: str, new_hash: str) -> None:
        self.update([doc_id], group_ids=[new_hash])

    def leftover_cleanup(self, run_start_time: float) -> int:
        leftover_keys = self.list_keys(before=run_start_time)
        if leftover_keys:
            self.delete_keys(leftover_keys)
            return len(leftover_keys)
        return 0

    def upsert_doc(self, doc_id: str, page_content: str, metadata: dict) -> str:
        new_hash = self.compute_doc_hash(page_content, metadata)
        old_hash = self.get_doc_hash(doc_id)
        if old_hash is None:
            self.set_doc_hash(doc_id, new_hash)
            return "added"
        elif old_hash == new_hash:
            self.set_doc_hash(doc_id, old_hash)
            return "skipped"
        elif old_hash != new_hash:
            self.set_doc_hash(doc_id, new_hash)
            return "updated"
