from pydantic import BaseModel
from typing import Dict, Any
from langchain_core.documents import Document


class DocumentDTO(BaseModel):
    page_content: str
    metadata: Dict[str, Any] = {}


def dto_to_document(dto: DocumentDTO) -> Document:
    return Document(page_content=dto.page_content, metadata=dto.metadata)
