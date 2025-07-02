# _fao_/src/api/models/base_responses.py
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class PaginationMeta(BaseModel):
    total: int
    total_pages: int
    current_page: int
    per_page: int
    from_: int = Field(alias="from")
    to: int
    has_next: bool
    has_prev: bool


class ResponseMeta(BaseModel):
    generated_at: str  # ISO datetime string
    filters_applied: int


class BaseDataResponse(BaseModel):
    """Base response model that matches ResponseFormatter.format_data_response()"""

    data: List[Any]  # Will be overridden in specific responses
    pagination: PaginationMeta
    links: Dict[str, str]
    _meta: ResponseMeta
    aggregations: Optional[Dict[str, Any]] = None
