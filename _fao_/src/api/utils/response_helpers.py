# fao/src/api/utils/response_helpers.py (complete)
from typing import Dict, List, Any, Set, Optional
from datetime import datetime, timezone

import math
from urllib.parse import urlencode, urlparse, parse_qs

from fastapi import Response


class PaginationBuilder:
    """Build pagination metadata and links."""

    @staticmethod
    def build_pagination_meta(total_count: int, limit: int, offset: int) -> Dict:
        """Build pagination metadata."""
        total_pages = math.ceil(total_count / limit) if limit > 0 else 1
        current_page = (offset // limit) + 1 if limit > 0 else 1

        return {
            "total": total_count,
            "total_pages": total_pages,
            "current_page": current_page,
            "per_page": limit,
            "from": offset + 1 if total_count > 0 else 0,
            "to": min(offset + limit, total_count),
            "has_next": current_page < total_pages,
            "has_prev": current_page > 1,
        }

    @staticmethod
    def build_links(base_url: str, total_count: int, limit: int, offset: int, params: Dict) -> Dict:
        """Build pagination links."""
        links = {}
        total_pages = math.ceil(total_count / limit) if limit > 0 else 1
        current_page = (offset // limit) + 1 if limit > 0 else 1

        # Parse base URL
        parsed = urlparse(str(base_url))
        base_params = parse_qs(parsed.query)

        # Merge with provided params
        query_params = {k: v for k, v in params.items() if k not in ["offset", "limit"] and v is not None}

        def build_url(new_offset: int) -> str:
            all_params = {**query_params, "limit": limit, "offset": new_offset}
            query_string = urlencode(all_params, doseq=True)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query_string}"

        # Build links
        links["first"] = build_url(0)
        links["last"] = build_url((total_pages - 1) * limit)

        if current_page < total_pages:
            links["next"] = build_url(offset + limit)

        if current_page > 1:
            links["prev"] = build_url(max(0, offset - limit))

        return links


class ResponseFormatter:
    """Format API responses consistently."""

    @staticmethod
    def format_data_response(
        data: List[Dict], pagination: Dict, links: Dict, filters_applied: int = 0, aggregations: Optional[Dict] = None
    ) -> Dict:
        """Format standard data response with pagination."""
        response = {
            "data": data,
            "pagination": pagination,
            "links": links,
            "_meta": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "filters_applied": filters_applied,
            },
        }

        if aggregations:
            response["aggregations"] = aggregations

        return response

    @staticmethod
    def format_metadata_response(dataset: str, metadata_type: str, total: int, items: List[Dict]) -> Dict:
        """Format metadata response."""
        return {"dataset": dataset, f"total_{metadata_type}": total, metadata_type: items}

    @staticmethod
    def set_pagination_headers(response: Response, total_count: int, limit: int, offset: int, links: dict):
        """Set pagination-related response headers"""
        total_pages = (total_count + limit - 1) // limit if limit > 0 else 1
        current_page = (offset // limit) + 1 if limit > 0 else 1

        response.headers["X-Total-Count"] = str(total_count)
        response.headers["X-Total-Pages"] = str(total_pages)
        response.headers["X-Current-Page"] = str(current_page)
        response.headers["X-Per-Page"] = str(limit)

        # Build Link header
        link_parts = []
        for rel, url in links.items():
            link_parts.append(f'<{url}>; rel="{rel}"')
        if link_parts:
            response.headers["Link"] = ", ".join(link_parts)
