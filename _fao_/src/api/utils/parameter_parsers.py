# fao/src/api/utils/parameter_parsers.py
from typing import List, Tuple, Optional, Dict, Any, Type
from sqlalchemy import Column
from fastapi import HTTPException


def parse_sort_parameter(sort_params: List[str]) -> List[Tuple[str, str]]:
    """Parse sort parameter string into column name and direction pairs.

    Returns list of (column_name, direction) tuples.
    Validation should be done separately.
    """
    if not sort_params:
        return []

    sort_fields = []
    for field in sort_params:
        field = field.strip()
        if not field:
            continue

        if ":" in field:
            parts = field.split(":", 1)
            print(parts)
            column_name = parts[0].strip()
            direction = parts[1].strip().lower()
        else:
            # Default to ascending if no direction specified
            column_name = field
            direction = "asc"

        sort_fields.append((column_name, direction))

    return sort_fields


def parse_fields_parameter(fields: Optional[List[str]]) -> List[str]:
    """Parse comma-separated fields parameter.

    Returns list of field names or None if no fields specified.
    Validation should be done separately.
    """
    if not fields:
        return []

    return [f.strip() for f in fields if f.strip()]


def parse_aggregation_parameter(agg: str) -> Dict[str, str]:
    """Parse aggregation parameter into components.

    Format: 'field:function' or 'field:function:alias'
    Returns dict with 'field', 'function', and 'alias'.
    Validation should be done separately.
    """
    parts = agg.split(":")

    if len(parts) < 2:
        # Invalid format, but let validation handle it
        return {"field": agg, "function": "", "alias": ""}

    field = parts[0].strip()
    function = parts[1].strip()
    alias = parts[2].strip() if len(parts) >= 3 else f"{field}_{function}"

    return {"field": field, "function": function, "alias": alias}
