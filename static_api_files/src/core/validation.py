from typing import Set, Optional, Dict, Any, TYPE_CHECKING
from sqlalchemy.orm import Session
from sqlalchemy import select, distinct
from datetime import datetime, timedelta
from functools import lru_cache

# Type checking imports (doesn't run at runtime)
if TYPE_CHECKING:
    from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
    from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
    from fao.src.db.pipelines.elements.elements_model import Elements


class ValidationCache:
    """Simple in-memory cache for validation data"""

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl = timedelta(seconds=ttl_seconds)
        self._cache: Dict[str, tuple[Any, datetime]] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get value if not expired"""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if datetime.utcnow() - timestamp < self.ttl:
                return value
            else:
                del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """Set value with timestamp"""
        self._cache[key] = (value, datetime.utcnow())


# Global cache instance
_cache = ValidationCache(ttl_seconds=3600)


def get_valid_area_codes(db: Session) -> Set[str]:
    """Get valid area codes with caching"""
    from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes

    cached = _cache.get("area_codes")
    if cached is not None:
        return cached

    area_codes = db.query(AreaCodes.area_code).distinct().all()
    valid_codes = {code[0] for code in area_codes}

    _cache.set("area_codes", valid_codes)
    return valid_codes


def get_valid_item_codes(db: Session) -> Set[str]:
    """Get valid item codes with caching"""
    # Local import for this specific model
    from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes

    cached = _cache.get("item_codes")
    if cached is not None:
        return cached

    item_codes = db.query(ItemCodes.item_code).distinct().all()
    valid_codes = {code[0] for code in item_codes}

    _cache.set("item_codes", valid_codes)
    return valid_codes


def get_valid_element_codes(db: Session) -> Set[str]:
    """Get valid element codes with caching"""
    from fao.src.db.pipelines.elements.elements_model import Elements

    cached = _cache.get("element_codes")
    if cached is not None:
        return cached

    element_codes = db.query(Elements.element_code).distinct().all()
    valid_codes = {code[0] for code in element_codes}

    _cache.set("element_codes", valid_codes)
    return valid_codes


def is_valid_area_code(code: str, db: Session) -> bool:
    """Check if area code is valid"""
    valid_codes = get_valid_area_codes(db)
    return code in valid_codes


def is_valid_item_code(code: str, db: Session) -> bool:
    """Check if item code is valid"""
    valid_codes = get_valid_item_codes(db)
    return code in valid_codes


def is_valid_element_code(code: str, db: Session) -> bool:
    """Check if element code is valid"""
    valid_codes = get_valid_element_codes(db)
    return code in valid_codes


def is_valid_year_range(year_start: int, year_end: int):
    """Shared validation function"""
    return year_start < year_end
