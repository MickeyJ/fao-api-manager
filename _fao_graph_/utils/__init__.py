from .logger import logger
from .load_sql import load_sql


def singularize(name: str) -> str:
    """Convert plural to singular for common cases"""
    if name.endswith("ies"):
        return name[:-3] + "y"  # currencies -> currency
    elif name.endswith("ses"):
        return name[:-2]  # purposes -> purpose
    elif name.endswith("s"):
        return name[:-1]  # area_codes -> area_code
    return name


__all__ = ["logger", "load_sql", "singularize"]
