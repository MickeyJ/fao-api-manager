# static_api_files/src/api_custom/routers/versions.py
from fastapi import APIRouter
from ....core.versioning import VERSIONS
from ....core import settings

router = APIRouter(prefix="/versions", tags=["versions"])


@router.get("/")
def get_api_versions():
    """Get all API versions and their status"""
    return {
        version: {
            "version": v.version,
            "status": v.status.value,
            "deprecated_date": v.deprecated_date.isoformat() if v.deprecated_date else None,
            "sunset_date": v.sunset_date.isoformat() if v.sunset_date else None,
            "days_until_sunset": v.days_until_sunset,
            "map": f"/{settings.api_version_prefix}",
        }
        for version, v in VERSIONS.items()
    }


@router.get("/current")
def get_current_version():
    """Get current API version details"""

    current = VERSIONS.get(settings.api_version_prefix)
    if current:
        return {
            "version": current.version,
            "status": current.status.value,
            "is_deprecated": current.is_deprecated,
            "days_until_sunset": current.days_until_sunset,
            "map": f"/{settings.api_version_prefix}",
        }
