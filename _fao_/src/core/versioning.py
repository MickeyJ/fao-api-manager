# static_api_files/src/core/versioning.py
from enum import Enum
from datetime import datetime
from typing import Optional


class APIVersionStatus(Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"  # No longer available


class APIVersion:
    def __init__(
        self,
        version: str,
        status: APIVersionStatus,
        deprecated_date: Optional[datetime] = None,
        sunset_date: Optional[datetime] = None,
    ):
        self.version = version
        self.status = status
        self.deprecated_date = deprecated_date
        self.sunset_date = sunset_date

    @property
    def is_deprecated(self) -> bool:
        return self.status == APIVersionStatus.DEPRECATED

    @property
    def days_until_sunset(self) -> Optional[int]:
        if self.sunset_date:
            return (self.sunset_date - datetime.now()).days
        return None


# Version registry
VERSIONS = {
    "v1": APIVersion("1.0.0", APIVersionStatus.ACTIVE),
    # "v0": APIVersion(
    #     "0.9.0",
    #     APIVersionStatus.DEPRECATED,
    #     deprecated_date=datetime(2024, 1, 1),
    #     sunset_date=datetime(2024, 6, 1)
    # )
}
