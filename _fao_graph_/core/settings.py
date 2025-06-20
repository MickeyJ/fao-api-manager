import os
from typing import List

from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import computed_field

load_dotenv(override=True)


class Settings(BaseSettings):
    # API Configuration
    api_version: str = "1.0.0"
    api_title: str = "FAO Graph API"
    api_description: str = "Graph API for FAO data"
    api_port: int = 8001
    api_host: str = "localhost"

    @computed_field
    @property
    def api_version_prefix(self) -> str:
        """Extract major version for URL prefix (v1, v2, etc)"""
        major_version = self.api_version.split(".")[0]
        return f"v{major_version}"

    @computed_field
    @property
    def api_version_date(self) -> str:
        """Version date for date-based versioning (optional)"""
        # You could use this for date-based versioning like GitHub
        return "2024-01-15"  # Or pull from git tag date

    # Database Configuration
    graph_db_user: str = os.getenv("GRAPH_DB_USER", "postgres")
    graph_db_password: str = os.getenv("GRAPH_DB_PASSWORD", "password")
    graph_db_host: str = os.getenv("GRAPH_DB_HOST", "localhost")
    graph_db_port: str = os.getenv("GRAPH_DB_PORT", "5433")
    graph_db_name: str = os.getenv("GRAPH_DB_NAME", "fao_graph")

    # Cache Configuration
    cache_enabled: bool = os.getenv("CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
    redis_host: str = os.getenv("REDIS_HOST") or "localhost"
    redis_port: int = int(os.getenv("REDIS_PORT") or 6379)
    redis_password: str = os.getenv("REDIS_PASSWORD") or "password"
    default_cache_ttl: int = 3600
    cache_prefix: str = "fao_graph"
    cache_key_separator: str = ":"
    max_scan_count: int = 100

    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "https://app.mickeymalotte.com"]

    # Query defaults
    default_limit: int = 100
    max_limit: int = 1000
    default_offset: int = 0

    # Documentation URLs
    docs_url: str | None = None
    redoc_url: str | None = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Allow environment variables like API_PORT to override api_port
        env_prefix = ""  # No prefix by default
        extra = "ignore"


settings = Settings()
