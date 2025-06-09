from pydantic_settings import BaseSettings
from pydantic import computed_field
from typing import List


class Settings(BaseSettings):
    # API Configuration
    api_version: str = "1.0.0"
    api_title: str = "Food Price Analysis API"
    api_description: str = "API for analyzing global food commodity prices"
    api_port: int = 8000
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

    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "https://app.mickeymalotte.com"]

    # Query defaults
    default_limit: int = 100
    max_limit: int = 1000
    default_offset: int = 0

    # Documentation URLs
    docs_url: str = "/docs"
    redoc_url: str = "/redoc"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        # Allow environment variables like API_PORT to override api_port
        env_prefix = ""  # No prefix by default
        extra = "ignore"


settings = Settings()
