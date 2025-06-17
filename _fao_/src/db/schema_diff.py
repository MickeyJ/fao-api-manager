import subprocess
import os

from sqlalchemy import create_engine

from _fao_.logger import logger
from _fao_.src.db.database import Base, DATABASE_URL
from _fao_.src.db.system_models import *
from _fao_.all_model_imports import *


def schema_diff():
    # Force reload of modules to ensure latest changes are loaded
    logger.info("Running migra Schema Diff Checker...")

    # Show what tables are registered
    logger.info(f"\nRegistered tables: {len(Base.metadata.tables)}")
    logger.info(f"Table names: {list(Base.metadata.tables.keys())[:10]}...")

    temp_db = f"temp_schema_{os.getpid()}"
    base_url = DATABASE_URL.rsplit("/", 1)[0]
    temp_url = f"{base_url}/{temp_db}"
    temp_engine = None

    try:
        logger.info(f"Creating temporary database {temp_db}...")
        subprocess.run(["createdb", temp_db], check=True)

        logger.info("Creating fresh schema from models...")
        temp_engine = create_engine(temp_url)
        Base.metadata.create_all(temp_engine)

        logger.info("Comparing schemas...")
        result = subprocess.run(
            ["migra", "--unsafe", DATABASE_URL, temp_url],
            capture_output=True,
            text=True,
        )

        if result.stderr:
            logger.error(f"Migra stderr: {result.stderr}")

        if result.stdout:
            lines = result.stdout.split("\n")

            # Filter out materialized view related drops
            mv_prefixes = ["item_stats_", "price_details_", "price_ratios_"]
            filtered_lines = []

            for line in lines:
                line_lower = line.lower()
                # Skip drops for materialized views and their indexes
                if "drop materialized view" in line_lower:
                    continue
                if "drop index" in line_lower and any(prefix in line for prefix in mv_prefixes):
                    continue
                filtered_lines.append(line)

            # Remove empty lines from filtering
            filtered_lines = [line for line in filtered_lines if line.strip()]

            if filtered_lines:
                logger.warning("Schema differences found (excluding materialized views):")
                print("\n".join(filtered_lines))
            else:
                logger.info(
                    "Found differences only in materialized views/indexes - these are not managed by SQLAlchemy"
                )
        else:
            logger.success("No schema differences found!")

    finally:
        # CRITICAL: Dispose of engine to close all connections
        if temp_engine:
            temp_engine.dispose()

        logger.info(f"\nCleaning up temporary database...")
        # Try normal drop first
        drop_result = subprocess.run(["dropdb", temp_db], capture_output=True, text=True)

        # If normal drop fails due to connections, force disconnect
        if drop_result.returncode != 0 and "being accessed by other users" in drop_result.stderr:
            logger.info("Force disconnecting active connections...")
            subprocess.run(
                [
                    "psql",
                    "-d",
                    "postgres",
                    "-c",
                    f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{temp_db}';",
                ]
            )
            # Try drop again
            subprocess.run(["dropdb", temp_db])


if __name__ == "__main__":
    schema_diff()
