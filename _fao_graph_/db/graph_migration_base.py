from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, List, Sequence, Any, Optional, Type, TypeVar
from sqlalchemy import text
from sqlalchemy.engine import Row
from _fao_graph_.core.exceptions import MigrationError
from _fao_graph_.db.database import get_session, get_db
from _fao_graph_.utils import load_sql
from logger import logger


class GraphMigrationBase(ABC):
    """Base class for all graph migrations"""

    def __init__(self, table_name: str, migration_type: str):
        self.table_name = table_name
        self.migration_type = migration_type
        self.batch_size = 5000
        self.created = 0
        self.updated = 0

    def get_count_query(self) -> str:
        """Return SQL query to count total records to migrate.
        Default implementation - override if needed."""
        return f"""
            SELECT COUNT(*) as total
            FROM {self.table_name}
            WHERE value > 0
        """

    @abstractmethod
    def get_index_queries(self) -> str:
        """Return SQL query to create indexes."""
        pass

    @abstractmethod
    def get_migration_query(self) -> str:
        """Return SQL query to fetch records for migration with LIMIT/OFFSET."""
        pass

    @abstractmethod
    def get_verification_query(self) -> str:
        """Return AGE queries for verification. Should include 'total_count' key."""
        pass

    def create(self, records: Sequence[Row]) -> None:
        """Create nodes or relationships"""
        pass

    def update(self, records: Sequence[Row]) -> None:
        """Update nodes or relationships"""
        pass

    def migrate(self, start_offset: int = 0, mode: str = "create") -> None:
        """Main migration entry point with resume capability.

        Can be overridden for simpler migrations that don't need batch processing.
        """
        logger.info(f"Starting {self.table_name} migration in {mode} mode from offset {start_offset:,}...")
        logger.info(f"Using batch size: {self.batch_size}")

        try:
            # Count total records
            count_query = text(self.get_count_query())

            with get_session() as session:
                total_records = session.execute(count_query).scalar() or 0
                logger.info(f"Total records to process: {total_records:,}")

                if start_offset > 0:
                    logger.info(
                        f"Resuming from offset {start_offset:,} ({start_offset/total_records*100:.1f}% already done)"
                    )
        except Exception as e:
            raise MigrationError(f"Failed to count total records: {e}") from e

        # Get migration query
        query = text(self.get_migration_query())
        offset = start_offset

        try:
            while offset < total_records:
                with get_session() as pg_session:
                    # Fetch batch
                    result = pg_session.execute(query, {"limit": self.batch_size, "offset": offset})
                    records = result.fetchall()

                    if not records:
                        break

                    # Process batch based on mode
                    try:
                        if mode == "create":
                            self.create(records)
                        elif mode == "update":
                            self.update(records)
                        else:
                            raise ValueError(f"Unknown mode: {mode}")
                    except Exception as e:
                        logger.error(f"Failed to process batch at offset {offset}: {e}")
                        logger.info(f"Resume with: --offset {offset}")
                        raise MigrationError(f"Batch processing failed at offset {offset}") from e

                    offset += len(records)

                    # Progress logging
                    pct_complete = offset / total_records * 100
                    self.log_progress(offset, total_records, pct_complete)

        except KeyboardInterrupt:
            logger.warning(f"\nMigration interrupted at offset {offset}")
            logger.info(f"Resume with: --offset {offset}")
            raise
        except MigrationError:
            # Already logged, just re-raise
            raise
        except Exception as e:
            logger.error(f"Unexpected error at offset {offset}: {e}")
            logger.info(f"Resume with: --offset {offset}")
            raise MigrationError(f"Migration failed at offset {offset}") from e

    def create_indexes(self):
        """Create indexes"""
        with get_session() as session:
            index_queries = self.get_index_queries()
            session.execute(text(index_queries))
            logger.success(f"Created {self.table_name} indexes")

    def log_progress(self, offset: int, total_records: int, pct_complete: float) -> None:
        """Log migration progress. Override for custom logging."""
        if self.updated > 0:
            logger.info(
                f"Progress: {offset:,}/{total_records:,} records ({pct_complete:.1f}%) | "
                f"Created: {self.created:,} | Updated: {self.updated:,}"
            )
        else:
            logger.info(
                f"Progress: {offset:,}/{total_records:,} records ({pct_complete:.1f}%) | " f"Created: {self.created:,}"
            )

    def verify_migration(self) -> None:
        """Verify the migration completed successfully."""

        try:
            with get_session() as session:
                result = session.execute(text(self.get_verification_query())).fetchall()
                logger.info(f"Verification complete: {result}")

        except Exception as e:
            logger.error(f"Verification Failed: {e}")
            raise MigrationError("Migration Verification Failed") from e

    def run_verification_query(self, session, name: str, query: str) -> None:
        """Run a single verification query. Override for custom handling."""
        logger.info(f"Running verification: {name}")
        result = session.execute(text(query))
        for record in result:
            logger.info(f"  {dict(record)}")

    def transform_records_for_age(self, records: Sequence[Row], field_mapping: Dict[str, str]) -> List[Dict[str, Any]]:
        """Helper to transform PostgreSQL records to AGE format."""
        relationships = []
        for record in records:
            rel_data = {}
            for pg_field, age_field in field_mapping.items():
                value = getattr(record, pg_field)
                if pg_field == "value" and value is not None:
                    value = float(value)
                rel_data[age_field] = value
            relationships.append(rel_data)
        return relationships
