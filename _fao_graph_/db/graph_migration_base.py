from abc import ABC, abstractmethod
from time import time
from typing import Sequence
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row

from _fao_graph_.logger import logger
from _fao_graph_.core.exceptions import MigrationError
from _fao_graph_.db.db_connections import db_connections  # Import the new connection manager


class GraphMigrationBase(ABC):
    """Base class for all graph migrations"""

    def __init__(self, table_name: str, migration_type: str, project_name: str, node_label: str):
        self.project_name = project_name
        self.table_name = table_name
        self.migration_type = migration_type
        self.node_label = node_label
        self.batch_size = 5000
        self.created = 0
        self.updated = 0
        self.current_batch = 0  # Track batch number for progress
        self.last_select_duration = 0  # Track SELECT performance

    def get_count_query(self) -> str:
        """Return SQL query to count total records to migrate.
        Default implementation - override if needed."""
        return f"""
            SELECT COUNT(*) as total
            FROM {self.table_name}
            WHERE value > 0
        """

    @abstractmethod
    def get_migration_query(self) -> str:
        """Return SQL query to fetch records for migration with LIMIT/OFFSET."""
        pass

    @abstractmethod
    def get_verification_query(self) -> str:
        """Return AGE queries for verification. Should include 'total_count' key."""
        pass

    def get_index_queries(self) -> str:
        """Return SQL query to create indexes."""
        return ""

    def create(self, records: Sequence[Row], session: Session) -> None:
        """Create nodes or relationships"""
        pass

    def update(self, records: Sequence[Row], session: Session) -> None:
        """Update nodes or relationships"""
        pass

    def migrate(self, start_offset: int = 0, mode: str = "create") -> None:
        """Main migration entry point with resume capability."""

        try:
            if self.migration_type == "node":
                # Nodes - fetch from regular PG, create in graph
                with db_connections.pg_session() as pg_session:
                    records = pg_session.execute(text(f"SELECT * FROM {self.table_name}")).fetchall()

                with db_connections.graph_session() as graph_session:
                    self.create(records, graph_session)
                    self.run_verification_query(graph_session, self.table_name)

            else:
                logger.info(
                    f"Starting {self.table_name} relationship migration in {mode} mode from offset {start_offset:,}..."
                )
                logger.info(f"Using batch size: {self.batch_size}")

                # Count total records using regular PG connection
                count_query = text(self.get_count_query())

                with db_connections.pg_session() as pg_session:
                    logger.info("Starting count query...")
                    start_time = time()
                    total_records = pg_session.execute(count_query).scalar() or 0

                    elapsed = time() - start_time
                    logger.info(f"Count query took {elapsed:.2f} seconds")
                    logger.info(f"Total records to process: {total_records:,}")

                    if start_offset > 0:
                        logger.info(
                            f"Resuming from offset {start_offset:,} ({start_offset/total_records*100:.1f}% already done)"
                        )

                # Get migration query
                query = text(self.get_migration_query())
                offset = start_offset
                self.current_batch = offset // self.batch_size

                while offset < total_records:
                    # Fetch batch from regular PG
                    with db_connections.pg_session() as pg_session:
                        select_start = time()
                        result = pg_session.execute(query, {"limit": self.batch_size, "offset": offset})
                        records = result.fetchall()
                        self.last_select_duration = (time() - select_start) * 1000  # ms

                        if not records:
                            break

                    # Process batch in graph DB
                    with db_connections.graph_session() as graph_session:
                        batch_start = time()
                        try:
                            if mode == "create":
                                self.create(records, graph_session)
                            elif mode == "update":
                                self.update(records, graph_session)
                            else:
                                raise ValueError(f"Unknown mode: {mode}")

                            # Record progress after successful batch
                            insert_duration = (time() - batch_start) * 1000
                            db_connections._record_progress(
                                graph_session,
                                table_name=self.table_name,
                                relationship_type=getattr(self, "relationship_type", None),
                                batch_number=self.current_batch,
                                batch_size=len(records),
                                select_duration_ms=int(self.last_select_duration),
                                insert_duration_ms=int(insert_duration),
                                records_processed=len(records),
                                cumulative_records=self.created,
                                error_message=None,
                            )

                        except Exception as e:
                            # Record failure
                            db_connections._record_progress(
                                graph_session,
                                table_name=self.table_name,
                                relationship_type=getattr(self, "relationship_type", None),
                                batch_number=self.current_batch,
                                batch_size=len(records),
                                select_duration_ms=int(self.last_select_duration),
                                insert_duration_ms=None,
                                records_processed=0,
                                cumulative_records=self.created,
                                error_message=str(e),
                            )
                            logger.error(f"Failed to process batch at offset {offset}: {e}")
                            logger.info(f"Resume with: --offset {offset}")
                            raise MigrationError(f"Batch processing failed at offset {offset}") from e

                        offset += len(records)
                        self.current_batch += 1

                        # Progress logging
                        pct_complete = offset / total_records * 100
                        self.log_progress(offset, total_records, pct_complete)

        except KeyboardInterrupt:
            logger.error(f"\nMigration interrupted")
            raise
        except MigrationError as e:
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise MigrationError(f"Migration failed {e}")

    def create_indexes(self):
        """Create indexes in graph database"""
        with db_connections.graph_session() as graph_session:
            index_queries = self.get_index_queries()
            graph_session.execute(text(index_queries))
            logger.success(f"Created {self.table_name} indexes")

    def verify(self) -> None:
        """Verify the migration completed successfully."""
        try:
            with db_connections.graph_session() as graph_session:
                result = graph_session.execute(text(self.get_verification_query())).mappings().all()
                logger.info(f"Verification complete: {result}")

        except Exception as e:
            logger.error(f"Verification Failed: {e}")
            raise MigrationError("Migration Verification Failed") from e

    def run_verification_query(self, session, name) -> None:
        """Run a single verification query."""
        logger.info(f"Running verification: {name}")
        result = session.execute(text(self.get_verification_query())).mappings().all()
        for record in result:
            logger.info(f"  {record}")

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
