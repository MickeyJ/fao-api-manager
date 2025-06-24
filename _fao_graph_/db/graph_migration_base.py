from abc import ABC, abstractmethod
from time import time
from typing import Sequence, Any, Dict
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Row

from _fao_graph_.logger import logger
from _fao_graph_.core.exceptions import MigrationError
from _fao_graph_.db.db_connections import db_connections  # Import the new connection manager


class GraphMigrationBase(ABC):
    """Base class for all graph migrations"""

    def __init__(
        self,
        table_name: str,
        migration_type: str,
        project_name: str,
        node_label: str | None = None,
        relationship_type: str | None = None,
        batch_size: int = 5000,
    ):
        self.project_name = project_name
        self.table_name = table_name
        self.migration_type = migration_type
        self.node_label = node_label
        self.relationship_type = relationship_type
        self.batch_size = batch_size
        self.created = 0
        self.updated = 0
        self.current_batch = 0  # Track batch number for progress
        self.last_select_duration = 0  # Track SELECT performance

    @abstractmethod
    def get_migration_query(self) -> str:
        """Return SQL query to fetch records for migration with LIMIT/OFFSET."""
        pass

    @abstractmethod
    def get_verification_query(self) -> str:
        """Return AGE queries for verification. Should include 'total_count' key."""
        pass

    def get_total_rows_query(self) -> str:
        """Return SQL query to count total records to migrate."""
        return ""

    def get_index_queries(self) -> str:
        """Return SQL query to create indexes."""
        return ""

    def create(self, records: Sequence[Row], session: Session) -> None:
        """Create nodes or relationships"""
        pass

    def update(self, records: Sequence[Row], session: Session) -> None:
        """Update nodes or relationships"""
        pass

    def get_progress_record(self, table_name: str) -> Dict[str, Any] | None:
        """Get existing progress record for this migration"""
        try:
            with db_connections.graph_session() as session:
                query = text(
                    """
                    SELECT * FROM migration_progress
                    WHERE table_name = :table_name
                    AND (relationship_type = :rel_type OR (relationship_type IS NULL AND :rel_type IS NULL))
                """
                )

                result = (
                    session.execute(query, {"table_name": table_name, "rel_type": self.relationship_type or None})
                    .mappings()
                    .first()
                )

                return dict(result) if result else None

        except Exception as e:
            logger.error(f"Failed to get progress record: {e}")
            raise MigrationError(f"Could not check migration progress: {e}")

    def create_progress_record(self, table_name: str, total_records: int) -> Dict[str, Any] | None:
        """Create new progress record"""
        try:
            with db_connections.graph_session() as session:
                query = text(
                    """
                    INSERT INTO migration_progress 
                    (migration_type, table_name, relationship_type, batch_size, total_records, status, started_at)
                    VALUES (:migration_type, :table_name, :rel_type, :batch_size, :total_records, 'in_progress', NOW())
                    RETURNING *
                """
                )

                result = (
                    session.execute(
                        query,
                        {
                            "migration_type": self.migration_type,
                            "table_name": table_name,
                            "rel_type": self.relationship_type or None,
                            "batch_size": self.batch_size,
                            "total_records": total_records,
                        },
                    )
                    .mappings()
                    .first()
                )

                session.commit()
                logger.info(f"Created progress record for {table_name} with total records: {total_records}")
                return dict(result)

        except Exception as e:
            logger.error(f"Failed to create progress record: {e}")
            raise MigrationError(f"Could not create migration progress record: {e}")

    def update_progress(
        self,
        table_name: str,
        batch_number: int,
        records_processed: int,
        last_offset: int,
        status: str = "in_progress",
        error_message: str | None = None,
        insert_duration_ms: int | None = None,
        indexes_created: bool = False,
        completed: bool = False,
    ) -> None:
        """Update progress record"""
        try:
            with db_connections.graph_session() as session:
                query = text(
                    """
                    UPDATE migration_progress 
                    SET batch_number = :batch_number,
                        records_processed = :records_processed,
                        last_offset = :last_offset,
                        status = :status,
                        error_message = :error_message,
                        select_duration_ms = :select_duration,
                        insert_duration_ms = :insert_duration,
                        completed_at = CASE WHEN :status = 'completed' THEN NOW() ELSE NULL END,
                        indexes_created = :indexes_created,
                        completed = :completed
                    WHERE table_name = :table_name
                    AND (relationship_type = :rel_type OR (relationship_type IS NULL AND :rel_type IS NULL))
                """
                )

                result = session.execute(
                    query,
                    {
                        "batch_number": batch_number,
                        "records_processed": records_processed,
                        "last_offset": last_offset,
                        "status": status,
                        "error_message": error_message,
                        "select_duration": int(self.last_select_duration) if self.last_select_duration else None,
                        "insert_duration": insert_duration_ms,
                        "table_name": table_name,
                        "rel_type": self.relationship_type or None,
                        "indexes_created": indexes_created,
                        "completed": completed,
                    },
                )

                if result.rowcount == 0:
                    raise MigrationError(f"No progress record found for {table_name}")

                session.commit()

        except MigrationError:
            raise
        except Exception as e:
            logger.error(f"Failed to update progress: {e}")
            raise MigrationError(f"Could not update migration progress: {e}")

    def migrate(self, start_offset: int = 0, mode: str = "create") -> None:
        """Main migration entry point with resume capability."""

        # =========================================================
        #                     Progress Check
        # =========================================================
        progress = self.get_progress_record(table_name=self.node_label or self.table_name)
        if progress:
            if progress["completed"] and progress["indexes_created"]:
                logger.success(f"✓ {self.table_name} already completed - skipping")
                return
            elif progress["status"] == "in_progress":
                # Resume from last successful batch
                start_offset = progress["last_offset"]
                self.current_batch = progress["batch_number"]
                self.created = progress["records_processed"]
                logger.info(f"Resuming {self.table_name} from offset {start_offset:,} (batch {self.current_batch})")
            elif progress["status"] == "failed":
                logger.warning(f"Previous migration failed: {progress['error_message']}")
                logger.info(f"Retrying from offset {progress['last_offset']:,}")
                start_offset = progress["last_offset"]
                self.current_batch = progress["batch_number"]
                self.created = progress["records_processed"]

        try:
            if self.migration_type == "node":
                # =========================================================
                #                     Node Migration
                # =========================================================
                # -------------------------
                #   Get Total Records
                # -------------------------
                total_records = 0
                with db_connections.pg_session() as pg_session:
                    records = pg_session.execute(text(f"SELECT * FROM {self.table_name}")).fetchall()
                    total_records = len(records) or 0

                    # Create progress record if doesn't exist
                    if not progress:
                        progress = self.create_progress_record(
                            table_name=self.node_label or self.table_name,
                            total_records=total_records,
                        )

                if progress and not progress["completed"]:
                    # -------------------------------------------
                    #   Pass Records to subclass create method
                    # -------------------------------------------
                    with db_connections.graph_session() as graph_session:
                        self.create(records, graph_session)
                        self.run_verification_query(graph_session, self.table_name)

                    self.update_progress(
                        table_name=self.node_label or self.table_name,
                        batch_number=1,
                        records_processed=len(records),
                        last_offset=len(records),
                        status="completed",
                        completed=True,
                    )
                    logger.success(f"✓ Completed {self.node_label} nodes")
                # -------------------------------------------
                #           Create Node Indexes
                # -------------------------------------------
                if progress and not progress["indexes_created"]:
                    logger.info(f"Creating indexes for {self.node_label or self.table_name}")
                    try:
                        # TODO: figure out why error:
                        # example - 'fao_graph.AreaCode' does not exist
                        self.create_indexes()
                        self.update_progress(
                            table_name=self.node_label or self.table_name,
                            batch_number=1,
                            records_processed=len(records),
                            last_offset=len(records),
                            indexes_created=True,
                            completed=progress["completed"],
                        )
                    except Exception as e:
                        logger.error(f"Failed to create indexes: {e}")

            else:
                # =========================================================
                #               Batch Relationship Migration
                # =========================================================
                rel_desc = f" ({self.relationship_type})" if self.relationship_type else ""
                logger.info(f"Starting {self.table_name}{rel_desc} migration from offset {start_offset:,}")
                # -------------------------
                #   Get Total Records
                # -------------------------
                with db_connections.pg_session() as pg_session:
                    total_records = pg_session.execute(text(self.get_total_rows_query())).scalar() or 0
                    logger.info(f"Total records to process: {total_records:,}")

                if total_records == 0:
                    logger.debug(f"No records to migrate for {self.table_name}{rel_desc}")
                    if not progress:
                        progress = self.create_progress_record(
                            table_name=self.table_name,
                            total_records=0,
                        )
                    self.update_progress(
                        table_name=self.table_name,
                        batch_number=0,
                        records_processed=0,
                        last_offset=0,
                        status="completed",
                        indexes_created=True,
                        completed=True if progress and progress["completed"] else False,
                    )
                    return
                # -------------------------------------------
                #   Create progress record if doesn't exist
                # -------------------------------------------
                if not progress:
                    progress = self.create_progress_record(
                        table_name=self.table_name,
                        total_records=total_records,
                    )
                # -------------------------
                #   Batch Processing
                # -------------------------
                offset = start_offset
                logger.debug(
                    f"LOOP START: offset={offset}, total_records={total_records}, condition: {offset} < {total_records} = {offset < total_records}"
                )
                while offset < total_records:
                    logger.info(f"LOOP ITERATION {self.current_batch}: offset={offset}, total_records={total_records}")
                    # --------------------------------------------
                    #   Select Records from PostgreSQL DB
                    # --------------------------------------------
                    with db_connections.pg_session() as pg_session:
                        select_start = time()
                        logger.debug(f"Executing query with LIMIT={self.batch_size}, OFFSET={offset}")
                        result = pg_session.execute(
                            text(self.get_migration_query()),
                            {
                                "limit": self.batch_size,
                                "offset": offset,
                            },
                        )
                        records = result.fetchall()
                        logger.debug(f"Query returned {len(records)} records")
                        self.last_select_duration = (time() - select_start) * 1000

                        if not records:
                            logger.debug(
                                f"No more records to process. offset {offset} of {total_records} total records. Ending migration."
                            )
                            break
                    # ---------------------------------------------------------------
                    #   Pass Records to subclass create method (Apache AGE Session)
                    # ---------------------------------------------------------------
                    with db_connections.graph_session() as graph_session:
                        insert_start = time()
                        try:
                            if mode == "create":
                                self.create(records, graph_session)
                            elif mode == "update":
                                self.update(records, graph_session)

                            # Update progress after successful batch
                            old_offset = offset
                            offset += len(records)
                            self.current_batch += 1
                            logger.warning(f"Offset updated: {old_offset} + {len(records)} = {offset}")
                            logger.warning(
                                f"Next iteration check: {offset} < {total_records} = {offset < total_records}"
                            )
                            if offset > total_records + self.batch_size:
                                logger.error(f"OFFSET OVERRUN: offset={offset} is way past total={total_records}!")
                                logger.error(f"This should never happen - investigating...")
                                break

                            insert_duration = (time() - insert_start) * 1000
                            self.update_progress(
                                table_name=self.table_name,
                                batch_number=self.current_batch,
                                records_processed=self.created,
                                last_offset=offset,
                                status="in_progress",
                                insert_duration_ms=int(insert_duration),
                                indexes_created=True,
                                completed=True if progress and progress["completed"] else False,
                            )

                            # Log progress
                            pct_complete = self.created / total_records * 100
                            self.log_progress(self.created, total_records, pct_complete)

                        except Exception as e:
                            # Record failure
                            insert_duration = (time() - insert_start) * 1000
                            self.update_progress(
                                table_name=self.table_name,
                                batch_number=self.current_batch,
                                records_processed=self.created,
                                last_offset=offset,
                                status="failed",
                                error_message=str(e),
                                insert_duration_ms=int(insert_duration),
                                indexes_created=True,
                                completed=True if progress and progress["completed"] else False,
                            )
                            logger.error(f"Failed at offset {offset}: {e}")
                            raise

                logger.debug(f"LOOP EXITED: final offset={offset}, total_records={total_records}")
                logger.debug(f"Exit condition met: {offset} >= {total_records}")
                # Mark as completed
                self.update_progress(
                    table_name=self.table_name,
                    batch_number=self.current_batch,
                    records_processed=self.created,
                    last_offset=offset,
                    status="completed",
                    indexes_created=True,
                    completed=True if progress and progress["completed"] else False,
                )
                logger.success(f"✓ Completed {self.table_name}{rel_desc}")

        except KeyboardInterrupt as e:
            logger.warning(f"Migration interrupted by user: {str(e)}")
            self.update_progress(
                table_name=self.node_label or self.table_name,
                batch_number=self.current_batch,
                records_processed=self.created,
                last_offset=self.created,
                status="failed",
                error_message=f"User interrupted: {str(e)}",
                completed=True if progress and progress["completed"] else False,
            )
            raise
        except MigrationError:
            raise
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            self.update_progress(
                table_name=self.node_label or self.table_name,
                batch_number=self.current_batch,
                records_processed=self.created,
                last_offset=self.created,
                status="failed",
                error_message=str(e),
                completed=True if progress and progress["completed"] else False,
            )
            raise MigrationError(f"Migration failed for {self.table_name}: {e}")

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
