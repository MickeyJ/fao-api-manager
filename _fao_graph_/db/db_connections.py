import psutil
from pathlib import Path
from contextlib import contextmanager
from typing import Generator, Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from _fao_graph_.logger import logger
from _fao_graph_.utils import load_sql
from _fao_graph_.core.settings import settings


class DatabaseConnections:
    """Manage both PostgreSQL and Neo4j connections."""

    def __init__(self) -> None:
        self._pg_engine: Optional[Engine] = None
        self._pg_session_factory: Optional[sessionmaker] = None

        self._graph_engine: Optional[Engine] = None
        self._graph_session_factory: Optional[sessionmaker] = None

        self._progress_table_exists = False
        self._progress_tracking_failed = False

    @property
    def pg_engine(self) -> Engine:
        """Lazy-load PostgreSQL engine."""
        if self._pg_engine is None:
            # Build PostgreSQL URL from settings
            pg_url = f"postgresql://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"

            self._pg_engine = create_engine(pg_url, pool_pre_ping=True, pool_size=10)
            self._pg_session_factory = sessionmaker(bind=self._pg_engine)
            logger.info("PostgreSQL engine created")
        return self._pg_engine

    @property
    def graph_engine(self) -> Engine:
        """Lazy-load Graph PostgreSQL engine with AGE setup."""
        if self._graph_engine is None:
            # Build PostgreSQL URL from settings
            pg_url = f"postgresql://{settings.graph_db_user}:{settings.graph_db_password}@{settings.graph_db_host}:{settings.graph_db_port}/{settings.graph_db_name}"

            self._graph_engine = create_engine(
                pg_url,
                pool_pre_ping=True,
                pool_size=10,
                # Execute AGE initialization on each new connection
                connect_args={"options": "-c search_path=ag_catalog,public"},
            )

            # Set up pool event to initialize AGE on each connection
            from sqlalchemy import event

            @event.listens_for(self._graph_engine, "connect")
            def receive_connect(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("LOAD 'age';")
                cursor.execute('SET search_path = ag_catalog, public, "$user";')
                cursor.close()

            self._graph_session_factory = sessionmaker(bind=self._graph_engine)
            logger.info("Graph PostgreSQL engine created with AGE initialization")

        return self._graph_engine

    @contextmanager
    def pg_session(self) -> Generator[Session, None, None]:
        """Context manager for PostgreSQL sessions."""
        # Ensure engine and factory are initialized
        _ = self.pg_engine  # This creates the factory as a side effect

        if self._pg_session_factory is None:
            raise RuntimeError("PostgreSQL session factory not initialized")

        session = self._pg_session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def graph_session(self) -> Generator[Any, None, None]:
        """Context manager for PostgreSQL sessions."""
        # Ensure engine and factory are initialized
        _ = self.graph_engine  # This creates the factory as a side effect

        if self._graph_session_factory is None:
            raise RuntimeError("Graph PostgreSQL session factory not initialized")

        session = self._graph_session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        """Clean up connections."""
        if self._pg_engine:
            self._pg_engine.dispose()
        if self._graph_engine:
            self._graph_engine.dispose()

    def ensure_progress_table(self):
        """Create progress table if it doesn't exist"""
        if self._progress_table_exists:
            return

        try:
            with self.graph_session() as session:
                # Check if table exists first
                check_sql = text(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'migration_progress'
                    )
                """
                )
                exists = session.execute(check_sql).scalar()

                if not exists:
                    create_table_sql = load_sql("create_migration_progress_table.sql", Path(__file__).parent)
                    session.execute(text(create_table_sql))
                    logger.info("Created migration progress table")
                else:
                    logger.info("Migration progress table already exists")

                self._progress_table_exists = True

        except Exception as e:
            logger.error(f"Failed to ensure progress table: {e}")

    def _record_progress(self, session, table_name, relationship_type, **kwargs):
        """Record migration progress - best effort, don't fail migration"""
        if self._progress_tracking_failed:
            logger.debug("Skipping progress tracking due to previous failure")
            return

        try:
            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024

            progress_sql = text(
                """
                INSERT INTO migration_progress (
                    migration_type, table_name, relationship_type,
                    batch_number, batch_size, records_processed,
                    select_duration_ms, insert_duration_ms, total_duration_ms,
                    cumulative_records, memory_usage_mb, error_message
                ) VALUES (
                    :migration_type, :table_name, :relationship_type,
                    :batch_number, :batch_size, :records_processed,
                    :select_duration_ms, :insert_duration_ms, 
                    COALESCE(:select_duration_ms, 0) + COALESCE(:insert_duration_ms, 0),
                    :cumulative_records, :memory_usage_mb, :error_message
                )
                """
            )

            session.execute(
                progress_sql,
                {
                    "migration_type": "relationship",
                    "table_name": table_name,
                    "relationship_type": relationship_type,
                    "memory_usage_mb": int(memory_mb),
                    **kwargs,
                },
            )
            # Note: Don't commit here - let the context manager handle it

        except Exception as e:
            # Log but don't fail the migration over progress tracking
            logger.debug(f"Could not record progress: {e}")
            # Could set a flag to stop trying if you want
            self._progress_tracking_failed = True


db_connections = DatabaseConnections()
