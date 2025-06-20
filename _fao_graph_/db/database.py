# _fao_graph_/db/database.py
from functools import lru_cache
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from typing import Iterator
from sqlalchemy.orm import Session, sessionmaker, declarative_base
from sqlalchemy.pool import NullPool
from _fao_graph_.core import settings
from _fao_graph_.logger import logger

# Build DATABASE_URL for AGE database
DB_USER = settings.graph_db_user
DB_PASSWORD = settings.graph_db_password
DB_HOST = settings.graph_db_host
DB_PORT = settings.graph_db_port
DB_NAME = settings.graph_db_name

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Base for any AGE-specific models (if needed)
Base = declarative_base()


@lru_cache
def get_engine():
    """Create engine only when needed"""
    logger.success(f"AGE DB connection: postgresql+psycopg2://{DB_USER}:[password]@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    # Use NullPool to avoid connection pool issues with AGE
    return create_engine(DATABASE_URL, echo=False, poolclass=NullPool)


@lru_cache
def get_session_factory():
    """Create session factory only when needed"""
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def initialize_age_session(session):
    """Initialize AGE extension for the session"""
    session.execute(text("LOAD 'age';"))
    session.execute(text('SET search_path = ag_catalog, "$user", public;'))
    session.commit()


@contextmanager
def get_session() -> Iterator[Session]:  # Add return type hint
    """Context manager for AGE-enabled sessions"""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        # Initialize AGE for this session
        initialize_age_session(session)
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db():
    """Dependency to get AGE-enabled DB session (for FastAPI-style usage)"""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        # Initialize AGE for this session
        initialize_age_session(db)
        yield db
    finally:
        db.close()


def run_with_session(fn):
    """Run a function with an AGE-enabled session"""
    with get_session() as session:
        fn(session)
