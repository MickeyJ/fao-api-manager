# fao/src/db/database.py
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from _fao_.src.core import settings
from _fao_.logger import logger

# Build DATABASE_URL at module level (just string manipulation, no connection)
DB_USER = settings.db_user
DB_PASSWORD = settings.db_password
DB_HOST = settings.db_host
DB_PORT = settings.db_port
DB_NAME = settings.db_name

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Base can be created immediately
Base = declarative_base()


@lru_cache
def get_engine():
    """Create engine only when needed"""
    logger.success(f"DB connection: postgresql+psycopg2://{DB_USER}:[password]@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return create_engine(DATABASE_URL, echo=False)


@lru_cache
def get_session_factory():
    """Create session factory only when needed"""
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_db():
    """Dependency to get DB session"""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_with_session(fn):
    db = next(get_db())
    try:
        fn(db)
    finally:
        db.close()
