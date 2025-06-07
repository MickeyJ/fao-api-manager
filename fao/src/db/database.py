# database.py
import os
from functools import lru_cache
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

load_dotenv(override=True)

# Build DATABASE_URL at module level (just string manipulation, no connection)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Base can be created immediately
Base = declarative_base()

@lru_cache
def get_engine():
    """Create engine only when needed"""
    print(f"DB_HOST: {DB_HOST}")  # Move the print here
    return create_engine(DATABASE_URL, echo=True)

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