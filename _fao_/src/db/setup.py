# scripts/init_db.py
from sqlalchemy import create_engine, text, inspect
from _fao_.logger import logger
from _fao_.src.db.database import Base, DATABASE_URL
from _fao_.all_model_imports import *
from _fao_.src.db.system_models import *
from _fao_.src.db.views import ALL_VIEWS, ALL_DROP_VIEWS, refresh_views_sql, create_view_indexes_sql


def create_views(engine):
    """Create all views and materialized views"""

    with engine.connect() as conn:
        for view_name, view_sql in ALL_VIEWS.items():
            logger.info(f"Creating view {view_name}...")
            try:
                conn.execute(text(view_sql))
                conn.commit()  # Commit after EACH view

                # Verify it was created
                result = conn.execute(
                    text(f"SELECT EXISTS (SELECT FROM pg_matviews WHERE matviewname = '{view_name}')")
                )
                exists = result.scalar()
                if exists:
                    logger.debug(f"  ✓ View {view_name} created successfully")
                else:
                    logger.warning(f"  ✗ View {view_name} was NOT created!")

            except Exception as e:
                logger.error(f"  ✗ ERROR creating {view_name}: {e}")
                raise

    # Then create indexes with CONCURRENTLY (requires autocommit)
    print("Creating indexes (this may take a few minutes)...")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # Split the SQL file into individual statements
        statements = create_view_indexes_sql.split(";")

        for statement in statements:
            statement = statement.strip()
            if statement:  # Skip empty statements
                # Extract index name for progress reporting (optional)
                if "CREATE INDEX" in statement:
                    index_name = statement.split("IF NOT EXISTS")[1].split("ON")[0].strip()
                    logger.info(f"  Creating index {index_name}...")

                try:
                    conn.execute(text(statement))
                except Exception as e:
                    # IF NOT EXISTS might not work with CONCURRENTLY in older Postgres
                    if "already exists" in str(e):
                        logger.info(f"    Index already exists, skipping...")
                    else:
                        raise


def refresh_views(engine):
    """Refresh materialized views and create indexes"""
    with engine.connect() as conn:
        logger.info("Refreshing materialized views...")
        conn.execute(text(refresh_views_sql))

        conn.commit()


def update_database(engine):
    """Drop and recreate everything"""

    logger.info(f"{len(list(Base.metadata.tables.keys()))} registered tables")

    # Show what tables exist
    inspector = inspect(engine)
    existing = inspector.get_table_names()
    logger.info(f"Existing tables: {existing}")

    # Create tables
    logger.info("Creating tables...")
    Base.metadata.create_all(engine, checkfirst=True)

    # Show what was created
    new_tables = set(Base.metadata.tables.keys()) - set(existing)
    if new_tables:
        logger.info(f"Created tables: {new_tables}")


def drop_views(engine):
    """Nuclear option - drop everything and start fresh"""

    # Drop views first
    logger.info(f"Dropping all views...")
    with engine.connect() as conn:
        for view_name, drop_sql in ALL_DROP_VIEWS.items():
            logger.info(f"Dropping view {view_name}")
            try:
                conn.execute(text(drop_sql))
                logger.debug(f"  ✓ Dropped {view_name}")
            except Exception as e:
                logger.error(f"  Could not drop {view_name}: {e}")
                # Continue - it might not exist
        conn.commit()

    # Drop all tables
    # Base.metadata.drop_all(engine)
    # logger.info("Dropped everything!")


def reset_database(engine):
    """Drop and recreate everything"""
    drop_views(engine)
    update_database(engine)


if __name__ == "__main__":
    import sys

    engine = create_engine(DATABASE_URL)

    if len(sys.argv) > 1:
        if sys.argv[1] == "update":
            update_database(engine)
        elif sys.argv[1] == "drop-views":
            drop_views(engine)
        elif sys.argv[1] == "refresh-views":
            refresh_views(engine)
        elif sys.argv[1] == "create-views":
            create_views(engine)
    else:
        logger.info("Usage: python -m fao.src.db.setup [ reset | drop-views | refresh-views | create-views ]")
        sys.exit(1)
