import subprocess
import tempfile
from sqlalchemy import create_engine, inspect, text
from _fao_.src.db.database import Base, DATABASE_URL
from _fao_.src.db.system_models import *
from _fao_.all_model_imports import *
import os
import importlib
import sys


def schema_diff():
    # DEBUGGING: Force reload of modules
    print("Reloading model modules...")
    for module_name in list(sys.modules.keys()):
        if module_name.startswith("fao."):
            try:
                importlib.reload(sys.modules[module_name])
            except:
                pass

    # DEBUGGING: Show what tables are registered
    print(f"\nRegistered tables: {len(Base.metadata.tables)}")
    print(f"Table names: {list(Base.metadata.tables.keys())[:10]}...")  # First 10

    # What table did you change? Let's verify it's loaded
    print(f"\nLooking for your changed table...")
    if "dataset_metadata" in Base.metadata.tables:
        table = Base.metadata.tables["dataset_metadata"]
        print(f"Columns: {[c.name for c in table.columns]}")

    temp_db = f"temp_schema_{os.getpid()}"
    base_url = DATABASE_URL.rsplit("/", 1)[0]
    temp_url = f"{base_url}/{temp_db}"

    try:
        print(f"\nCreating temporary database {temp_db}...")
        subprocess.run(["createdb", temp_db], check=True)

        print("Creating fresh schema from models...")
        temp_engine = create_engine(temp_url)
        Base.metadata.create_all(temp_engine)

        print("\nComparing schemas...\n")
        result = subprocess.run(["migra", DATABASE_URL, temp_url], capture_output=True, text=True)

        if result.stdout:
            print("Schema differences found:")
            print(result.stdout)
        else:
            print("No schema differences found!")

        # DEBUGGING: Let's manually check one table
        actual_engine = create_engine(DATABASE_URL)
        actual_inspector = inspect(actual_engine)
        temp_inspector = inspect(temp_engine)

        table_name = "dataset_metadata"
        print(f"\nManual check of {table_name}:")
        print(f"Actual DB columns: {[c['name'] for c in actual_inspector.get_columns(table_name)]}")
        print(f"Model columns: {[c['name'] for c in temp_inspector.get_columns(table_name)]}")

    finally:
        print(f"\nCleaning up temporary database...")
        subprocess.run(["dropdb", temp_db])

    engine = create_engine(DATABASE_URL)
    inspector = inspect(engine)

    # Which table/column did you change?
    table_name = "dataset_metadata"  # <-- CHANGE THIS
    column_name = "dataset_code"  # <-- CHANGE THIS

    # Check database
    columns = inspector.get_columns(table_name)
    db_column = next((c for c in columns if c["name"] == column_name), None)

    if db_column:
        print(f"Database: {table_name}.{column_name} nullable = {db_column['nullable']}")

    # Check model
    if table_name in Base.metadata.tables:
        model_table = Base.metadata.tables[table_name]
        model_column = model_table.c.get(column_name)
        if model_column is not None:
            print(f"Model: {table_name}.{column_name} nullable = {model_column.nullable}")

    # Direct SQL check
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"""
            SELECT is_nullable 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND column_name = '{column_name}'
        """
            )
        )
        print(f"SQL check: nullable = {result.scalar()}")


def schema_diff_debug():
    temp_db = f"temp_schema_{os.getpid()}"
    base_url = DATABASE_URL.rsplit("/", 1)[0]
    temp_url = f"{base_url}/{temp_db}"

    try:
        print(f"Creating temporary database {temp_db}...")
        subprocess.run(["createdb", temp_db], check=True)

        print("Creating fresh schema from models...")
        temp_engine = create_engine(temp_url)
        Base.metadata.create_all(temp_engine)

        # DEBUG: Verify the temp database has the correct schema
        temp_inspector = inspect(temp_engine)

        # First check if table exists
        if "dataset_metadata" not in temp_inspector.get_table_names():
            print("ERROR: dataset_metadata table not created in temp database!")
            print(f"Available tables: {temp_inspector.get_table_names()}")
            return

        temp_columns = temp_inspector.get_columns("dataset_metadata")
        temp_col = next((c for c in temp_columns if c["name"] == "dataset_code"), None)

        if temp_col:
            print(f"Temp DB: dataset_metadata.dataset_code nullable = {temp_col['nullable']}")
        else:
            print("ERROR: dataset_code column not found in temp database!")
            print(f"Available columns: {[c['name'] for c in temp_columns]}")
            return

        print("\nRunning migra...")
        result = subprocess.run(["migra", "--unsafe", DATABASE_URL, temp_url], capture_output=True, text=True)

        if result.stderr:
            print(f"Migra stderr: {result.stderr}")

        if result.stdout:
            print("Schema differences found:")
            print(result.stdout)
        else:
            print("No differences found - this is wrong!")

    finally:
        subprocess.run(["dropdb", temp_db])


if __name__ == "__main__":
    # schema_diff()
    schema_diff_debug()
