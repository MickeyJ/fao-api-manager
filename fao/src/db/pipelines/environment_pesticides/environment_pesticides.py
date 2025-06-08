# templates/dataset_module.py.jinja2
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fao.src.db.utils import load_csv, get_csv_path_for, generate_numeric_id
from fao.src.db.database import run_with_session
from .environment_pesticides_model import EnvironmentPesticides

# Dataset CSV file
CSV_PATH = get_csv_path_for("Environment_Pesticides_E_All_Data_(Normalized)/Environment_Pesticides_E_All_Data_(Normalized).csv")

table_name = "environment_pesticides"
CHUNK_SIZE = 10000  # Process in chunks for large datasets


def load():
    """Load the dataset CSV file"""
    return load_csv(CSV_PATH)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare dataset data"""
    if df.empty:
        print(f"No {table_name} data to clean.")
        return df

    print(f"\nCleaning {table_name} data...")
    initial_count = len(df)

    # Replace 'nan' strings with None for ALL columns
    df = df.replace({'nan': None, 'NaN': None, 'NAN': None})

    
    # Basic column cleanup
    df['Year Code'] = df['Year Code'].astype(str).str.strip().str.replace("'", "")
        
    df['Year'] = df['Year'].astype(str).str.strip().str.replace("'", "")
        
    df['Unit'] = df['Unit'].astype(str).str.strip().str.replace("'", "")
        
    df['Value'] = df['Value'].astype(str).str.strip().str.replace("'", "")
        
    df['Value'] = df['Value'].replace({'<0.1': 0.05, 'nan': None})
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    df['Flag'] = df['Flag'].str.upper()
    
    # Generate hash IDs for foreign key columns
    dataset_name = "environment_pesticides"  # This dataset's name
    
    # Generate hash IDs for Area Code
    df['area_code_id'] = df['Area Code'].apply(
        lambda val: generate_numeric_id(
            {
                'Area Code': str(val),
                'source_dataset': dataset_name,
            },
            ["Area Code", "source_dataset"]
        ) if pd.notna(val) and str(val).strip() else None
    )
    # Generate hash IDs for Item Code
    df['item_code_id'] = df['Item Code'].apply(
        lambda val: generate_numeric_id(
            {
                'Item Code': str(val),
                'source_dataset': dataset_name,
            },
            ["Item Code", "source_dataset"]
        ) if pd.notna(val) and str(val).strip() else None
    )
    # Generate hash IDs for Element Code
    df['element_code_id'] = df['Element Code'].apply(
        lambda val: generate_numeric_id(
            {
                'Element Code': str(val),
                'source_dataset': dataset_name,
            },
            ["Element Code", "source_dataset"]
        ) if pd.notna(val) and str(val).strip() else None
    )
    # Generate hash IDs for Flag
    df['flag_id'] = df['Flag'].apply(
        lambda val: generate_numeric_id(
            {
                'Flag': str(val),
            },
            ["Flag"]
        ) if pd.notna(val) and str(val).strip() else None
    )
    
    # Remove redundant columns that can be looked up via foreign keys
    columns_to_drop = [col for col in ["Area", "Area Code", "Area Code (M49)", "Element", "Element Code", "Flag", "Item", "Item Code"] if col in df.columns]
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
        print(f"  Dropped redundant columns: {columns_to_drop}")
    
    # Remove complete duplicates
    df = df.drop_duplicates()
    
    final_count = len(df)
    print(f"  Cleaned: {initial_count} → {final_count} rows")
    return df


def insert(df: pd.DataFrame, session: Session):
    """Insert dataset data with chunking for large files"""
    if df.empty:
        print(f"No {table_name} data to insert.")
        return
    
    print(f"\nInserting {table_name} data ({len(df):,} rows)...")
    
    total_rows = len(df)
    total_inserted = 0
    
    # Process in chunks
    for chunk_idx, start_idx in enumerate(range(0, total_rows, CHUNK_SIZE)):
        end_idx = min(start_idx + CHUNK_SIZE, total_rows)
        chunk_df = df.iloc[start_idx:end_idx]
        
        records = []
        for _, row in chunk_df.iterrows():
            record = {}
            # Add the hash ID columns
            record['area_code_id'] = row['area_code_id']
            record['item_code_id'] = row['item_code_id']
            record['element_code_id'] = row['element_code_id']
            record['flag_id'] = row['flag_id']
            record['year_code'] = row['Year Code']
            record['year'] = row['Year']
            record['unit'] = row['Unit']
            record['value'] = row['Value']
            records.append(record)
        
        if records:
            try:
                stmt = pg_insert(EnvironmentPesticides).values(records)
                stmt = stmt.on_conflict_do_nothing()
                result = session.execute(stmt)
                session.commit()
                
                total_inserted += result.rowcount
                print(f"  Chunk {chunk_idx + 1}: Inserted {result.rowcount} rows " +
                      f"(Total: {total_inserted:,}/{total_rows:,})")
            except Exception as e:
                print(f"  ❌ Error in chunk {chunk_idx + 1}: {e}")
                session.rollback()
                raise
    
    print(f"✅ {table_name} insert complete: {total_inserted:,} rows inserted")


def run(db):
    """Run the complete ETL pipeline for this dataset"""
    df = load()
    df = clean(df)
    insert(df, db)


if __name__ == "__main__":
    run_with_session(run)