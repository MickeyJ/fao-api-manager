import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fao.src.db.utils import load_csv, get_csv_path_for, generate_numeric_id
from fao.src.db.database import run_with_session
from .sexs_model import Sexs


# Direct path to synthetic lookup CSV
CSV_PATH = get_csv_path_for("synthetic_lookups/sexs.csv")

table_name = "sexs"


def load():
    """Load the lookup CSV file"""
    return load_csv(CSV_PATH)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare lookup data"""
    if df.empty:
        print(f"No {table_name} data to clean.")
        return df

    print(f"\nCleaning {table_name} data...")
    initial_count = len(df)

    # Replace 'nan' strings with None for ALL columns
    df = df.replace({'nan': None, 'NaN': None, 'NAN': None})
    
    # Basic column cleanup
    df['Sex Code'] = df['Sex Code'].astype(str).str.strip().str.replace("'", "")
    # Keep primary key as string
    df['Sex'] = df['Sex'].astype(str).str.strip().str.replace("'", "")
    df['source_dataset'] = df['source_dataset'].astype(str).str.strip().str.replace("'", "")
    
   
    # Remove any remaining duplicates (shouldn't be any after PK updates)
    df = df.drop_duplicates(subset=['Sex Code', 'source_dataset'], keep='first')
    
    # Drop any rows with null PKs
    df = df.dropna(subset=['Sex Code'])
    
    final_count = len(df)
    print(f"  Cleaned: {initial_count} → {final_count} rows")
    return df


def insert(df: pd.DataFrame, session: Session):
    """Insert lookup data - simple bulk insert since conflicts are already resolved"""
    if df.empty:
        print(f"No {table_name} data to insert.")
        return
    
    print(f"\nInserting {table_name} data...")
    
    records = []
    for _, row in df.iterrows():
        record = {}
        
        # Generate hash ID from configured columns
        hash_columns = ["Sex Code", "source_dataset"]
        record['id'] = generate_numeric_id(row.to_dict(), hash_columns)
        record['sex_code'] = row['Sex Code']
        record['sex'] = row['Sex']
        record['source_dataset'] = row['source_dataset']
        records.append(record)
    
    if records:
        try:
            stmt = pg_insert(Sexs).values(records)
            stmt = stmt.on_conflict_do_nothing()
            result = session.execute(stmt)
            session.commit()
            print(f"  ✅ Inserted {result.rowcount} rows")
        except Exception as e:
            print(f"  ❌ Error during bulk insert: {e}")
            session.rollback()
            raise
    
    print(f"✅ {table_name} insert complete")


def run(db):
    """Run the complete ETL pipeline for this lookup table"""
    df = load()
    df = clean(df)
    insert(df, db)


if __name__ == "__main__":
    run_with_session(run)