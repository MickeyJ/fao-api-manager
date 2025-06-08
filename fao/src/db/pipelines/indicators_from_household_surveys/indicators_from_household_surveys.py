# templates/dataset_module.py.jinja2
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from fao.src.db.utils import load_csv, get_csv_path_for, generate_numeric_id
from fao.src.db.database import run_with_session
from .indicators_from_household_surveys_model import IndicatorsFromHouseholdSurveys

# Dataset CSV file
CSV_PATH = get_csv_path_for("Indicators_from_Household_Surveys_E_All_Data_(Normalized)/Indicators_from_Household_Surveys_E_All_Data_(Normalized).csv")

table_name = "indicators_from_household_surveys"
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
    df['Breakdown Variable Code'] = df['Breakdown Variable Code'].astype(str).str.strip().str.replace("'", "")
        
    df['Breakdown Variable'] = df['Breakdown Variable'].astype(str).str.strip().str.replace("'", "")
        
    df['Breadown by Sex of the Household Head Code'] = df['Breadown by Sex of the Household Head Code'].astype(str).str.strip().str.replace("'", "")
        
    df['Breadown by Sex of the Household Head'] = df['Breadown by Sex of the Household Head'].astype(str).str.strip().str.replace("'", "")
        
    df['Unit'] = df['Unit'].astype(str).str.strip().str.replace("'", "")
        
    df['Value'] = df['Value'].astype(str).str.strip().str.replace("'", "")
        
    df['Value'] = df['Value'].replace({'<0.1': 0.05, 'nan': None})
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    df['Flag'] = df['Flag'].str.upper()
    
    # Generate hash IDs for foreign key columns
    dataset_name = "indicators_from_household_surveys"  # This dataset's name
    
    # Generate hash IDs for Survey Code
    df['survey_code_id'] = df['Survey Code'].apply(
        lambda val: generate_numeric_id(
            {
                'Survey Code': str(val),
                'source_dataset': dataset_name,
            },
            ["Survey Code", "source_dataset"]
        ) if pd.notna(val) and str(val).strip() else None
    )
    # Generate hash IDs for Indicator Code
    df['indicator_code_id'] = df['Indicator Code'].apply(
        lambda val: generate_numeric_id(
            {
                'Indicator Code': str(val),
                'source_dataset': dataset_name,
            },
            ["Indicator Code", "source_dataset"]
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
    columns_to_drop = [col for col in ["Element", "Element Code", "Flag", "Indicator", "Indicator Code", "Survey", "Survey Code"] if col in df.columns]
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
            record['survey_code_id'] = row['survey_code_id']
            record['indicator_code_id'] = row['indicator_code_id']
            record['element_code_id'] = row['element_code_id']
            record['flag_id'] = row['flag_id']
            record['breakdown_variable_code'] = row['Breakdown Variable Code']
            record['breakdown_variable'] = row['Breakdown Variable']
            record['breadown_by_sex_of_the_household_head_code'] = row['Breadown by Sex of the Household Head Code']
            record['breadown_by_sex_of_the_household_head'] = row['Breadown by Sex of the Household Head']
            record['unit'] = row['Unit']
            record['value'] = row['Value']
            records.append(record)
        
        if records:
            try:
                stmt = pg_insert(IndicatorsFromHouseholdSurveys).values(records)
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