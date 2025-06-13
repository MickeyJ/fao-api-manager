import pandas as pd
import zipfile, hashlib
from pathlib import Path
import os, sys
from dotenv import load_dotenv

load_dotenv(override=True)

FAO_ZIP_PATH = os.getenv("FAO_ZIP_PATH")

def calculate_optimal_chunk_size(df: pd.DataFrame, base_chunk_size: int = 10000) -> int:
    """
    Calculate optimal chunk size based on number of columns and PostgreSQL limits.
    
    PostgreSQL has a 65,535 parameter limit per statement.
    Each row uses (number of columns) parameters.
    """
    num_columns = len(df.columns)
    
    # PostgreSQL's parameter limit with safety margin
    max_params = 100000  # Leave some headroom from 65,535 limit
    
    # Calculate max rows that fit within parameter limit
    max_rows_by_params = max_params // num_columns
    
    # Set reasonable bounds
    min_chunk = 1000    # Don't go below this for efficiency
    max_chunk = 50000   # Cap at 50k for memory/timeout safety
    
    # Use the smaller of: parameter limit, memory limit, or base preference
    optimal_chunk = min(max_rows_by_params, base_chunk_size, max_chunk)
    optimal_chunk = max(optimal_chunk, min_chunk)  # Ensure minimum
    
    return optimal_chunk

def safe_index_name(table_name, column_name):
    # Always fits in 63 chars: ix_ + 8 hash chars + _ + column (max 50)
    table_hash = hashlib.md5(table_name.encode()).hexdigest()[:8]
    col_part = column_name[:50]  # Ensure total < 63
    return f"{table_hash}_{col_part}"

def generate_numeric_id(row_data: dict, hash_columns: list[str]) -> int:
    """Generate deterministic numeric ID from specified columns
    
    Args:
        row_data: Dictionary containing the row data
        hash_columns: List of column names to include in hash
        
    Returns:
        Positive integer suitable for database ID
    """
    # Extract values in consistent order
    values = []
    for col in sorted(hash_columns):  # Sort for consistency
        value = str(row_data.get(col, '')).strip()
        values.append(value)
    
    # Create hash string
    content = '|'.join(values)
    
    # Generate hash
    hash_bytes = hashlib.md5(content.encode('utf-8')).digest()
    
    # Convert to positive integer (PostgreSQL INTEGER max is 2147483647)
    numeric_id = int.from_bytes(hash_bytes[:8], byteorder='big')
    
    # Ensure it fits in PostgreSQL INTEGER type
    return numeric_id % 2147483647


def get_csv_path_for(csv_path):
    """Get CSV path, extracting from ZIP if necessary"""
    assert FAO_ZIP_PATH is not None, "FAO_ZIP_PATH must be set"

    full_path = Path(FAO_ZIP_PATH) / csv_path

    if full_path.exists():
        return str(full_path)

    # Split path and try to find/extract ZIP
    parts = Path(csv_path).parts
    if len(parts) >= 2:
        zip_name = parts[0] + ".zip"  # e.g., "Prices_E_All_Data_(Normalized).zip"
        zip_path = Path(FAO_ZIP_PATH) / zip_name

        if zip_path.exists():
            extract_dir = Path(FAO_ZIP_PATH) / parts[0]
            extract_dir.mkdir(exist_ok=True)

            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)

            print(f"Extracted {zip_name}")
            return str(full_path)  # Should exist now

    raise FileNotFoundError(f"Could not find or extract {csv_path}")


def extract_zip_if_needed(zip_path, csv_filename):
    """Extract ZIP to directory named after the ZIP file"""
    extract_dir = zip_path.parent / zip_path.stem
    extract_dir.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    print(f"Extracted {zip_path.name} to {extract_dir}")


def strip_quote(df: pd.DataFrame, column_name, quote="'"):
    return df[column_name].str.replace(quote, "").str.strip()


def load_csv(csv_path) -> pd.DataFrame:
    """Load and preview data from single file or multiple files."""
    # Handle both single path and list of paths

    try:
        # Try different encodings like CSVAnalyzer does
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
        df = None

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, dtype=str, encoding=encoding)
                print(f"Loading: {csv_path} (encoding: {encoding})")
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            # If all encodings fail, read with latin-1 (accepts any byte)
            df = pd.read_csv(csv_path, dtype=str, encoding="latin-1")
            print(f"Loading: {csv_path} (encoding: latin-1 fallback)")

        df.columns = df.columns.str.strip()
        print(df.shape)
       
    except FileNotFoundError as e:
        print(f"File not found: {csv_path}")
        raise e
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        raise e

    if not len(df):
        return pd.DataFrame()

    return df