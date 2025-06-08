import pandas as pd
import zipfile, hashlib
from pathlib import Path


SMALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\small"
MEDIUM_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\medium"
LARGE_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\large"
ALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\all"

ZIP_PATH = ALL_ZIP_EXAMPLE


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
        value = str(row_data.get(col, "")).strip()
        values.append(value)

    # Create hash string
    content = "|".join(values)

    # Generate hash
    hash_bytes = hashlib.md5(content.encode("utf-8")).digest()

    # Convert to positive integer (PostgreSQL INTEGER max is 2147483647)
    numeric_id = int.from_bytes(hash_bytes[:8], byteorder="big")

    # Ensure it fits in PostgreSQL INTEGER type
    return numeric_id % 2147483647


def get_csv_path_for(csv_path):
    """Get CSV path, extracting from ZIP if necessary"""
    full_path = Path(ZIP_PATH) / csv_path

    if full_path.exists():
        return str(full_path)

    # Split path and try to find/extract ZIP
    parts = Path(csv_path).parts
    if len(parts) >= 2:
        zip_name = parts[0] + ".zip"  # e.g., "Prices_E_All_Data_(Normalized).zip"
        zip_path = Path(ZIP_PATH) / zip_name

        if zip_path.exists():
            extract_dir = Path(ZIP_PATH) / parts[0]
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


# if __name__ == "__main__":
#     csv_path = (
#         Path(ZIP_PATH)
#         / "Individual_Quantitative_Dietary_Data_Food_and_Diet_E_All_Data_(Normalized)/Individual_Quantitative_Dietary_Data_Food_and_Diet_E_All_Data_(Normalized).csv"
#     )
#     df = pd.read_csv(csv_path, nrows=1000000)  # Sample first 10k rows

#     values = [
#         "All",
#         "9-18 years",
#         "9-13 years",
#         "14-18 years",
#         "19 years +",
#         "19-50 years",
#         "51 years +",
#         "< 12 months",
#         "1-8 years",
#         "1-3 years",
#         "4-8 years",
#     ]

#     cols = ["allages", "9t18y", "9t13y", "14t18y", "19ya", "19t50y", "51ya", "12mb", "1t8y", "1t3y", "4t8y"]

#     for i, col in enumerate(cols):
#         id = generate_numeric_id(
#             {
#                 "Population Age Group Code": col,
#                 "source_dataset": "individual_quantitative_dietary_data_food_and_diet",
#             },
#             ["Population Age Group Code", "source_dataset"],
#         )

#         print(
#             f"insert into population_age_groups (id, population_age_group_code, population_age_group, source_dataset, created_at, updated_at) values ({id}, '{col}', '{values[i]}', 'individual_quantitative_dietary_data_food_and_diet', current_timestamp, current_timestamp);"
#         )

#     # print(df["Population Age Group Code"].unique())
