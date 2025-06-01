import pandas as pd

SMALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\small"
MEDIUM_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\medium"
LARGE_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\large"
ALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\all"

ZIP_PATH = ALL_ZIP_EXAMPLE


def strip_quote(df: pd.DataFrame, column_name, quote="'"):
    return df[column_name].str.replace(quote, "").str.strip()


def get_csv_path_for(filename):
    return f"{ZIP_PATH}/{filename}"


def load_csv(csv_paths) -> pd.DataFrame:
    """Load and preview data from single file or multiple files."""
    # Handle both single path and list of paths
    if isinstance(csv_paths, str):
        csv_paths = [csv_paths]

    dfs = []
    for csv_path in csv_paths:
        try:
            df = pd.read_csv(csv_path, dtype=str)
            print(f"Loading: {csv_path}")
            df.columns = df.columns.str.strip()
            print(df.shape)
            if not df.empty:
                dfs.append(df)
        except FileNotFoundError:
            print(f"File not found: {csv_path}")
            continue

    if not dfs:
        return pd.DataFrame()

    # This works whether dfs has 1 or many DataFrames
    result_df = pd.concat(dfs, ignore_index=True)
    if len(dfs) > 1:
        print(f"Combined {len(dfs)} files into {result_df.shape}")
    print(result_df.head(5))

    return result_df
