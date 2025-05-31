import pandas as pd


def strip_quote(df: pd.DataFrame, column_name, quote="'"):
    return df[column_name].str.replace(quote, "").str.strip()


def load_csv(CSV_PATH: str) -> pd.DataFrame:
    """Load and preview data."""
    try:
        df = pd.read_csv(CSV_PATH, dtype=str)
    except FileNotFoundError:
        print(f"File not found: {CSV_PATH}")
        return pd.DataFrame()

    print(f"Loading: {CSV_PATH}")

    df.columns = df.columns.str.strip()

    print(df.shape)
    print(df.head(5))
    return df
