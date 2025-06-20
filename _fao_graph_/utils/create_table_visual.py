# utils/create_table_visual.py
import argparse
import pandas as pd
import dataframe_image as dfi
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

from db.database import get_engine


def create_table_visual(sql_file: str, output_dir: str = "visuals", max_rows: int = 50):
    """
    Create a visual table from SQL query file

    Args:
        sql_file: Path to SQL file
        output_dir: Directory to save output (default: visuals)
        max_rows: Maximum rows to display (default: 50)
    """
    # Setup paths
    sql_path = Path(sql_file)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Load SQL
    print(f"Loading SQL from: {sql_path}")
    sql_query = sql_path.read_text()

    # Extract title from first line comment if present
    lines = sql_query.strip().split("\n")
    title = ""
    if lines[0].strip().startswith("--"):
        title = lines[0].replace("--", "").strip()
    else:
        title = sql_path.stem.replace("_", " ").title()

    # Run query
    print(f"Running query...")
    engine = get_engine()

    # Use text() to properly handle the SQL and connection context
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_query), connection)

    print(f"Query returned {len(df):,} rows and {len(df.columns)} columns")

    # Prepare display dataframe
    if len(df) > max_rows:
        df_display = df.head(max_rows).copy()
        display_title = f"{title} (First {max_rows} of {len(df):,} rows)"
    else:
        df_display = df.copy()
        display_title = title

    # Style the dataframe
    styled = df_display.style.set_properties(
        **{
            "text-align": "left",
            "font-size": "10pt",
        }
    )

    # Add gradient to numeric columns
    numeric_cols = df_display.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        styled = styled.background_gradient(cmap="Blues", subset=numeric_cols, low=0.1, high=0.3)

    # Style header and caption
    styled = styled.set_table_styles(
        [
            {"selector": "th", "props": [("background-color", "#40466e"), ("color", "white"), ("font-weight", "bold")]},
            {"selector": "caption", "props": [("caption-side", "top"), ("font-size", "14pt"), ("font-weight", "bold")]},
        ]
    ).set_caption(display_title)

    # Save output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{sql_path.stem}_{timestamp}.png"
    output_file = output_path / output_filename

    print(f"Saving visualization to: {output_file}")
    dfi.export(styled, output_file, max_rows=max_rows)

    # Also save CSV for reference
    csv_file = output_path / f"{sql_path.stem}_{timestamp}.csv"
    df.to_csv(csv_file, index=False)
    print(f"Also saved CSV to: {csv_file}")

    print("Done!")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Create table visualization from SQL file")
    parser.add_argument("sql_file", help="Path to SQL file")
    parser.add_argument("-o", "--output", default="outputs", help="Output directory (default: outputs)")
    parser.add_argument("-r", "--rows", type=int, default=50, help="Max rows to display (default: 50)")

    args = parser.parse_args()

    try:
        create_table_visual(args.sql_file, args.output, args.rows)
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
