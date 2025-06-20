from pathlib import Path


def load_sql(filename: str, base_dir: Path) -> str:
    """Load SQL query from file relative to base_dir"""

    sql_path = base_dir / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text()
