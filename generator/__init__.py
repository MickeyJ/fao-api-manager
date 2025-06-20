import re, hashlib, random, string
import pandas as pd
from pathlib import Path
import os, sys
from dotenv import load_dotenv

load_dotenv(override=True)

FAO_ZIP_PATH = os.getenv("FAO_ZIP_PATH")
API_OUTPUT_PATH = os.getenv("FAO_API_OUTPUT_PATH")
GRAPH_OUTPUT_PATH = os.getenv("FAO_GRAPH_OUTPUT_PATH")


if not FAO_ZIP_PATH or not API_OUTPUT_PATH or not GRAPH_OUTPUT_PATH:
    print("❌ Error: FAO_ZIP_PATH and FAO_API_OUTPUT_PATH and GRAPH_OUTPUT_PATH must be set in .env file")
    sys.exit(1)


def singularize(name: str) -> str:
    """Convert plural to singular for common cases"""
    if name.endswith("ies"):
        return name[:-3] + "y"  # currencies -> currency
    elif name.endswith("ses"):
        return name[:-1]  # purposes -> purpose
    elif name.endswith("s"):
        return name[:-1]  # area_codes -> area_code
    return name


def format_column_name(file_name: str) -> str:
    """Convert CSV name to database-friendly name"""
    return file_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace(".", "_")


def random_string(length=8):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def safe_index_name(table_name, column_name):
    # Always fits in 63 chars: ix_ + 8 hash chars + _ + column (max 50)
    table_hash = hashlib.md5(table_name.encode()).hexdigest()[:8]
    col_part = column_name[:50]  # Ensure total < 63
    return f"{table_hash}_{col_part}"


def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9\s]", "", str(text).strip())


def to_snake_case(text: str) -> str:
    """Convert text to snake_case"""
    # Remove parentheses and their contents
    text = re.sub(r"\([^)]*\)", "", text)

    # Handle camelCase and PascalCase
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # Clean up and convert to lowercase
    result = s2.replace("-", "_").lower()
    # Remove multiple underscores
    result = re.sub("_+", "_", result)
    return result.strip("_").replace(" ", "")


def snake_to_pascal_case(snake_str: str) -> str:
    """Convert snake_case to PascalCase"""
    return "".join(word.capitalize() for word in snake_str.split("_"))


class ColumnRule:
    def __init__(
        self,
        name: str,
        sql_type: str,
        sql_type_size: int | None = None,
        nullable: bool = False,
        index: bool = False,
        is_primary_key: bool = False,
        foreign_key_model_name: str | None = None,
    ):
        self.name = name
        self.sql_type = sql_type
        self.sql_type_size = sql_type_size
        self.nullable = nullable
        self.index = index
        self.is_primary_key = is_primary_key
        self.foreign_key_model_name = foreign_key_model_name
        self.is_foreign_key = foreign_key_model_name is not None


# Global column rules applied to all tables
GLOBAL_COLUMN_RULES = {
    # Exact name matches
    "Flag": ColumnRule(name="Flag", sql_type="String", sql_type_size=1, index=True, nullable=False),
    "Year": ColumnRule(name="Year", sql_type="SmallInteger", index=True, nullable=False),
    "Year Code": ColumnRule(name="Year Code", sql_type="String", sql_type_size=8, nullable=False),
    "Unit": ColumnRule(name="Unit", sql_type="String", sql_type_size=50, nullable=False),
    "Value": ColumnRule(name="Value", sql_type="Float", nullable=False),
    "Note": ColumnRule(name="Note", sql_type="String", nullable=True),
    # Month columns
    "Months": ColumnRule(name="Months", sql_type="String", sql_type_size=20, nullable=False),
    "Months Code": ColumnRule(name="Months Code", sql_type="String", sql_type_size=4, nullable=False),
}
