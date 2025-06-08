from typing import List, Dict, Literal
import re
from . import GLOBAL_COLUMN_RULES


# Usage example:
def analyze_column(sample_rows: List[Dict], column_name: str) -> Dict:
    """Complete column analysis"""
    checker = ValueTypeChecker()

    sql_type = checker.infer_column_type(sample_rows, column_name)
    stats = checker.get_column_stats(sample_rows, column_name)
    sql_type_size = None
    index = False

    if column_name in GLOBAL_COLUMN_RULES:
        column_rules = GLOBAL_COLUMN_RULES[column_name]
        index = column_rules.index
        # Only override the type if it's not already String
        if column_rules.sql_type:
            if column_name != "Year" or sql_type != "String":
                sql_type = column_rules.sql_type
                sql_type_size = column_rules.sql_type_size

    return {
        "csv_column_name": column_name,
        "sql_column_name": format_column_name(column_name),
        "inferred_sql_type": sql_type,
        "sql_type_size": sql_type_size,
        "index": index,
        "nullable": stats["null_count"] > 0,
        "null_count": stats["null_count"],
        "non_null_count": stats["non_null_count"],
        "unique_count": stats["unique_count"],
        "sample_values": stats["sample_values"],
    }


class ValueTypeChecker:
    """Infer SQL types from sample data values"""

    def __init__(self):
        # Common date formats in FAO data
        self.date_formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y",
            "%Y%m%d",
        ]

        # Boolean value patterns
        self.boolean_values = {
            "true",
            "false",
            "t",
            "f",
            "yes",
            "no",
            "y",
            "n",
            "1",
            "0",
            "x",
            "",  # FAO uses 'X' for flags
        }

    def infer_column_type(self, sample_rows: List[Dict], column_name: str) -> str:
        """
        Infer SQL type for a column based on sample values

        Args:
            sample_rows: List of dictionaries containing row data
            column_name: Name of column to analyze

        Returns:
            SQL type string: 'Integer', 'Float', 'Boolean', 'Date', 'String'
        """
        # Extract non-null values for this column
        values = []
        for row in sample_rows:
            val = row.get(column_name)
            if val is not None and str(val).strip() not in ["", "nan", "NaN", "NULL", "null"]:
                values.append(str(val).strip())

        if not values:
            return "String"  # Default for empty columns

        # Clean values (remove quotes, special characters)
        cleaned_values = []
        for val in values:
            # Remove leading quotes (common in FAO data like '012)
            cleaned = val.strip().lstrip("'").lstrip('"')
            cleaned_values.append(cleaned)

        if self._should_be_string(cleaned_values, column_name):
            return "String"

        # Check types in order of restrictiveness
        if self._all_boolean(cleaned_values):
            return "Boolean"

        if self._all_integer(cleaned_values):
            return "Integer"

        if self._all_float(cleaned_values):
            return "Float"

        if self._all_date(cleaned_values):
            return "Date"

        # Default to String
        return "String"

    def _should_be_string(self, values: List[str], column_name: str) -> bool:
        """Check if values should be treated as strings regardless of numeric appearance"""

        # Column name patterns that should always be strings
        string_column_patterns = [
            "code",
            "flag",
            "note",
            "description",
            "name",
            "unit",
            "source",
            "comment",
            "type",
            "category",
        ]

        # Check column name
        col_lower = column_name.lower()
        for pattern in string_column_patterns:
            if pattern in col_lower:
                return True

        # Check value patterns
        for val in values:

            try:
                float(val)  # This handles negative numbers and decimals correctly
                # If it succeeds, continue checking other patterns
            except ValueError:
                # Not a valid number - if it has ANY special characters, it's a string
                if not val.replace("_", "").isalnum():  # Allow underscores for some codes
                    return True

            # Leading zeros (codes like '001', '012')
            if val.startswith("0") and len(val) > 1 and val.isdigit():
                return True

            # Mixed alphanumeric
            has_alpha = any(c.isalpha() for c in val)
            has_digit = any(c.isdigit() for c in val)
            if has_alpha and has_digit:
                return True

            # Contains special characters (except . and -)
            special_chars = set(val) - set("0123456789.-")
            if special_chars:
                return True

            # Very long numeric strings (likely identifiers)
            if val.isdigit() and len(val) > 10:
                return True

        return False

    def _all_boolean(self, values: List[str]) -> bool:
        """Check if all values are boolean-like"""
        if not values:
            return False

        for val in values:
            if val.lower() not in self.boolean_values:
                return False
        return True

    def _all_integer(self, values: List[str]) -> bool:
        """Check if all values are integers"""
        if not values:
            return False

        for val in values:
            try:
                # Handle negative numbers
                int_val = int(val)

                # Check if it's too large for SQL Integer (PostgreSQL int is -2147483648 to 2147483647)
                if int_val < -2147483648 or int_val > 2147483647:
                    return False

            except ValueError:
                # Check for comma-separated thousands (e.g., "1,234")
                try:
                    int(val.replace(",", ""))
                except ValueError:
                    return False

        return True

    def _all_float(self, values: List[str]) -> bool:
        """Check if all values are floats"""
        if not values:
            return False

        has_decimal = False
        for val in values:
            try:
                # Remove commas for thousands
                clean_val = val.replace(",", "")
                float_val = float(clean_val)

                # Check if any value has decimals
                if "." in clean_val:
                    has_decimal = True

            except ValueError:
                return False

        # Only return Float if we found actual decimals
        # Otherwise integers misidentified as floats
        return has_decimal

    def _all_date(self, values: List[str]) -> bool:
        """Check if all values are dates"""
        if not values:
            return False

        # Quick check - if values are just 4-digit years, that's a date column
        if all(self._is_year(val) for val in values):
            return True

        # Try each date format
        for date_format in self.date_formats:
            if self._all_match_date_format(values, date_format):
                return True

        return False

    def _is_year(self, val: str) -> bool:
        """Check if value is a 4-digit year"""
        try:
            year = int(val)
            return 1900 <= year <= 2100
        except ValueError:
            return False

    def _all_match_date_format(self, values: List[str], date_format: str) -> bool:
        """Check if all values match a specific date format"""
        from datetime import datetime

        for val in values:
            try:
                datetime.strptime(val, date_format)
            except ValueError:
                return False
        return True

    def get_column_stats(self, sample_rows: List[Dict], column_name: str) -> Dict:
        """
        Get additional statistics about a column

        Returns dict with:
            - null_count
            - non_null_count
            - unique_count
            - has_empty_strings
            - sample_values
        """
        import pandas as pd

        all_values = [row.get(column_name) for row in sample_rows]

        # Check for both None and pandas NaN
        null_count = sum(1 for v in all_values if v is None or pd.isna(v))
        empty_string_count = sum(1 for v in all_values if v == "")

        non_null_values = [v for v in all_values if v is not None and not pd.isna(v) and str(v).strip() != ""]

        return {
            "null_count": null_count,
            "empty_string_count": empty_string_count,
            "non_null_count": len(non_null_values),
            "unique_count": len(set(str(v) for v in non_null_values)),
            "sample_values": all_values[:5],
            "has_empty_strings": empty_string_count > 0,
        }


def format_column_name(file_name: str) -> str:
    """Convert CSV name to database-friendly name"""
    return file_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace(".", "_")
