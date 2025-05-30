import zipfile
from io import StringIO
import pandas as pd
from pathlib import Path
from typing import Dict, List
from . import ZIP_PATH


class CSVAnalyzer:

    def analyze_csv_from_zip(self, zip_path: Path, csv_filename: str) -> Dict:
        """Analyze a CSV file directly from inside a ZIP"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(csv_filename) as csv_file:
                # Try different encodings
                encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                for encoding in encodings:
                    try:
                        csv_file.seek(0)
                        csv_data = csv_file.read().decode(encoding)
                        df = pd.read_csv(StringIO(csv_data), dtype=str)
                        return self._analyze_dataframe(df, csv_filename, encoding)
                    except UnicodeDecodeError:
                        continue

                # If all encodings fail, use errors='ignore'
                csv_file.seek(0)
                csv_data = csv_file.read().decode("utf-8", errors="ignore")
                df = pd.read_csv(StringIO(csv_data), dtype=str)
                return self._analyze_dataframe(
                    df, csv_filename, "utf-8 (with errors ignored)"
                )

    def _analyze_dataframe(
        self, df: pd.DataFrame, csv_filename: str, encoding: str
    ) -> Dict:
        """Analyze a DataFrame and return structured information"""
        # Clean column names
        df.columns = df.columns.str.strip()

        # Analyze each column for type inference
        column_analysis = []
        for col in df.columns:
            col_info = self._analyze_column(df[col].head(100), col)
            column_analysis.append(col_info)

        # Sample rows
        sample_rows = df.head(2).to_dict("records")

        return {
            "file_name": csv_filename,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": df.columns.tolist(),
            "suggested_db_column_names": [
                self._suggest_db_column_name(col) for col in df.columns
            ],
            "encoding_used": encoding,
            "sample_rows": sample_rows,
            "column_analysis": column_analysis,
        }

    def _analyze_column(self, series, column_name: str) -> Dict:
        """Analyze a single column to infer type and properties"""
        sample_values = series.dropna().head(4).tolist()
        non_null_count = int(series.count())  # Convert to Python int
        total_count = len(series)
        null_count = total_count - non_null_count
        unique_count = int(series.nunique())  # Convert to Python int

        # Infer SQL type
        inferred_type = self._infer_sql_type(series, column_name)
        suggested_db_column_name = self._suggest_db_column_name(column_name)

        return {
            "column_name": column_name,
            "suggested_db_column_name": suggested_db_column_name,
            "sample_values": sample_values,
            "null_count": null_count,
            "non_null_count": non_null_count,
            "unique_count": series.nunique(),
            "inferred_sql_type": inferred_type,
            "is_likely_foreign_key": self._is_likely_foreign_key(column_name),
        }

    def _suggest_db_column_name(self, column_name: str) -> str:
        """Convert CSV column name to database-friendly name"""
        return (
            column_name.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
        )

    def _infer_sql_type(self, series, column_name: str) -> str:
        """Infer SQLAlchemy column type from data patterns"""
        # Drop nulls for analysis
        clean_series = series.dropna()

        if len(clean_series) == 0:
            return "String"

        # Check for specific FAO patterns first
        if self._is_year_column(column_name, clean_series):
            return "Integer"

        if self._is_code_column(column_name, clean_series):
            return "Integer"  # Most FAO codes are integers

        if self._is_value_column(column_name, clean_series):
            return "Float"

        # General pattern matching
        if self._is_integer_pattern(clean_series):
            return "Integer"

        if self._is_float_pattern(clean_series):
            return "Float"

        # Default to String
        return "String"

    def _is_year_column(self, column_name: str, series) -> bool:
        """Check if this looks like a year column"""
        if "year" in column_name.lower():
            try:
                clean_values = self._clean_quoted_values(series)
                numeric_values = pd.to_numeric(clean_values, errors="coerce").dropna()
                if len(numeric_values) > 0:
                    return numeric_values.between(1900, 2030).all()
            except:
                pass
        return False

    def _is_code_column(self, column_name: str, series) -> bool:
        """Check if this looks like an area/item/element code"""
        code_patterns = ["code", "area code", "item code", "element code", "m49"]
        if any(pattern in column_name.lower() for pattern in code_patterns):
            return True
        return False

    def _is_value_column(self, column_name: str, series) -> bool:
        """Check if this looks like a numeric value column"""
        value_patterns = ["value", "price", "amount", "quantity", "rate"]
        return any(pattern in column_name.lower() for pattern in value_patterns)

    def _clean_quoted_values(self, series):
        """Remove quotes from values like '004', '123'"""
        return (
            series.astype(str)
            .str.strip()
            .str.replace("^'", "", regex=True)
            .str.replace("'$", "", regex=True)
        )

    def _is_integer_pattern(self, series) -> bool:
        """Check if series contains integer-like values"""
        try:
            # Clean quoted values like "'004'" -> "004"
            clean_values = self._clean_quoted_values(series)
            numeric_values = pd.to_numeric(clean_values, errors="coerce")

            # If most convert to numbers and are whole numbers
            valid_numbers = numeric_values.dropna()
            if len(valid_numbers) / len(series) > 0.8:  # 80% are numeric
                return (valid_numbers % 1 == 0).all()  # All are whole numbers
        except:
            pass
        return False

    def _is_float_pattern(self, series) -> bool:
        """Check if series contains float-like values"""
        try:
            clean_values = self._clean_quoted_values(series)
            numeric_values = pd.to_numeric(clean_values, errors="coerce")

            # If most convert to numbers
            valid_numbers = numeric_values.dropna()
            return len(valid_numbers) / len(series) > 0.8
        except:
            pass
        return False

    def _is_likely_foreign_key(self, column_name: str) -> bool:
        """Check if this looks like a foreign key"""
        fk_patterns = ["area code", "item code", "element code", "area_id", "item_id"]
        return any(pattern in column_name.lower() for pattern in fk_patterns)


# Test usage
if __name__ == "__main__":
    analyzer = CSVAnalyzer()
    # Replace with actual path to test

    # Point to a specific ZIP file
    zip_file = Path(ZIP_PATH) / "Emissions_Land_Use_Fires_E_All_Data_(Normalized).zip"
    csv_filename = "Emissions_Land_Use_Fires_E_All_Data_(Normalized).csv"
    result = analyzer.analyze_csv_from_zip(zip_file, csv_filename)
    print(result)
