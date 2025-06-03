"""CSV file analyzer for FAO datasets, handling ZIP files and caching results"""

import zipfile
from pathlib import Path
from typing import Dict
from io import StringIO
import pandas as pd
from .structure import Structure
from .scanner import Scanner
from .file_generator import FileGenerator
from .csv_cache import CSVCache
from . import logger


class CSVAnalyzer:
    """Analyze CSV files from FAO datasets, handling ZIP files and caching results"""

    def __init__(
        self,
        structure: Structure,
        scanner: Scanner,
        file_generator: FileGenerator,
        shared_cache: CSVCache | None = None,
    ):
        self.structure = structure
        self.scanner = scanner
        self.file_generator = file_generator
        # Use shared cache or create new one
        self.cache = shared_cache or CSVCache()

    def analyze_csv_from_zip(self, zip_path: Path, csv_filename: str) -> Dict:
        """Analyze a CSV file directly from inside a ZIP with caching"""
        return self.cache.get_analysis(zip_path, csv_filename, self._analyze_csv_from_zip_uncached)[
            0
        ]  # Just return the analysis, not the cache key

    def _analyze_csv_from_zip_uncached(self, zip_path: Path, csv_filename: str) -> Dict:
        """The actual analysis logic (without caching)"""
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
                return self._analyze_dataframe(df, csv_filename, "utf-8 (with errors ignored)")

    def _analyze_dataframe(self, df: pd.DataFrame, csv_filename: str, encoding: str) -> Dict:
        """Analyze a DataFrame and return structured information"""
        # Clean column names
        df.columns = df.columns.str.strip()
        table_name = self.structure.extract_module_name(csv_filename)

        # Analyze each column for type inference
        column_analysis = []
        for col in df.columns:
            col_info = self._analyze_column(df[col].head(1000), col, table_name)
            column_analysis.append(col_info)

        # Sample rows
        sample_rows = df.head(10).to_dict("records")

        return {
            "file_name": csv_filename,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": df.columns.tolist(),
            "sql_column_names": [self._format_column_name(col) for col in df.columns],
            "encoding_used": encoding,
            "sample_rows": sample_rows,
            "column_analysis": column_analysis,
        }

    def analyze_files(self) -> Dict[str, Dict]:
        """Scan all ZIP files and identify duplicate files across datasets"""
        logger.info("🔍 Starting comprehensive file analysis...")
        all_files = {}

        for zip_path in self.scanner.zip_dir.glob("*.zip"):
            if self.scanner.is_zip(zip_path):
                with zipfile.ZipFile(zip_path, "r") as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith(".csv")]

                for csv_filename in csv_files:
                    normalized_name = self.structure.extract_module_name(csv_filename)

                    if normalized_name not in all_files:
                        all_files[normalized_name] = {
                            "normalized_name": normalized_name,
                            "csv_filename": csv_filename,
                            "occurrence_count": 0,
                            "occurrences": [],
                        }

                    all_files[normalized_name]["occurrence_count"] += 1

                    logger.info(f"📊 Analyzing {normalized_name} in {zip_path.name}")

                    # Use cached analysis
                    csv_analysis = self.analyze_csv_from_zip(zip_path, csv_filename)

                    occurrence = {
                        "csv_filename": csv_filename,
                        "row_count": csv_analysis["row_count"],
                        "column_count": csv_analysis["column_count"],
                        "columns": csv_analysis["columns"],
                        "sample_rows": csv_analysis["sample_rows"],
                    }

                    all_files[normalized_name]["occurrences"].append(occurrence)

        # Save results
        self.file_generator.write_json_file(
            self.file_generator.output_dir / "all_csv_file_analysis.json",
            all_files,
        )

        # Log cache stats
        cache_stats = self.cache.get_cache_stats()
        logger.info(f"💾 Analysis complete. Cache contains {cache_stats['total_analyses']} analyses")

        return all_files

    def _analyze_column(self, series, column_name: str, table_name: str) -> Dict:
        """Analyze a single column to infer type and properties"""

        clean_series = self._clean_quoted_values(series)

        sample_values = clean_series.dropna().head(4).tolist()
        non_null_count = int(clean_series.count())  # Convert to Python int
        total_count = len(clean_series)
        null_count = total_count - non_null_count
        unique_count = int(clean_series.nunique())  # Convert to Python int

        # Infer SQL type
        inferred_type = self._infer_sql_type(column_name, clean_series)
        sql_column_name = self._format_column_name(column_name)

        return {
            "column_name": column_name,
            "csv_column_name": column_name.replace(".1", ""),
            "sql_column_name": sql_column_name,
            "sql_table_name": table_name,
            "sample_values": sample_values,
            "null_count": null_count,
            "non_null_count": non_null_count,
            "unique_count": unique_count,
            "inferred_sql_type": inferred_type,
            "is_likely_foreign_key": self._is_code_column(column_name),
        }

    def _format_column_name(self, column_name: str) -> str:
        """Convert CSV column name to database-friendly name"""
        return (
            column_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace(".1", "")
        )

    def _infer_sql_type(self, column_name: str, series) -> str:
        """Infer SQLAlchemy column type from data patterns"""
        # Drop nulls for analysis
        clean_series = series.dropna()

        if len(clean_series) == 0:
            return "String"

        # Check for specific FAO patterns first
        if self._is_year_column(column_name, clean_series):
            return "Integer"

        if self._is_code_column(column_name):
            return "Integer"  # Most FAO codes are integers

        if self._is_value_column(column_name):
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

    def _is_code_column(self, column_name: str) -> bool:
        """Check if this looks like an area/item/element code"""

        if "code" in column_name.lower():
            return True
        return False

    def _is_value_column(self, column_name: str) -> bool:
        """Check if this looks like a numeric value column"""
        value_patterns = ["value", "price", "amount", "quantity", "rate"]
        return any(pattern in column_name.lower() for pattern in value_patterns)

    def _clean_quoted_values(self, series):
        """Remove quotes from values like '004', '123'"""
        return series.astype(str).str.strip().str.replace("^'", "", regex=True).str.replace("'$", "", regex=True)

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

    def _could_be_fk(self, column_name: str, series) -> bool:
        """Check if this column could be a foreign key based on patterns"""
        # Check if it matches any known foreign key patterns
        if len(series) < 10 and any(char.isdigit() for char in series):
            return True
        return False
