# generator/fao_analyzer.py
import json
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger, to_snake_case, snake_to_pascal_case, format_column_name
from .structure import Structure
from .utils.value_type_checker import analyze_column


class FAOStructureModules:
    """First pass - just discover what files we have"""

    def __init__(self, input_dir: str | Path, lookup_mappings: Dict, json_cache_path: Path, cache_bust: bool = False):
        self.input_dir = Path(input_dir)
        self.json_cache_path = json_cache_path
        self.synthetic_lookups_dir = self.input_dir / "synthetic_lookups"
        self.lookup_mappings = lookup_mappings
        self.results = {"lookups": {}, "datasets": {}}
        self.structure = Structure()
        self.cache_bust = cache_bust

    def run(self) -> None:
        """Discover all lookups and datasets"""

        if self.json_cache_path.exists() and not self.cache_bust:
            logger.info(f"📁 Using cached module structure from {self.json_cache_path}")
            with open(self.json_cache_path, "r") as f:
                self.results = json.load(f)
            return

        logger.info("🔍 Starting FAO module structuring...")
        lookups = self._make_lookups()
        datasets = self._make_datasets()

        self.results = {"lookups": lookups, "datasets": datasets}  # Store in instance

    def save(self) -> Path:
        """Save discovery results to JSON"""

        # Convert Path objects to strings
        def convert_paths(obj):
            if isinstance(obj, dict):
                return {k: convert_paths(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_paths(item) for item in obj]
            elif isinstance(obj, Path):
                return str(obj)
            else:
                return obj

        # Save directly with path conversion
        with open(self.json_cache_path, "w") as f:
            json.dump(convert_paths(self.results), f, indent=2, default=str)

        logger.info(f"💾 Saved discovery results to {self.json_cache_path}")
        return self.json_cache_path

    def _make_lookups(self) -> Dict[str, dict]:
        """Discover all synthetic lookup files"""
        lookups = {}

        logger.info(f"📋 Discovering lookups in {self.synthetic_lookups_dir}")

        for lookup_key, mapping in self.lookup_mappings.items():
            lookup_name = mapping["lookup_name"]
            csv_path = self.synthetic_lookups_dir / f"{lookup_name}.csv"

            if csv_path.exists():
                csv_info = self._get_csv_info(csv_path)

                columns = csv_info["columns"]
                row_count = csv_info["row_count"]
                sample_rows = csv_info["sample_rows"]

                # Analyze columns using sample rows
                column_analysis = []
                for col in columns:

                    col_spec = analyze_column(sample_rows=sample_rows, column_name=col)

                    # col_spec = {
                    #     "csv_column_name": col,
                    #     "sql_column_name": format_column_name(col),
                    #     "sample_values": sample_values[:5],  # Keep just a few for reference
                    #     "null_count": null_count,
                    #     "non_null_count": non_null_count,
                    #     "unique_count": unique_count,
                    #     "inferred_sql_type": inferred_type,
                    # }
                    column_analysis.append(col_spec)

                lookup = dict(
                    name=lookup_name,
                    module_name=lookup_name,
                    primary_key=mapping["output_columns"]["pk"],
                    description_col=mapping["output_columns"]["desc"],
                    sql_table_name=to_snake_case(lookup_name),
                    sql_model_name=snake_to_pascal_case(lookup_name),
                    file_path=csv_path,
                    row_count=row_count,
                    columns=columns,
                    column_analysis=column_analysis,  # Add this!
                )
                lookups[lookup_name] = lookup
                logger.info(f"  ✓ Found {lookup_name}: {row_count} rows")
            else:
                logger.warning(f"  ⚠️ Missing {lookup_name}.csv")

        return lookups

    def _infer_sql_type(self, sample_values: List) -> str:
        """Infer SQL type from sample values"""
        if not sample_values:
            return "String"

        # Clean sample values
        clean_values = []
        for val in sample_values:
            if val is not None and str(val).strip() != "":
                clean_values.append(str(val).strip().replace("'", ""))

        if not clean_values:
            return "String"

        # Check if all values are integers
        try:
            all([int(v) for v in clean_values])
            return "Integer"
        except ValueError:
            pass

        # Check if all values are floats
        try:
            all([float(v) for v in clean_values])
            return "Float"
        except ValueError:
            pass

        # Default to String
        return "String"

    def _make_datasets(self) -> Dict[str, dict]:
        """Discover all dataset directories"""
        datasets = {}

        logger.info(f"📊 Discovering datasets in {self.input_dir}")

        for path in self.input_dir.iterdir():
            if path.is_dir() and not path.name.startswith("."):
                # Skip synthetic_lookups
                if path.name == "synthetic_lookups":
                    continue

                # Check if it looks like a FAO dataset
                if self._is_fao_dataset(path):
                    dataset = self._extract_dataset_info(path)
                    if dataset:
                        datasets[dataset["name"]] = dataset
                        logger.info(f"  ✓ Found {dataset['name']}: {dataset['row_count']} rows")

        return datasets

    def _is_fao_dataset(self, path: Path) -> bool:
        """Check if directory looks like a FAO dataset"""
        # Look for All_Data CSV files
        return any(f.name for f in path.glob("*.csv") if "all_data" in f.name.lower())

    def _extract_dataset_info(self, path: Path) -> Optional[dict]:
        """Basic analysis of a dataset directory"""
        # Find main data file
        main_file = None
        for f in path.glob("*.csv"):
            if "all_data" in f.name.lower() and "normalized" in f.name.lower():
                main_file = f
                break

        if not main_file:
            return None

        csv_info = self._get_csv_info(main_file)

        columns = csv_info["columns"]
        row_count = csv_info["row_count"]
        sample_rows = csv_info["sample_rows"]

        # Analyze columns using sample rows
        column_analysis = []
        for col in columns:
            col_spec = analyze_column(sample_rows=sample_rows, column_name=col)

            # col_spec = {
            #     "csv_column_name": col,
            #     "sql_column_name": format_column_name(col),
            #     "sample_values": sample_values[:5],  # Keep just a few for reference
            #     "null_count": null_count,
            #     "non_null_count": non_null_count,
            #     "unique_count": unique_count,
            #     "inferred_sql_type": inferred_type,
            # }
            column_analysis.append(col_spec)

        # Don't create dataset if we couldn't read the file
        if row_count == -1 or not csv_info["columns"]:
            logger.error(f"  ✗ Failed to read {path.name}")
            return None

        dataset_name = self.structure.extract_module_name(path.name)

        return dict(
            name=dataset_name,
            directory=path,
            sql_table_name=dataset_name,
            sql_model_name=snake_to_pascal_case(dataset_name),
            main_data_file=main_file,
            row_count=row_count,
            columns=columns,
            sample_rows=sample_rows,
            column_analysis=column_analysis,
        )

    def _get_csv_info(self, csv_path: Path) -> dict:
        """Get CSV info including columns, row count, and first 50 rows"""
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                # Read first 50 rows to get columns and sample data
                df_sample = pd.read_csv(csv_path, nrows=50, encoding=encoding)

                # Get total row count efficiently
                # Option 1: Quick count by reading the rest of the file
                with open(csv_path, "r", encoding=encoding) as f:
                    # Skip header line we already read
                    next(f)
                    # Start at 50 since we already read those
                    row_count = 50 + sum(1 for _ in f)

                # If file has less than 50 rows, adjust count
                if len(df_sample) < 50:
                    row_count = len(df_sample)

                return dict(
                    row_count=row_count,
                    columns=df_sample.columns.str.strip().tolist(),
                    sample_rows=df_sample.to_dict("records"),
                )

            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading {csv_path} with {encoding}: {e}")
                continue

        logger.error(f"Failed to read {csv_path} with any encoding")
        return dict(row_count=0, columns=[], rows=[])

    def _get_csv_row_count(self, csv_path: Path) -> int:
        """Get row count from CSV with proper encoding handling"""
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                with open(csv_path, "r", encoding=encoding) as f:
                    return sum(1 for _ in f) - 1  # Minus header
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error counting rows in {csv_path}: {e}")
                return -1  # Signal error with -1

        logger.error(f"Failed to count rows in {csv_path} with any encoding")
        return -1
