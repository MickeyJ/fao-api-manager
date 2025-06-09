# generator/fao_analyzer.py
import json
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger, to_snake_case, snake_to_pascal_case, format_column_name, safe_index_name
from .structure import Structure
from .value_type_checker import analyze_column


class FAOStructureModules:
    """First pass - just discover what files we have"""

    def __init__(
        self, input_dir: str | Path, reference_mappings: Dict, json_cache_path: Path, cache_bust: bool = False
    ):
        self.input_dir = Path(input_dir)
        self.json_cache_path = json_cache_path
        self.synthetic_references_dir = self.input_dir / "synthetic_references"
        self.reference_mappings = reference_mappings
        self.results = {"references": {}, "datasets": {}}
        self.structure = Structure()
        self.cache_bust = cache_bust

    def run(self) -> None:
        """Discover all references and datasets"""

        if self.json_cache_path.exists() and not self.cache_bust:
            logger.info(f"📁 Using cached module structure from {self.json_cache_path}")
            with open(self.json_cache_path, "r") as f:
                self.results = json.load(f)
            return

        logger.info("🔍 Starting FAO module structuring...")
        references = self._make_references()
        datasets = self._make_datasets()

        self.results = {"references": references, "datasets": datasets}  # Store in instance

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

    def _make_references(self) -> Dict[str, dict]:
        """Discover all synthetic reference files"""
        references = {}
        logger.info(f"📊 Creating Lookup Data Pipeline Structures")

        for reference_key, mapping in self.reference_mappings.items():
            reference_name = mapping["reference_name"]
            csv_path = self.synthetic_references_dir / f"{reference_name}.csv"

            if csv_path.exists():
                csv_info = self._get_csv_info(csv_path)

                column_names = csv_info["columns"]
                row_count = csv_info["row_count"]
                sample_rows = csv_info["sample_rows"]

                # Analyze columns using sample rows
                column_analysis = []
                for column_name in column_names:
                    col_spec = analyze_column(sample_rows=sample_rows, column_name=column_name)
                    csv_column_name = col_spec["csv_column_name"]
                    col_spec["format_methods"] = mapping["format_methods"].get(csv_column_name, [])
                    # Mark if this is the original PK column
                    if csv_column_name == mapping["output_columns"]["pk"]:
                        col_spec["indexed"] = True
                        col_spec["original_pk"] = True

                    column_analysis.append(col_spec)

                # Build reference structure
                reference = {
                    "name": reference_name,
                    "is_reference_module": True,
                    "file_info": {
                        "csv_file": str(csv_path.relative_to(self.input_dir).as_posix()),
                        "csv_filename": csv_path.name,
                        "zip_path": None,
                        "row_count": row_count,
                    },
                    "model": {
                        "table_name": to_snake_case(reference_name),
                        "model_name": snake_to_pascal_case(reference_name),
                        "pk_column": mapping["output_columns"]["pk"],
                        "pk_sql_column_name": format_column_name(mapping["output_columns"]["pk"]),
                        "hash_columns": mapping.get("hash_columns", []),
                        "column_names": column_names,
                        "column_analysis": column_analysis,
                        "format_methods": mapping["format_methods"],
                        "indexes": [],  # Initialize empty
                    },
                    "metadata": {
                        "primary_key_variations": mapping["primary_key_variations"],
                        "description_variations": mapping["description_variations"],
                        "additional_columns": mapping.get("additional_columns", {}),
                    },
                }

                # Build composite unique index on original PK + source_dataset
                original_pk_col = format_column_name(mapping["output_columns"]["pk"])
                index_name = safe_index_name(reference["model"]["table_name"], f"{original_pk_col}_src")

                reference["model"]["indexes"].append(
                    {
                        "name": index_name,
                        "columns": [original_pk_col, "source_dataset"],
                        "unique": True,
                        "description": "Ensures unique reference values per source dataset",
                    }
                )

                references[reference_name] = reference
                logger.info(f"  🛢 {reference_name}: {row_count} rows")
            else:
                logger.warning(f"  ⚠️ Missing {reference_name}.csv")

        return references

    # In FAOStructureModules, when processing columns
    def _find_reference_mapping_for_column(self, column_name: str):
        """Find the reference mapping that contains this column as a primary key variation"""
        for reference_key, mapping in self.reference_mappings.items():
            if column_name in mapping["primary_key_variations"]:
                return mapping
        return {}

    def _is_fao_dataset(self, path: Path) -> bool:
        """Check if directory looks like a FAO dataset"""
        # Look for All_Data CSV files
        return any(f.name for f in path.glob("*.csv") if "all_data" in f.name.lower())

    def _make_datasets(self) -> Dict[str, dict]:
        """Discover all dataset directories"""
        datasets = {}

        logger.info(f"📊 Creating Dataset Pipeline Structures")

        for path in self.input_dir.iterdir():
            if path.is_dir() and not path.name.startswith("."):
                # Skip synthetic_references
                if path.name == "synthetic_references":
                    continue

                # Check if it looks like a FAO dataset
                if self._is_fao_dataset(path):
                    dataset = self._extract_dataset_info(path)
                    if dataset:
                        datasets[dataset["name"]] = dataset

        return datasets

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

        column_names = csv_info["columns"]
        row_count = csv_info["row_count"]
        sample_rows = csv_info["sample_rows"]

        # Analyze columns using sample rows
        column_analysis = []
        for column_name in column_names:
            col_spec = analyze_column(sample_rows=sample_rows, column_name=column_name)
            mapping = self._find_reference_mapping_for_column(col_spec["csv_column_name"])
            if mapping and "format_methods" in mapping:
                format_methods = mapping.get("format_methods", {})
                if col_spec["csv_column_name"] in format_methods:
                    col_spec["format_methods"] = format_methods[col_spec["csv_column_name"]]

            column_analysis.append(col_spec)

        # Don't create dataset if we couldn't read the file
        if row_count == -1 or not column_names:
            logger.error(f"  🩸 Failed to read {path.name}")
            return None

        dataset_name = self.structure.extract_module_name(path.name)

        logger.info(f"  🛢 {dataset_name}: {row_count} rows")

        return {
            "name": dataset_name,
            "is_reference_module": False,
            "file_info": {
                "csv_file": str(main_file.relative_to(self.input_dir).as_posix()),
                "csv_filename": main_file.name,
                "directory": str(path.relative_to(self.input_dir)),
                "row_count": row_count,
            },
            "model": {
                "table_name": dataset_name,
                "model_name": snake_to_pascal_case(dataset_name),
                "column_names": column_names,
                "column_analysis": column_analysis,  # Raw column names, will be enhanced by FK mapper
                "foreign_keys": [],  # Will be populated by FAOForeignKeyMapper
                "exclude_columns": [],  # Will be populated by FAOForeignKeyMapper
                "indexes": [],  # Will be built based on foreign keys
            },
            "metadata": {
                "directory_path": str(path),
            },
        }

    def _get_csv_info(self, csv_path: Path) -> dict:
        """Get CSV info including columns, row count, and first 50 rows"""
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                # Read first 1000 rows to get good variety of sample values
                df_sample = pd.read_csv(csv_path, nrows=1000, encoding=encoding)

                # Get total row count efficiently
                # Option 1: Quick count by reading the rest of the file
                with open(csv_path, "r", encoding=encoding) as f:
                    # Skip header line we already read
                    next(f)
                    # Start at 1000 since we already read those
                    row_count = 1000 + sum(1 for _ in f)

                # If file has less than 1000 rows, adjust count
                if len(df_sample) < 1000:
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
