# generator/fao_analyzer.py
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger, to_snake_case, snake_to_pascal_case


@dataclass
class FAOLookup:
    """Represents a lookup table (areas, items, etc.)"""

    name: str  # "areas", "items"
    primary_key: str  # "Area Code", "Item Code"
    description_col: str  # "Area", "Item"
    sql_table_name: str  # snake_case sqlalchemy table name
    sql_model_name: str  # PascalCase sqlalchemy class name
    file_path: Path
    row_count: int = 0
    columns: List[str] = field(default_factory=list)


@dataclass
class FAODataset:
    """Represents a dataset (prices, production, etc.)"""

    name: str
    directory: Path
    sql_table_name: str  # snake_case sqlalchemy table name
    sql_model_name: str  # PascalCase sqlalchemy class name
    main_data_file: Path
    row_count: int = 0
    columns: List[str] = field(default_factory=list)


class FAOAnalyzer:
    """First pass - just discover what files we have"""

    def __init__(self, input_dir: str | Path, lookup_mappings: Dict):
        self.input_dir = Path(input_dir)
        self.synthetic_lookups_dir = self.input_dir / "synthetic_lookups"
        self.lookup_mappings = lookup_mappings
        self.results = {"lookups": {}, "datasets": {}}

    def save_discovery_results(self) -> Path:
        """Save discovery results to JSON"""

        output_path = Path("./analysis/fao_discovery.json")

        output_path.parent.mkdir(exist_ok=True)

        # Convert to JSON-serializable format
        json_data = {
            "lookups": {
                name: {
                    "name": lookup.name,
                    "primary_key": lookup.primary_key,
                    "description_col": lookup.description_col,
                    "sql_table_name": lookup.sql_table_name,
                    "sql_model_name": lookup.sql_model_name,
                    "file_path": str(lookup.file_path),
                    "row_count": lookup.row_count,
                    "columns": lookup.columns,
                }
                for name, lookup in self.results["lookups"].items()
            },
            "datasets": {
                name: {
                    "name": dataset.name,
                    "directory": str(dataset.directory),
                    "sql_table_name": dataset.sql_table_name,
                    "sql_model_name": dataset.sql_model_name,
                    "main_data_file": str(dataset.main_data_file) if dataset.main_data_file else None,
                    "row_count": dataset.row_count,
                    "columns": dataset.columns,
                }
                for name, dataset in self.results["datasets"].items()
            },
        }

        with open(output_path, "w") as f:
            json.dump(json_data, f, indent=2)

        logger.info(f"💾 Saved discovery results to {output_path}")
        return output_path

    def discover_all(self) -> None:
        """Discover all lookups and datasets"""
        logger.info("🔍 Starting FAO data discovery...")

        lookups = self._discover_lookups()
        datasets = self._discover_datasets()

        self.results = {"lookups": lookups, "datasets": datasets}  # Store in instance

    def _discover_lookups(self) -> Dict[str, FAOLookup]:
        """Discover all synthetic lookup files"""
        lookups = {}

        logger.info(f"📋 Discovering lookups in {self.synthetic_lookups_dir}")

        for lookup_key, mapping in self.lookup_mappings.items():
            lookup_name = mapping["lookup_name"]
            csv_path = self.synthetic_lookups_dir / f"{lookup_name}.csv"

            if csv_path.exists():
                # Just basic info for now
                columns = self._get_csv_columns(csv_path)
                row_count = self._get_csv_row_count(csv_path)

                lookup = FAOLookup(
                    name=lookup_name,
                    primary_key=mapping["output_columns"]["pk"],
                    description_col=mapping["output_columns"]["desc"],
                    sql_table_name=to_snake_case(lookup_name),  # Add this
                    sql_model_name=snake_to_pascal_case(lookup_name),  # Add this
                    file_path=csv_path,
                    row_count=row_count,
                    columns=columns,
                )
                lookups[lookup_name] = lookup
                logger.info(f"  ✓ Found {lookup_name}: {row_count} rows")
            else:
                logger.warning(f"  ⚠️ Missing {lookup_name}.csv")

        return lookups

    def _discover_datasets(self) -> Dict[str, FAODataset]:
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
                    dataset = self._analyze_dataset_dir(path)
                    if dataset:
                        datasets[dataset.name] = dataset
                        logger.info(f"  ✓ Found {dataset.name}: {dataset.row_count} rows")

        return datasets

    def _is_fao_dataset(self, path: Path) -> bool:
        """Check if directory looks like a FAO dataset"""
        # Look for All_Data CSV files
        return any(f.name for f in path.glob("*.csv") if "all_data" in f.name.lower())

    def _analyze_dataset_dir(self, path: Path) -> Optional[FAODataset]:
        """Basic analysis of a dataset directory"""
        # Find main data file
        main_file = None
        for f in path.glob("*.csv"):
            if "all_data" in f.name.lower() and "normalized" in f.name.lower():
                main_file = f
                break

        if not main_file:
            return None

        columns = self._get_csv_columns(main_file)
        row_count = self._get_csv_row_count(main_file)

        # Don't create dataset if we couldn't read the file
        if row_count == -1 or not columns:
            logger.error(f"  ✗ Failed to read {path.name}")
            return None

        return FAODataset(
            name=path.name,
            directory=path,
            sql_table_name=to_snake_case(path.name),
            sql_model_name=snake_to_pascal_case(path.name),
            main_data_file=main_file,
            row_count=row_count,
            columns=columns,
        )

    def _get_csv_columns(self, csv_path: Path) -> List[str]:
        """Get column names from CSV with proper encoding handling"""
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_path, nrows=0, encoding=encoding)
                return df.columns.str.strip().tolist()
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading {csv_path}: {e}")
                return []

        logger.error(f"Failed to read {csv_path} with any encoding")
        return []

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
