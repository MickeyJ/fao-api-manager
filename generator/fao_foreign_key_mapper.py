import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger, safe_index_name


def format_column_name(column_name: str) -> str:
    """Convert CSV column name to database-friendly name"""
    return column_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace(".", "_")


class FAOForeignKeyMapper:
    """Enhances datasets with foreign key relationships"""

    def __init__(self, structure_results: Dict, lookup_mappings: Dict, json_cache_path: Path, cache_bust: bool = False):
        self.lookups = structure_results["lookups"]
        self.datasets = structure_results["datasets"]
        self.lookup_mappings = lookup_mappings
        self.json_cache_path = json_cache_path
        self.cache_bust = cache_bust

    def enhance_datasets_with_foreign_keys(self) -> Dict[str, dict]:
        """Add FK relationships to existing dataset objects"""

        if self.json_cache_path.exists() and not self.cache_bust:
            logger.info(f"📁 FAOForeignKeyMapper - Using cached module structure from {self.json_cache_path}")
            return {"lookups": self.lookups, "datasets": self.datasets}

        logger.info("🔗 Mapping foreign key relationships...")

        for dataset_name, dataset in self.datasets.items():
            print(f"Processing dataset: {dataset_name}")
            self._process_dataset_foreign_keys(dataset)

            if dataset["model"]["foreign_keys"]:
                logger.info(
                    f"  📊 {dataset_name}: {len(dataset['model']['foreign_keys'])} FKs, excluding {len(dataset['model']['exclude_columns'])} columns"
                )

        logger.info(f"✅ Enhanced {len(self.datasets)} datasets with FK information")
        return {"lookups": self.lookups, "datasets": self.datasets}

    def _process_dataset_foreign_keys(self, dataset: dict):
        """Process all foreign keys for a single dataset"""
        all_exclude_columns = set()
        column_renames = {}

        # Initialize foreign_keys in model if not present
        if "foreign_keys" not in dataset["model"]:
            dataset["model"]["foreign_keys"] = []

        # Get columns from the model
        column_names = dataset["model"]["column_names"]
        column_analysis = dataset["model"]["column_analysis"]

        for column_name in column_names:
            for lookup_key, mapping in self.lookup_mappings.items():
                lookup_name = mapping["lookup_name"]

                if column_name in mapping["primary_key_variations"]:
                    lookup = self.lookups.get(lookup_name)
                    if not lookup:
                        logger.warning(f"  ⚠️ Lookup table {lookup_name} not found")
                        continue

                    # Get the lookup's PK from its model
                    lookup_pk = lookup["model"]["pk_column"]
                    lookup_pk_sql = lookup["model"]["pk_sql_column_name"]

                    # Check if column needs renaming
                    if column_name != lookup_pk:
                        column_renames[column_name] = lookup_pk
                        logger.info(f"  📝 Will rename column: {column_name} → {lookup_pk}")

                    # Find columns to exclude (non-PK lookup columns in dataset)
                    for lookup_col_analysis in lookup["model"]["column_analysis"]:
                        lookup_col = lookup_col_analysis["csv_column_name"]

                        # Check description variations
                        for desc_variation in mapping["description_variations"]:
                            if desc_variation in column_names:
                                all_exclude_columns.add(desc_variation)

                        # Check if this lookup column exists in dataset
                        if lookup_col in column_names:
                            all_exclude_columns.add(lookup_col)

                    # Create FK relationship in template-ready format
                    fk = {
                        "table_name": lookup_name,
                        "model_name": lookup["model"]["model_name"],
                        "sql_column_name": lookup_pk_sql,
                        "hash_fk_sql_column_name": f"{lookup_pk_sql}_id",
                        "hash_fk_csv_column_name": f"{column_name}_id",
                        "hash_pk_sql_column_name": "id",  # Assuming 'hash' is the standard PK in lookup tables
                        "csv_column_name": column_name,
                        "pipeline_name": lookup_name,
                        "index_hash": safe_index_name(f"{dataset['model']['table_name']}{lookup_name}", lookup_name),
                        # Additional info for ETL
                        "lookup_pk_csv_column": lookup_pk,
                        "hash_columns": lookup["model"]["hash_columns"],
                        "format_methods": mapping["format_methods"].get(column_name, []),
                        # NEW: Add basic lookup table info
                        "lookup_column_count": len(lookup["model"]["column_analysis"]),
                        "lookup_description_column": next(
                            (
                                col["sql_column_name"]
                                for col in lookup["model"]["column_analysis"]
                                if col["csv_column_name"] in mapping["description_variations"]
                            ),
                            None,
                        ),
                        "lookup_additional_columns": [
                            col["sql_column_name"]
                            for col in lookup["model"]["column_analysis"]
                            if col["csv_column_name"] in mapping.get("additional_columns", {}).keys()
                        ],
                    }

                    dataset["model"]["foreign_keys"].append(fk)
                    break  # Found match, no need to check other lookups

        # Update column_analysis with renamed columns
        if column_renames:
            for col_analysis in column_analysis:
                if col_analysis["csv_column_name"] in column_renames:
                    new_name = column_renames[col_analysis["csv_column_name"]]
                    col_analysis["original_csv_column_name"] = col_analysis["csv_column_name"]
                    col_analysis["csv_column_name"] = new_name
                    col_analysis["sql_column_name"] = format_column_name(new_name)

        # Update model with exclusions and renames
        dataset["model"]["exclude_columns"] = sorted(list(all_exclude_columns))
        dataset["model"]["column_renames"] = column_renames

        # Build indexes for foreign keys (already using correct format)
        for fk in dataset["model"]["foreign_keys"]:
            dataset["model"]["indexes"].append(
                {
                    "name": fk["index_hash"],  # Reuse the hash from FK
                    "columns": [f"{fk['sql_column_name']}_id"],  # The hash FK column
                    "unique": False,
                    "description": f"Index for FK to {fk['table_name']}",
                }
            )

        # Build SQL columns list excluding redundant columns
        sql_columns = []
        for col in column_analysis:
            if col["csv_column_name"] not in all_exclude_columns:
                sql_columns.append(col["sql_column_name"])

        dataset["model"]["sql_all_columns"] = ", ".join(sql_columns)
