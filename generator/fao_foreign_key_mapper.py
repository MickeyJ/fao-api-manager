import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger


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

            if dataset["foreign_keys"]:
                logger.info(
                    f"  📊 {dataset_name}: {len(dataset['foreign_keys'])} FKs, excluding {len(dataset['exclude_columns'])} columns"
                )

        logger.info(f"✅ Enhanced {len(self.datasets)} datasets with FK information")
        return {"lookups": self.lookups, "datasets": self.datasets}

    def _process_dataset_foreign_keys(self, dataset: dict):
        """Process all foreign keys for a single dataset"""
        all_exclude_columns = set()
        column_renames = {}

        if "foreign_keys" not in dataset:
            dataset["foreign_keys"] = []

        for column in dataset["columns"]:

            for lookup_key, mapping in self.lookup_mappings.items():
                lookup_name = mapping["lookup_name"]

                if column in mapping["primary_key_variations"]:

                    lookup = self.lookups.get(lookup_name)
                    if not lookup:
                        logger.warning(f"  ⚠️ Lookup table {lookup_name} not found")
                        continue

                    if column != lookup["primary_key"]:
                        column_renames[column] = lookup["primary_key"]
                        logger.info(f"  📝 Will rename column: {column} → {lookup['primary_key']}")

                    # Find columns to exclude (non-PK lookup columns that exist in dataset)
                    for lookup_col in lookup["columns"]:
                        # Skip the PK column
                        if lookup_col == lookup["primary_key"]:
                            continue

                        # Find columns to exclude (description columns)
                        for desc_variation in mapping["description_variations"]:
                            if desc_variation in dataset["columns"]:
                                all_exclude_columns.add(desc_variation)

                        # Check if this lookup column exists in the dataset
                        if lookup_col in dataset["columns"]:
                            all_exclude_columns.add(lookup_col)

                    # Create FK relationship with clear names
                    fk = dict(
                        dataset_fk_csv_column=column,
                        dataset_fk_sql_column=format_column_name(lookup["primary_key"]),
                        lookup_sql_table=lookup_name,
                        lookup_sql_model=lookup["sql_model_name"],
                        lookup_pk_csv_column=lookup["primary_key"],  # Use lookup's actual PK
                        lookup_pk_sql_column=format_column_name(lookup["primary_key"]),  # Use lookup's actual PK
                    )

                    dataset["foreign_keys"].append(fk)
                    break  # Found match, no need to check other lookups

        # Update column_analysis with renamed columns
        if column_renames:
            for col_analysis in dataset["column_analysis"]:
                if col_analysis["csv_column_name"] in column_renames:
                    new_name = column_renames[col_analysis["csv_column_name"]]
                    col_analysis["original_csv_column_name"] = col_analysis["csv_column_name"]
                    col_analysis["csv_column_name"] = new_name
                    col_analysis["sql_column_name"] = format_column_name(new_name)

        # Update dataset with exclusions
        dataset["exclude_columns"] = sorted(list(all_exclude_columns))
        dataset["column_renames"] = column_renames  # Store for use in templates

        # Build SQL columns list excluding redundant columns
        sql_columns = [format_column_name(col) for col in dataset["columns"] if col not in all_exclude_columns]
        dataset["sql_all_columns"] = ", ".join(sql_columns)
