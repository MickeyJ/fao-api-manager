from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict
from .logger import logger
from . import safe_index_name


def format_column_name(column_name: str) -> str:
    """Convert CSV column name to database-friendly name"""
    return column_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace(".", "_")


class FAOForeignKeyMapper:
    """Enhances datasets with foreign key relationships"""

    def __init__(
        self, structure_results: Dict, reference_mappings: Dict, json_cache_path: Path, cache_bust: bool = False
    ):
        self.references = structure_results["references"]
        self.datasets = structure_results["datasets"]
        self.reference_mappings = reference_mappings
        self.json_cache_path = json_cache_path
        self.cache_bust = cache_bust

    def enhance_datasets_with_foreign_keys(self) -> Dict[str, dict]:
        """Add FK relationships to existing dataset objects"""

        if self.json_cache_path.exists() and not self.cache_bust:
            logger.info(f"📁 FAOForeignKeyMapper - Using cached module structure from {self.json_cache_path}")
            return {"references": self.references, "datasets": self.datasets}

        logger.info("🔗 Mapping foreign key relationships...")

        for dataset_name, dataset in self.datasets.items():
            print(f"Processing dataset: {dataset_name}")
            self._process_dataset_foreign_keys(dataset)

            if dataset["model"]["foreign_keys"]:
                logger.info(
                    f"  📊 {dataset_name}: {len(dataset['model']['foreign_keys'])} FKs, excluding {len(dataset['model']['exclude_columns'])} columns"
                )

        logger.info(f"✅ Enhanced {len(self.datasets)} datasets with FK information")
        return {"references": self.references, "datasets": self.datasets}

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
            for reference_key, mapping in self.reference_mappings.items():
                reference_name = mapping["reference_name"]

                if column_name in mapping["primary_key_variations"]:
                    reference = self.references.get(reference_name)
                    if not reference:
                        logger.warning(f"  ⚠️ Lookup table {reference_name} not found")
                        continue

                    # Get the reference's PK from its model
                    reference_pk = reference["model"]["pk_column"]
                    reference_pk_sql = reference["model"]["pk_sql_column_name"]

                    # Check if column needs renaming
                    if column_name != reference_pk:
                        column_renames[column_name] = reference_pk
                        logger.info(f"  📝 Will rename column: {column_name} → {reference_pk}")
                        all_exclude_columns.add(reference_pk)

                    # Find columns to exclude (non-PK reference columns in dataset)
                    for reference_col_analysis in reference["model"]["column_analysis"]:
                        reference_col = reference_col_analysis["csv_column_name"]

                        # Check description variations
                        for desc_variation in mapping["description_variations"]:
                            if desc_variation in column_names:
                                all_exclude_columns.add(desc_variation)

                        # Check if this reference column exists in dataset
                        if reference_col in column_names:
                            all_exclude_columns.add(reference_col)

                    # Create FK relationship in template-ready format
                    fk = {
                        "table_name": reference_name,
                        "model_name": reference["model"]["model_name"],
                        "sql_column_name": reference_pk_sql,
                        "hash_fk_sql_column_name": f"{reference_pk_sql}_id",
                        "hash_fk_csv_column_name": f"{column_name}_id",
                        "hash_pk_sql_column_name": "id",  # Assuming 'hash' is the standard PK in reference tables
                        "csv_column_name": column_name,
                        "pipeline_name": reference_name,
                        "index_hash": safe_index_name(
                            f"{dataset['model']['table_name']}{reference_name}", reference_name
                        ),
                        # Additional info for ETL
                        "reference_pk_csv_column": reference_pk,
                        "hash_columns": reference["model"]["hash_columns"],
                        "format_methods": mapping["format_methods"].get(column_name, []),
                        "validation_func": mapping.get("validation_func"),
                        "exception_func": mapping.get("exception_func"),
                        # NEW: Add basic reference table info
                        "reference_column_count": len(reference["model"]["column_analysis"]),
                        "reference_description_column": next(
                            (
                                col["sql_column_name"]
                                for col in reference["model"]["column_analysis"]
                                if col["csv_column_name"] in mapping["description_variations"]
                            ),
                            None,
                        ),
                        "reference_additional_columns": [
                            col["sql_column_name"]
                            for col in reference["model"]["column_analysis"]
                            if col["csv_column_name"] in mapping.get("additional_columns", {}).keys()
                        ],
                    }

                    dataset["model"]["foreign_keys"].append(fk)
                    break  # Found match, no need to check other references

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
