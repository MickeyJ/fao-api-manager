import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
from . import logger


class FAOConflictDetector:
    """Detects conflicts in lookup tables and tracks dataset references"""

    def __init__(self, structure_results: Dict):
        self.lookups = structure_results["lookups"]
        self.datasets = structure_results["datasets"]
        self.synthetic_pk_counter = 900_000

    def enhance_with_conflicts(self) -> Dict:
        """Main method - detect conflicts and update all structures"""
        logger.info("🔍 Starting conflict detection...")

        total_conflicts = 0
        total_referenced = 0

        # Step 1: Find conflicts in each lookup
        for lookup_name, lookup_spec in self.lookups.items():
            conflicts = self._find_lookup_conflicts(lookup_spec)

            if conflicts:
                # Update lookup structure with conflict info
                lookup_spec["conflicts"] = conflicts
                lookup_spec["has_conflicts"] = True
                lookup_spec["conflict_count"] = len(conflicts)

                total_conflicts += len(conflicts)
                logger.info(f"  ⚠️  {lookup_name}: Found {len(conflicts)} conflicts")
            else:
                lookup_spec["conflicts"] = []
                lookup_spec["has_conflicts"] = False
                lookup_spec["conflict_count"] = 0
                logger.info(f"  ✅ {lookup_name}: No conflicts")

        # Step 2: Check which datasets reference these conflicts
        dataset_references = self._check_dataset_references()
        total_referenced = sum(len(refs) for refs in dataset_references.values())

        logger.info(
            f"✅ Conflict detection complete: {total_conflicts} conflicts found, "
            f"{total_referenced} referenced by datasets"
        )

        return {"lookups": self.lookups, "datasets": self.datasets}

    def _find_lookup_conflicts(self, lookup_spec: Dict) -> List[Dict]:
        """Find all duplicate PKs in a lookup table"""
        file_path = Path(lookup_spec["file_path"])
        logger.info(f"📂 Checking lookup '{lookup_spec['name']}' at {file_path}")

        # Read CSV with error handling
        df = self._read_csv_safely(file_path)
        if df is None:
            return []

        pk_col = lookup_spec["primary_key"]
        desc_col = lookup_spec["description_col"]

        # Find duplicates
        conflicts = []
        duplicated = df[df.duplicated(pk_col, keep=False)]

        if duplicated.empty:
            return conflicts

        # Group duplicates by PK value
        for pk_value, group in duplicated.groupby(pk_col):
            conflict = {
                "pk_value": str(pk_value),
                "pk_column": pk_col,
                "desc_column": desc_col,
                "variation_count": len(group),
                "variations": [],
            }

            # Create variations with synthetic PKs
            for idx, (_, row) in enumerate(group.iterrows()):
                variation = {
                    "index": idx,
                    "new_primary_key": str(pk_value) if idx == 0 else str(self.synthetic_pk_counter),
                    desc_col: str(row[desc_col]),
                    "row_data": row.to_dict(),
                }

                if idx > 0:
                    self.synthetic_pk_counter += 1

                conflict["variations"].append(variation)

            conflicts.append(conflict)

        return conflicts

    def _check_dataset_references(self) -> Dict[str, List[Dict]]:
        """Check which datasets reference conflicting PKs - OPTIMIZED"""
        dataset_references = defaultdict(list)

        # Pre-collect all conflicting PKs by lookup
        conflicts_by_lookup = {}
        for lookup_name, lookup_spec in self.lookups.items():
            if lookup_spec.get("has_conflicts"):
                conflicts_by_lookup[lookup_spec["sql_table_name"]] = {
                    "conflicts": lookup_spec["conflicts"],
                    "pk_values": [c["pk_value"] for c in lookup_spec["conflicts"]],
                    "pk_to_conflict": {c["pk_value"]: c for c in lookup_spec["conflicts"]},
                }

        # Process each dataset
        for dataset_name, dataset_spec in self.datasets.items():
            if not dataset_spec.get("foreign_keys"):
                continue

            logger.info(f"🥓 Conflict processing - '{dataset_name}'")
            logger.info(f"     {len(conflicts_by_lookup)} potential lookups with conflicts")

            # Collect columns we need to read
            columns_needed = set()
            fk_mappings = []

            for fk in dataset_spec["foreign_keys"]:
                lookup_table = fk["lookup_sql_table"]
                if lookup_table in conflicts_by_lookup:
                    columns_needed.add(fk["dataset_fk_csv_column"])
                    # Also add description column if it exists
                    desc_col = next(
                        (col for col in dataset_spec["columns"] if col in ["Area", "Item", "Element", "Flag"]), None
                    )
                    if desc_col:
                        columns_needed.add(desc_col)
                    fk_mappings.append((fk, lookup_table))

            if not columns_needed:
                continue

            df = None
            encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

            logger.info(f"    ⌚ Reading dataset csv")
            for encoding in encodings:
                try:
                    df = pd.read_csv(
                        dataset_spec["main_data_file"], usecols=list(columns_needed), dtype=str, encoding=encoding
                    ).fillna("")
                    break  # Success!
                except UnicodeDecodeError:
                    continue  # Try next encoding
                except ValueError as e:
                    # Column not found - try without usecols
                    try:
                        df = pd.read_csv(dataset_spec["main_data_file"], dtype=str, encoding=encoding).fillna("")
                        # Filter to only columns that exist
                        columns_to_use = [col for col in columns_needed if col in df.columns]
                        df = df[columns_to_use]
                        break  # Success!
                    except UnicodeDecodeError:
                        continue  # Try next encoding
                    except Exception as e2:
                        if encoding == encodings[-1]:  # Last encoding attempt
                            logger.error(f"  ❌ Could not read {dataset_name}: {e2}")
                        continue

            if df is None:
                logger.warning(f"  ⚠️  Could not read dataset: {dataset_name}")
                continue

            # Check all FKs at once
            conflicts_referenced = []
            logger.info(f"    🔍 Checking {len(fk_mappings)} fks for conflicts")
            for fk, lookup_table in fk_mappings:
                conflict_info = conflicts_by_lookup[lookup_table]
                fk_column = fk["dataset_fk_csv_column"]

                # Check all conflicting PKs at once using isin()
                filtered_df = df[df[fk_column].isin(conflict_info["pk_values"])]

                if len(filtered_df) > 0:
                    used_pks = filtered_df[fk_column].unique()

                    for pk_value in used_pks:
                        conflict = conflict_info["pk_to_conflict"][pk_value]
                        conflicts_referenced.append(
                            {
                                "pk_value": pk_value,
                                "lookup_name": lookup_table,
                                "matched_variation": None,  # Could enhance this if needed
                                "conflict": conflict,
                            }
                        )

            if conflicts_referenced:
                # Add to dataset
                if "conflict_resolutions" not in dataset_spec:
                    dataset_spec["conflict_resolutions"] = {}

                # Group by lookup
                for lookup_table in set(cr["lookup_name"] for cr in conflicts_referenced):
                    lookup_conflicts = [cr for cr in conflicts_referenced if cr["lookup_name"] == lookup_table]

                    dataset_spec["conflict_resolutions"][lookup_table] = {
                        "fk_column": next(fk["dataset_fk_csv_column"] for fk, lt in fk_mappings if lt == lookup_table),
                        "conflicts": lookup_conflicts,
                    }
                logger.info(f"    📍 Found {len(conflicts_referenced)} conflicts")
                dataset_references[dataset_name].extend(conflicts_referenced)

        return dataset_references

    def _find_lookup_by_table_name(self, table_name: str) -> Optional[Dict]:
        """Find lookup spec by SQL table name"""
        for lookup_name, lookup_spec in self.lookups.items():
            if lookup_spec.get("sql_table_name") == table_name:
                return lookup_spec
        return None

    def _read_csv_safely(self, file_path: str | Path) -> Optional[pd.DataFrame]:
        """Read CSV with multiple encoding attempts"""
        file_path = Path(file_path)
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                df = df.fillna("")  # Replace NaN with empty strings
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                if encoding == encodings[-1]:  # Only log on last attempt
                    logger.error(f"Failed to read {file_path}: {e}")

        return None

    def get_summary(self) -> Dict:
        """Get a summary of all conflicts"""
        summary = {
            "total_lookups": len(self.lookups),
            "lookups_with_conflicts": sum(1 for l in self.lookups.values() if l.get("has_conflicts")),
            "total_conflicts": sum(l.get("conflict_count", 0) for l in self.lookups.values()),
            "datasets_with_conflicts": sum(1 for d in self.datasets.values() if d.get("conflict_resolutions")),
            "conflicts_by_lookup": {},
        }

        # Add per-lookup summary
        for lookup_name, lookup_spec in self.lookups.items():
            if lookup_spec.get("has_conflicts"):
                summary["conflicts_by_lookup"][lookup_name] = {
                    "conflict_count": lookup_spec["conflict_count"],
                    "sample_conflicts": [],
                }

                # Show first 3 conflicts as examples
                for conflict in lookup_spec["conflicts"][:3]:
                    summary["conflicts_by_lookup"][lookup_name]["sample_conflicts"].append(
                        {
                            "pk_value": conflict["pk_value"],
                            "variations": [
                                {
                                    "new_primary_key": v["new_primary_key"],
                                    conflict["desc_column"]: v[conflict["desc_column"]],
                                }
                                for v in conflict["variations"]
                            ],
                        }
                    )

        return summary

    def show_dataset_conflicts(self, dataset_name: str):
        """Show conflicts for a specific dataset"""
        dataset = self.datasets.get(dataset_name)
        if not dataset:
            logger.error(f"Dataset '{dataset_name}' not found")
            return

        if not dataset.get("conflict_resolutions"):
            logger.info(f"Dataset '{dataset_name}' has no conflicts")
            return

        logger.info(f"\n📊 Conflicts in dataset '{dataset_name}':")

        for lookup_name, resolution in dataset["conflict_resolutions"].items():
            logger.info(f"\n  From lookup '{lookup_name}' (column: {resolution['fk_column']}):")

            for conflict_ref in resolution["conflicts"]:
                pk_value = conflict_ref["pk_value"]
                conflict = conflict_ref["conflict"]

                logger.info(f"    PK {pk_value} has {len(conflict['variations'])} variations:")

                for v in conflict["variations"]:
                    marker = "→" if v["index"] == conflict_ref.get("matched_variation") else " "
                    logger.info(f"      {marker} [{v['new_primary_key']}] {v[conflict['desc_column']]}")
