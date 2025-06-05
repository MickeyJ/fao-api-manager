from . import logger, to_snake_case, safe_index_name, snake_to_pascal_case
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict

from .csv_cache import CSVCache
from generator.scanner import Scanner
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.csv_analyzer import CSVAnalyzer


class PipelineSpecs:
    def __init__(self, zip_path: str, shared_cache: CSVCache | None = None):
        self.json_path = Path("analysis/pipeline_spec.json")
        self.structure = Structure()
        self.scanner = Scanner(zip_path, self.structure)
        self.file_generator = FileGenerator("./analysis")
        self.csv_analyzer = CSVAnalyzer(self.structure, self.scanner, self.file_generator)
        self.cache = shared_cache or CSVCache()

    def create(self) -> Dict[str, Any]:
        if self.json_path.exists():
            with open(self.json_path, "r") as f:
                return json.load(f)
        else:
            return self.discover_file_patterns()

    def discover_file_patterns(self) -> Dict[str, Any]:
        """Simplified pattern discovery focused on core files"""
        all_zip_info = self.scanner.scan_all_zips()

        dataset_file_info = {}
        core_file_info = {}

        # First pass: collect all file info and dataset column signatures
        print("=== FIRST PASS: Collecting file information ===")
        for zip_info in all_zip_info:
            pipeline_name = zip_info["pipeline_name"]
            zip_path = zip_info["zip_path"]

            if pipeline_name not in dataset_file_info:
                print(f"Processing zip: {pipeline_name}")
                dataset_file_info[pipeline_name] = {
                    "pipeline_name": pipeline_name,
                    "zip_path": str(zip_path),
                    "foreign_keys": [],
                    "exclude_columns": [],
                    "columns_signature": [],
                    "modules": [],
                }

            for csv_file in zip_info["csv_files"]:
                csv_analysis, cache_key = self.cache.get_analysis(
                    zip_path,
                    csv_file,
                    self.csv_analyzer.analyze_csv_from_zip,
                )

                columns_signature = csv_analysis["columns"]
                module_name = self.structure.extract_module_name(csv_file)
                column_count = csv_analysis["column_count"]

                dataset_file_info[pipeline_name]["modules"].append(
                    {
                        "module_name": module_name,
                        "cache_key": cache_key,
                    }
                )

                is_core_file = not ("all_data" in csv_file.lower())

                if is_core_file:
                    # Core file - collect info and determine primary key
                    if csv_analysis["row_count"] > 0 and csv_analysis.get("column_analysis"):
                        # logger.info(f"CORE: {module_name}")

                        # Determine primary key (first column ending in "Code" or first column)
                        pk_column = None
                        for col_info in csv_analysis["column_analysis"]:
                            col_name = col_info["column_name"]
                            if col_name.endswith(" Code") or col_name.endswith("Code"):
                                pk_column = col_name
                                break

                        if not pk_column:
                            pk_column = csv_analysis["column_analysis"][0]["column_name"]

                        # Calculate PK score for has_pk determination
                        first_column = csv_analysis["column_analysis"][0]
                        pk_score = 0
                        is_int_score = 0

                        if first_column["sample_values"]:
                            sample_value = first_column["sample_values"][0]
                            if len(sample_value) < 5:
                                pk_score += 1
                            if "code" in pk_column.lower():
                                pk_score += 1
                            if len(sample_value) < 10 and any(char.isdigit() for char in sample_value):
                                pk_score += 1
                            if sample_value.isdigit():
                                pk_score += 1
                                is_int_score += 1
                            if first_column["is_likely_foreign_key"]:
                                pk_score += 1
                            if first_column["inferred_sql_type"] == "Integer":
                                pk_score += 1
                                is_int_score += 1
                            if column_count < 4:
                                pk_score += 1

                        has_pk = pk_score > 1
                        is_int = is_int_score > 1

                        if module_name not in core_file_info:
                            core_file_info[module_name] = {
                                "module_name": module_name,
                                "columns_signature": columns_signature,
                                "is_core_file": True,
                                "model_name": snake_to_pascal_case(module_name),
                                "column_count": column_count,
                                "has_pk": has_pk,
                                "pk_score": pk_score if has_pk else 0,
                                "pk_column": pk_column,
                                "pk_sql_column_name": to_snake_case(pk_column),
                                "occurrence": 0,
                                "cache_keys": [],
                            }

                        core_file_info[module_name]["occurrence"] += 1
                        core_file_info[module_name]["cache_keys"].append(cache_key)
                else:
                    # Dataset file - capture column signature
                    # print(f"DATASET: {module_name}")
                    dataset_file_info[pipeline_name]["columns_signature"] = columns_signature

        # Second pass: establish FK relationships
        print("\n=== SECOND PASS: Establishing FK relationships ===")
        for core_module_name, core_info in core_file_info.items():
            if not core_info["has_pk"]:
                continue

            # print(f"Processing FK relationships for {core_module_name}")
            pk_column = core_info["pk_column"]

            # Check this core module's primary key against all datasets
            for pipeline_name, pipeline_info in dataset_file_info.items():
                dataset_columns = pipeline_info["columns_signature"]

                if not dataset_columns:  # Skip if no dataset columns found
                    continue

                # Look for the primary key column in this dataset
                found_column = None
                for dataset_col in dataset_columns:
                    if pk_column == dataset_col:
                        # Perfect match
                        found_column = dataset_col
                        break
                    elif pk_column in dataset_col or dataset_col in pk_column:
                        # Partial match (like "Area Code" vs "Area Code (M49)")
                        found_column = dataset_col
                        break

                if found_column:
                    # print(f"  Found relationship: {pk_column} -> {found_column} in {pipeline_name}")

                    # Add to foreign keys
                    if found_column not in pipeline_info["foreign_keys"]:
                        pipeline_info["foreign_keys"].append(
                            {
                                "table_name": core_module_name,
                                "model_name": core_info["model_name"],
                                "pipeline_name": "core",
                                "column_name": to_snake_case(found_column),
                                "actual_column_name": found_column,
                                "index_hash": safe_index_name(f"{core_module_name}", found_column),
                            }
                        )

                    # Add non-primary columns to exclude list using fuzzy matching
                    # Get all FK columns identified so far to avoid excluding them
                    actual_fk_column_names = [
                        fk_info["actual_column_name"] for fk_info in pipeline_info["foreign_keys"]
                    ]
                    # logger.info(
                    #     f"\n\n  Found {len(actual_fk_column_names)} FK columns in dataset '{pipeline_name}': {actual_fk_column_names}"
                    # )
                    for core_col in core_info["columns_signature"]:
                        if core_col != pk_column:
                            # Filter out ALL FK columns from dataset columns when matching
                            filtered_dataset_columns = [
                                col for col in dataset_columns if col not in actual_fk_column_names
                            ]

                            # logger.info(
                            #     f"\n\n  Matching core column '{core_col}' against dataset columns: {filtered_dataset_columns}"
                            # )

                            found_dataset_col = self._find_matching_column_fuzzy(core_col, filtered_dataset_columns)

                            if found_dataset_col and found_dataset_col not in pipeline_info["exclude_columns"]:
                                pipeline_info["exclude_columns"].append(found_dataset_col)

                    # Set mismatch flag only if there's actually a mismatch
                    if found_column != pk_column:
                        pipeline_info["fk_pk_mismatch"] = True
                        core_info["fk_pk_mismatch"] = True
                        core_info["original_pk_column"] = pk_column
                        core_info["found_fk_column"] = found_column
                        core_info["pk_column"] = (found_column,)
                        core_info["original_pk_sql_column_name"] = (to_snake_case(pk_column),)
                        core_info["pk_sql_column_name"] = to_snake_case(found_column)
                    else:
                        # Perfect match - ensure no mismatch flags
                        if "fk_pk_mismatch" not in core_info:
                            core_info["fk_pk_mismatch"] = False

        # Build final output
        specs_output = {
            "core_file_info": dict(core_file_info),
            "dataset_file_info": dict(dataset_file_info),
        }

        specs_output = self.validate_core_file_consistency(specs_output)
        specs_output = self.resolve_conflicts_with_synthetic_pks(specs_output)

        self.file_generator.write_json_file(
            Path("analysis/pipeline_spec.json"),
            specs_output,
        )

        return specs_output

    def validate_core_file_consistency(self, specs_output: Dict[str, Any]) -> Dict[str, Any]:
        """Validate core file consistency - step by step approach"""
        logger.info("🔍 Starting core file validation...")

        core_file_info = specs_output["core_file_info"]

        for core_module_name, core_info in core_file_info.items():
            # logger.info(f"📊 Processing {core_module_name}...")

            if "pk_column" not in core_info:
                logger.warning(f"  ⚠️  No primary key column found for {core_module_name}, skipping validation")
                continue

            cache_keys = core_info["cache_keys"]
            # logger.info(f"  Found {len(cache_keys)} files for {core_module_name}")

            # Collect all dataframes for this core module
            all_dfs = []

            for cache_key in cache_keys:
                # Parse cache key: "ZipName:CSVFileName"
                zip_name, csv_filename = cache_key.split(":", 1)

                # Find the zip file path
                zip_path = None
                all_zip_info = self.scanner.scan_all_zips()
                for zip_info in all_zip_info:
                    if zip_info["zip_path"].name == zip_name:
                        zip_path = zip_info["zip_path"]
                        break

                if not zip_path:
                    logger.warning(f"    ⚠️  Could not find zip: {zip_name}")
                    continue

                # Read the CSV data
                try:
                    df = self._read_full_csv_from_zip(zip_path, csv_filename)
                    df["source_dataset"] = zip_name  # Track which dataset this came from
                    all_dfs.append(df)
                    # logger.info(f"    ✅ Read {len(df)} rows from {csv_filename}")

                except Exception as e:
                    logger.warning(f"    ⚠️  Error reading {csv_filename}: {e}")

            # Combine all dataframes for this core module
            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                # logger.info(f"  📋 Combined into {len(combined_df)} total rows for {core_module_name}")

                # Get unique combinations (excluding the source_dataset column we added)
                data_columns = [col for col in combined_df.columns if col != "source_dataset"]
                # Get the actual primary key column name from the dataframe
                pk_column = core_info.get("pk_column")

                # If pk_column doesn't exist in the dataframe, try original_pk_column or first column
                if pk_column not in combined_df.columns:
                    if "original_pk_column" in core_info and core_info["original_pk_column"] in combined_df.columns:
                        pk_column = core_info["original_pk_column"]
                        logger.info(f"  📝 Using original_pk_column: {pk_column}")
                    else:
                        pk_column = combined_df.columns[0]  # Use first column as fallback
                        logger.info(f"  📝 Using first column as pk: {pk_column}")
                #
                grouped = combined_df.groupby(pk_column, as_index=False)

                conflicts = []
                similarities = []

                for name, group in grouped:
                    data_columns = [col for col in group.columns if col != "source_dataset"]
                    unique_df = group.drop_duplicates(subset=data_columns)
                    if len(unique_df) > 1:
                        # logger.warning(f"  ⚠️  CONFLICT found for {name} in {core_module_name}")
                        # print(unique_df)
                        # print()  # Add blank line between conflicts
                        for col in data_columns:
                            if col == core_info.get("pk_column"):
                                continue  # Skip the primary key column

                            values = unique_df[col].unique()
                            if len(values) > 1:
                                if self._values_are_similar(values):
                                    similarities.append(
                                        {
                                            "pk_value": name,
                                            "column": col,
                                            "values": list(values),
                                            "source_dataset": unique_df["source_dataset"].unique().tolist(),
                                        }
                                    )
                                else:
                                    conflicts.append(
                                        {
                                            "pk_value": name,
                                            "column": col,
                                            "values": list(values),
                                            "source_dataset": unique_df["source_dataset"].unique().tolist(),
                                        }
                                    )

                # Add to the core_file_info
                core_info["conflicts"] = conflicts
                core_info["similarities"] = similarities
                core_info["data_consistent"] = len(conflicts) == 0
                core_info["total_conflicts"] = len(conflicts)
                core_info["total_similarities"] = len(similarities)

                # logger.info(f"  🔍 Found {len(unique_df)} unique combinations for {core_module_name}")

        return specs_output

    def resolve_conflicts_with_synthetic_pks(self, specs_output: Dict[str, Any]) -> Dict[str, Any]:
        """Third pass: Create synthetic PKs for conflicts and update dataset specs"""
        logger.info("🔧 Starting conflict resolution with synthetic PKs...")

        core_file_info = specs_output["core_file_info"]
        dataset_file_info = specs_output["dataset_file_info"]

        # Start synthetic PKs at a high number to avoid conflicts
        SYNTHETIC_PK_START = 12_345_678
        global_synthetic_counter = SYNTHETIC_PK_START

        for core_module_name, core_info in core_file_info.items():
            conflicts = core_info.get("conflicts", [])
            if not conflicts:
                continue

            logger.info(f"📝 Resolving {len(conflicts)} conflicts for {core_module_name}")

            # Read combined data to find max PK
            pk_column = core_info["pk_column"]
            # combined_df = self._get_combined_core_data(core_module_name, core_info)
            # max_pk = int(combined_df[pk_column].max())

            conflict_mappings = {}

            for conflict in conflicts:
                original_pk = conflict["pk_value"]
                conflict_column = conflict["column"]
                conflict_values = conflict["values"]

                # First value keeps original PK, others get synthetic PKs
                value_mapping = {conflict_values[0]: original_pk}

                for conflict_value in conflict_values[1:]:
                    value_mapping[conflict_value] = global_synthetic_counter
                    print(f"  Synthetic PK: {conflict_value} -> {global_synthetic_counter}")
                    global_synthetic_counter += 1

                conflict_mappings[original_pk] = {"column": conflict_column, "value_mapping": value_mapping}

                # Update the conflict object with synthetic PK mappings
                conflict["value_pk_mapping"] = value_mapping

            # Update the core_file_info with enhanced conflicts
            core_info["conflicts"] = conflicts

            # Get all source datasets that have conflicts for this core table
            conflicted_datasets = set()
            for conflict in conflicts:
                for zip_name in conflict["source_dataset"]:
                    # Convert ZIP name to pipeline name
                    pipeline_name = self.scanner._format_pipeline_name(zip_name)
                    conflicted_datasets.add(pipeline_name)

            logger.info(f"  Adding FK fixes to datasets: {conflicted_datasets}")

            # Update only datasets that actually contain conflicted data
            for dataset_name, dataset_info in dataset_file_info.items():
                if dataset_name in conflicted_datasets:
                    for fk in dataset_info.get("foreign_keys", []):
                        if fk["table_name"] == core_module_name:
                            if "conflict_pk_mappings" not in dataset_info:
                                dataset_info["conflict_pk_mappings"] = {}

                            # Include FK column name in the mappings
                            enhanced_conflict_mappings = {}
                            for original_pk, mapping_info in conflict_mappings.items():
                                enhanced_conflict_mappings[original_pk] = {
                                    **mapping_info,
                                    "fk_column_name": fk["column_name"],
                                }

                            dataset_info["conflict_pk_mappings"][core_module_name] = enhanced_conflict_mappings
                            break

        return specs_output

    def _find_matching_column_fuzzy(self, core_col: str, dataset_columns: list) -> str | None:
        """Find dataset column using fuzzy word matching"""
        import re

        # Phase 1: Check for exact matches first (highest priority)
        for dataset_col in dataset_columns:
            if core_col == dataset_col:
                return dataset_col

        # Phase 2: Check for substring matches (medium priority)
        for dataset_col in dataset_columns:
            if core_col in dataset_col or dataset_col in core_col:
                return dataset_col

        # Phase 3: Check for word overlap matches (lowest priority)
        core_words = set(re.findall(r"\b\w+\b", core_col.lower()))

        best_match = None
        best_score = 0

        for dataset_col in dataset_columns:
            # Word overlap scoring - require ALL core words to be present
            dataset_words = set(re.findall(r"\b\w+\b", dataset_col.lower()))
            common_words = core_words.intersection(dataset_words)

            if len(common_words) > 0:
                score = len(common_words) / len(core_words)
                if score == 1.0:  # ALL words from core column must be in dataset column
                    best_score = score
                    best_match = dataset_col

        return best_match

    def _get_combined_core_data(self, core_module_name: str, core_info: Dict) -> pd.DataFrame:
        """Read and combine all CSV files for a specific core module"""
        cache_keys = core_info["cache_keys"]
        all_dfs = []

        print(f"\n🔍 Combining data for core module: {core_module_name} {core_info}\n")
        conflict_csv_files = list(
            set([filename for conflict in core_info["conflicts"] for filename in conflict["source_dataset"]])
        )

        for cache_key in cache_keys:
            # Parse cache key: "ZipName:CSVFileName"
            zip_name, csv_filename = cache_key.split(":", 1)

            print(f"\n  Reading {csv_filename} from {conflict_csv_files}\n")

            if csv_filename not in conflict_csv_files:
                logger.info(f"    ❌ Skipping {csv_filename} as it is not in conflict CSV files")
                continue

            # Find the zip file path
            zip_path = None
            all_zip_info = self.scanner.scan_all_zips()
            for zip_info in all_zip_info:
                if zip_info["zip_path"].name == zip_name:
                    zip_path = zip_info["zip_path"]
                    break

            if not zip_path:
                logger.warning(f"    ⚠️  Could not find zip: {zip_name}")
                continue

            # Read the CSV data
            try:
                df = self._read_full_csv_from_zip(zip_path, csv_filename)
                df["source_dataset"] = zip_name  # Track which dataset this came from
                all_dfs.append(df)
            except Exception as e:
                logger.warning(f"    ⚠️  Error reading {csv_filename}: {e}")

        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            return combined_df
        else:
            return pd.DataFrame()

    def _values_are_similar(self, values) -> bool:
        """Check if values are similar enough to be considered formatting differences"""
        if len(values) < 2:
            return True

        values_list = [str(v).strip() for v in values]

        # Check each pair of values
        for i in range(len(values_list)):
            for j in range(i + 1, len(values_list)):
                val1 = values_list[i].lower()
                val2 = values_list[j].lower()

                # Basic cleaning - remove parentheses and normalize punctuation
                clean1 = self._clean_for_similarity(val1)
                clean2 = self._clean_for_similarity(val2)

                # If cleaned versions are identical, they're similar
                if clean1 == clean2:
                    continue

                # Calculate character-level similarity
                from difflib import SequenceMatcher

                similarity = SequenceMatcher(None, clean1, clean2).ratio()

                # If strings are similar (80%+), consider them similar
                if similarity >= 0.8:
                    continue

                # Word overlap logic with lower threshold
                words1 = set(clean1.split())
                words2 = set(clean2.split())

                if len(words1) == 0 or len(words2) == 0:
                    return False

                overlap = len(words1.intersection(words2))
                total_unique = len(words1.union(words2))
                word_similarity = overlap / total_unique

                # Lowered threshold from 0.7 to 0.5
                if word_similarity < 0.5:
                    return False

        return True

    def _clean_for_similarity(self, text: str) -> str:
        """Clean text for similarity comparison - basic cleaning only"""
        import re

        # Remove parenthetical content
        text = re.sub(r"\([^)]*\)", "", text)

        # Remove extra punctuation
        text = re.sub(r"[;,\.]", " ", text)

        # Normalize whitespace
        text = " ".join(text.split())

        return text.strip()

    def _read_full_csv_from_zip(self, zip_path: Path, csv_filename: str) -> pd.DataFrame:
        """Read complete CSV data from ZIP file"""
        import zipfile
        from io import StringIO

        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(csv_filename) as csv_file:
                # Try different encodings
                encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                for encoding in encodings:
                    try:
                        csv_file.seek(0)
                        csv_data = csv_file.read().decode(encoding)
                        df = pd.read_csv(StringIO(csv_data), dtype=str)
                        # Clean column names
                        df.columns = df.columns.str.strip()
                        return df
                    except UnicodeDecodeError:
                        continue

                # If all encodings fail, use errors='ignore'
                csv_file.seek(0)
                csv_data = csv_file.read().decode("utf-8", errors="ignore")
                df = pd.read_csv(StringIO(csv_data), dtype=str)
                df.columns = df.columns.str.strip()
                return df
