import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict

from .csv_cache import CSVCache
from generator.scanner import Scanner
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.csv_analyzer import CSVAnalyzer

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PipelineSpecs:
    def __init__(self, zip_path: str, shared_cache: CSVCache | None = None):
        self.scanner = Scanner(zip_path)
        self.structure = Structure()
        self.file_generator = FileGenerator("./analysis")
        self.csv_analyzer = CSVAnalyzer(self.structure, self.scanner, self.file_generator)
        self.cache = shared_cache or CSVCache()

    def create(self) -> Dict[str, Any]:
        return self.discover_file_patterns()

    def discover_file_patterns(self) -> Dict[str, Any]:
        """Simplified pattern discovery focused on core files"""
        file_groups = defaultdict(list)

        all_zip_info = self.scanner.scan_all_zips()

        """
            zip_info: {
                "zip_name": zip_path.name,
                "zip_path": zip_path,
                "csv_files": csv_files,
                "pipeline_name": self._format_pipeline_name(zip_path.name),
            }
        """

        dataset_file_info = {}
        core_file_info = {}

        for zip_info in all_zip_info:
            pipeline_name = zip_info["pipeline_name"]
            zip_path = zip_info["zip_path"]

            print(f"Processing zip: {pipeline_name}")

            if pipeline_name not in dataset_file_info:
                dataset_file_info[pipeline_name] = {
                    "pipeline_name": pipeline_name,
                    "zip_path": str(zip_path),
                    "foreign_keys": [],
                    "exclude_columns": [],
                    "columns_signature": [],
                    "modules": [],
                }

            for csv_file in zip_info["csv_files"]:

                is_core_file = not ("all_data" in csv_file.lower())

                csv_analysis, cache_key = self.cache.get_analysis(
                    zip_path,
                    csv_file,
                    self.csv_analyzer.analyze_csv_from_zip,
                )

                columns_signature = csv_analysis["columns"]
                module_name = self.structure.extract_module_name(csv_file)
                column_count = csv_analysis["column_count"]

                print(f"\nPipeline: {zip_info['pipeline_name']}")
                print(f"=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")

                dataset_file_info[pipeline_name]["modules"].append(
                    {
                        "module_name": module_name,
                        "cache_key": cache_key,
                    }
                )
                file_info = {
                    "module_name": module_name,
                    "columns_signature": columns_signature,
                    "is_core_file": is_core_file,
                    "column_count": column_count,
                }

                first_column = csv_analysis["column_analysis"][0]

                if is_core_file and first_column["sample_values"]:

                    print(f"CORE: {module_name}")
                    first_column_name = first_column["column_name"]
                    sample_value = first_column["sample_values"][0]

                    pk_score = 0

                    if len(sample_value) < 5:
                        pk_score += 1
                    if "code" in first_column_name.lower():
                        pk_score += 1
                    if len(sample_value) < 10 and any(char.isdigit() for char in sample_value):
                        pk_score += 1
                    if sample_value.isdigit():
                        pk_score += 1
                    if first_column["is_likely_foreign_key"]:
                        pk_score += 1
                    if first_column["inferred_sql_type"] == "Integer":
                        pk_score += 1
                    if column_count < 4:
                        pk_score += 1

                    print(f"First Column {first_column_name} - PK Score: {pk_score}")

                    has_pk = pk_score > 1

                    file_info["has_pk"] = has_pk
                    file_info["occurrence"] = 0
                    if has_pk:

                        file_info["pk_score"] = pk_score

                        for core_column in columns_signature:
                            dataset_columns = dataset_file_info[pipeline_name]["columns_signature"]
                            found_column = next(
                                (col for col in dataset_columns if core_column in col),
                                None,
                            )
                            print(f"\n\n\n{found_column}")
                            if found_column:
                                if found_column != core_column:
                                    dataset_file_info[pipeline_name]["fk_pk_mismatch"] = True
                                    dataset_file_info[pipeline_name]["foreign_keys"].append(found_column)
                                    file_info["fk_pk_mismatch"] = True
                                    file_info["original_pk_column"] = core_column
                                    file_info["found_fk_column"] = found_column
                                    file_info["pk_column"] = found_column

                                else:
                                    file_info["pk_column"] = first_column_name
                                    dataset_file_info[pipeline_name]["foreign_keys"].append(first_column_name)
                                    break

                        dataset_file_info[pipeline_name]["exclude_columns"].extend(csv_analysis["columns"][1:])

                    if module_name not in core_file_info:
                        core_file_info[module_name] = file_info

                    if not "cache_keys" in core_file_info[module_name]:
                        core_file_info[module_name]["cache_keys"] = []

                    core_file_info[module_name]["occurrence"] += 1
                    core_file_info[module_name]["cache_keys"].append(cache_key)

                    # dataset_file_info[pipeline_name]["files"].append(
                    #     core_file_info[module_name]
                    # )
                else:
                    print(f"DATASET: {module_name}")
                    file_info["cache_keys"] = []
                    file_info["cache_keys"].append(cache_key)
                    dataset_file_info[pipeline_name]["columns_signature"] = columns_signature

        print(f"Core Modules: {len(core_file_info)}")
        for module_name, file_info in core_file_info.items():
            print(f"{module_name} - {file_info['occurrence']}")

        print(f"Core Modules: {len(core_file_info)}")
        for pipeline_name, pipeline_info in dataset_file_info.items():
            print(f"{pipeline_name} - {pipeline_info['foreign_keys']}")

        specs_output = {
            "core_file_info": dict(core_file_info),
            "dataset_file_info": dict(dataset_file_info),
        }

        self.file_generator.write_json_file(
            Path("analysis/pipeline_spec.json"),
            specs_output,
        )

        return specs_output
