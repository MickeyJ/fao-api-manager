import json
from typing import List, Dict, Literal
from pathlib import Path
from dataclasses import dataclass
from .scanner import Scanner
from .structure import Structure
from .core_pipeline import CorePipeline
from .dataset_pipelines import DatasetPipelines
from .csv_analyzer import CSVAnalyzer
from .file_generator import FileGenerator
from .template_renderer import TemplateRenderer
from .pipeline_specs import PipelineSpecs
from . import logger, clean_text, format_column_name, safe_index_name


@dataclass
class ProjectPath:
    project: Path
    src: Path = Path("src")
    db: Path = Path("src/db")
    db_models: Path = Path("src/db/models")
    db_pipelines: Path = Path("src/db/pipelines")
    api: Path = Path("src/api")
    api_routers: Path = Path("src/api/routers")


class Generator:
    def __init__(self, output_dir: str | Path, input_dir: str):
        self.project_name = clean_text(output_dir)
        self.paths = ProjectPath(Path(output_dir))
        self.input_dir = input_dir
        self.structure = Structure()
        self.file_generator = FileGenerator(self.paths.project)
        self.scanner = Scanner(self.input_dir, self.structure)
        # self.csv_analyzer = CSVAnalyzer(self.structure, self.scanner, self.file_generator)
        self.template_renderer = TemplateRenderer(self.project_name)

        self.all_zip_info = self.scanner.scan_all_zips()

        # Will hold all modules from all pipelines
        self.all_modules = []
        self.pipelines = {}

    def generate(self) -> None:
        """Main generation workflow"""
        # Step 1: Discover all modules
        self._discover_modules(self.all_zip_info)

        # Step 3
        self._generate_directories()
        self._generate_files()

    def _discover_modules(self, all_zip_info: List[Dict]):
        """Discover modules from all pipelines"""
        from generator.fao_structure_modules import FAOStructureModules
        from generator.fao_foreign_key_mapper import FAOForeignKeyMapper
        from generator.fao_conflict_detector import FAOConflictDetector
        from generator.fao_reference_data_extractor import LOOKUP_MAPPINGS

        json_cache_path = Path("./analysis/fao_module_cache.json")
        cache_bust = False

        # Structure discovery
        structure_modules = FAOStructureModules(self.input_dir, LOOKUP_MAPPINGS, json_cache_path, cache_bust)
        structure_modules.run()

        # Add foreign keys
        fk_mapper = FAOForeignKeyMapper(structure_modules.results, LOOKUP_MAPPINGS, json_cache_path, cache_bust)
        fk_mapper.enhance_datasets_with_foreign_keys()

        # Add conflicts
        conflict_detector = FAOConflictDetector(structure_modules.results, json_cache_path, cache_bust)
        enhanced_results = conflict_detector.enhance_with_conflicts()

        # Save and use results
        structure_modules.save()

        # Step 2: Convert to module format
        self._process_lookups(enhanced_results["lookups"])
        # self._process_datasets(enhanced_results["datasets"])

        extraction_manifest = self.scanner.create_extraction_manifest(all_zip_info)
        self.file_generator.write_json_file(self.paths.project / "extraction_manifest.json", extraction_manifest)

    def _process_lookups(self, lookups: Dict):
        """Process lookup modules with conflict handling"""
        for lookup_name, lookup_spec in lookups.items():
            module = {
                "pipeline_name": lookup_name,  # Each lookup gets own pipeline
                "module_name": lookup_name,
                "model_name": lookup_spec["sql_model_name"],
                "table_name": lookup_spec["sql_table_name"],
                "column_analysis": lookup_spec["column_analysis"],
                "file_info": {
                    "csv_file": lookup_spec["file_path"],
                    "csv_filename": Path(lookup_spec["file_path"]).name,
                    "zip_path": None,  # Synthetic lookups don't have zips
                },
                "specs": {
                    "is_core_file": True,  # Flag as lookup/core
                    "pk_column": lookup_spec["primary_key"],
                    "pk_sql_column_name": format_column_name(lookup_spec["primary_key"]),
                    "conflicts": lookup_spec.get("conflicts", []),
                    "has_conflicts": lookup_spec.get("has_conflicts", False),
                },
            }
            self.all_modules.append(module)

    def _process_datasets(self, datasets: Dict):
        """Process dataset modules with FK and conflict info"""
        for dataset_name, dataset_spec in datasets.items():
            # Format foreign keys for template compatibility
            foreign_keys = []
            for fk in dataset_spec.get("foreign_keys", []):
                foreign_keys.append(
                    {
                        "table_name": fk["lookup_sql_table"],
                        "model_name": fk["lookup_sql_model"],
                        "column_name": format_column_name(fk["dataset_fk_csv_column"]),
                        "actual_column_name": fk["dataset_fk_csv_column"],
                        "pipeline_name": fk["lookup_sql_table"],  # For imports
                        "index_hash": safe_index_name(
                            f"{dataset_spec['sql_table_name']}{fk['lookup_sql_table']}", fk["dataset_fk_csv_column"]
                        ),
                    }
                )

            module = {
                "pipeline_name": dataset_name,
                "module_name": dataset_name,
                "model_name": dataset_spec["sql_model_name"],
                "table_name": dataset_spec["sql_table_name"],
                "file_info": {
                    "csv_files": [dataset_spec["main_data_file"]],
                    "csv_filename": Path(dataset_spec["main_data_file"]).name,
                },
                "specs": {
                    "is_core_file": False,
                    "foreign_keys": foreign_keys,
                    "exclude_columns": dataset_spec.get("exclude_columns", []),
                    "conflict_resolutions": dataset_spec.get("conflict_resolutions", {}),
                },
            }
            self.all_modules.append(module)

    def _generate_files(self):
        """Generate all pipeline and model files"""
        self._generate_project_files()
        self._group_modules_by_pipeline()

        for pipeline_name, modules in self.pipelines.items():
            self._generate_pipeline_and_models(pipeline_name, modules)
        self._generate_all_pipelines_main()
        self._generate_pipelines_init()
        self.generate_all_model_imports_file()
        # self._generate_api_routers()

    def _group_modules_by_pipeline(self) -> None:
        """Group modules by their pipeline name"""
        for module in self.all_modules:
            pipeline_name = module["pipeline_name"]

            if pipeline_name not in self.pipelines:
                self.pipelines[pipeline_name] = []
            self.pipelines[pipeline_name].append(module)

    def _generate_pipeline_and_models(self, pipeline_name: str, modules: List[Dict]):
        """Generate files for a single pipeline"""
        # Create pipeline and model directory
        self.file_generator.create_dir(self.paths.db_pipelines / pipeline_name)
        # self.file_generator.create_dir(self.paths.db_models / pipeline_name)

        # Generate pipeline files
        self._generate_pipeline_init(pipeline_name, modules)
        self._generate_pipeline_main(pipeline_name, modules)
        self._generate_modules_and_models(
            pipeline_name,
            self.paths.db_pipelines / pipeline_name,
            self.paths.db_models / pipeline_name,
            modules,
        )

    def _generate_pipeline_init(self, pipeline_name, modules):
        """Generate __init__.py for pipeline"""
        content = self.template_renderer.render_pipeline_init_template(directory_name=pipeline_name, modules=modules)
        self.file_generator.write_file_cache(self.paths.db_pipelines / pipeline_name / "__init__.py", content)

    def _generate_pipeline_main(self, pipeline_name, modules):
        """Generate __main__.py for pipeline"""
        # Deduplicate module names
        module_names = sorted(list(set(module["module_name"] for module in modules)))

        content = self.template_renderer.render_pipeline_main_template(
            pipeline_name=pipeline_name, modules=module_names
        )
        self.file_generator.write_file_cache(self.paths.db_pipelines / pipeline_name / "__main__.py", content)

    def _generate_all_pipelines_main(self):
        """Generate db/pipelines/__main__.py that runs all pipelines"""
        pipeline_names = list(self.pipelines.keys())

        content = self.template_renderer.render_pipelines_main_template(pipeline_names=pipeline_names)

        self.file_generator.write_file_cache(self.paths.db_pipelines / "__main__.py", content)

    def _generate_pipelines_init(self):
        """Generate db/models/__init__.py with all model imports"""

        content = self.template_renderer.render_pipelines_init_template()
        self.file_generator.write_file_cache(self.paths.db_pipelines / "__init__.py", content)

    def generate_all_model_imports_file(self):
        """Generate db/models/__init__.py with all model imports"""
        imports = []
        for pipeline_name, modules in self.pipelines.items():
            for module in modules:
                imports.append(
                    {
                        "pipeline_name": pipeline_name,
                        "module_name": module["module_name"],
                        "model_name": module["model_name"],
                    }
                )

        content = self.template_renderer.render_all_model_imports_template(imports=imports)
        self.file_generator.write_file_cache("all_model_imports.py", content)

    def _generate_modules_and_models(self, pipeline_name, pipeline_dir, model_dir, modules):
        """Generate both pipeline modules and model files"""
        for module in modules:
            module_name = module["module_name"]

            if module["specs"]["is_core_file"]:
                # Lookup module
                pipeline_content = self.template_renderer.render_lookup_module_template(
                    csv_file=module["file_info"]["csv_file"],  # Singular
                    model_name=module["model_name"],
                    table_name=module["table_name"],
                    column_analysis=module["column_analysis"],
                    specs=module["specs"],
                )
                self.file_generator.write_file_cache(pipeline_dir / f"{module_name}.py", pipeline_content)
            else:
                """Dataset module next"""

            # # Generate model file (areas.py in model_dir)
            # model_content = self.template_renderer.render_model_template(
            #     model_name=module["model_name"],
            #     table_name=module["table_name"],
            #     csv_analysis=module["column_analysis"],
            #     specs=module["specs"],
            # )
            # self.file_generator.write_file_cache(pipeline_dir / f"{module_name}_model.py", model_content)

            # Generate analysis JSON
            self.file_generator.write_json_file(
                self.paths.project / pipeline_dir / f"{module_name}.json",
                module["column_analysis"],
            )

    def _generate_api_routers(self):
        """Generate API routers for each pipeline"""
        api_routers = self._format_api_routers()

        # print(json.dumps(api_routers, indent=2))

        content = self.template_renderer.render_api_main_template(routers=api_routers)
        self.file_generator.write_file_cache(self.paths.api / "__main__.py", content)

        content = self.template_renderer.render_api_init_template()
        self.file_generator.write_file_cache(self.paths.api / "__init__.py", content)

        for router in api_routers:
            router_dir = router["router_dir"]
            self.file_generator.create_dir(router_dir)

            content = self.template_renderer.render_api_router_template(router=router)
            self.file_generator.write_file_cache(self.paths.api_routers / f"{router['name']}.py", content)

        # # Generate __init__.py for routers
        # init_content = self.template_renderer.render_api_routers_init_template(routers=api_routers)
        # self.file_generator.write_file(self.paths.api_routers / "__init__.py", init_content)

    def _format_api_routers(self) -> list[dict]:
        """Format API routers by generating __init__.py"""
        api_routers = []
        for module in self.all_modules:
            pipeline_name = module["pipeline_name"]
            module_name = module["module_name"]

            api_routers.append(
                {
                    "name": module_name,
                    "model": module["model_name"],
                    "pipeline_name": pipeline_name,
                    "router_dir": f"{self.paths.api_routers}",
                    "router_name": f"{module_name}_router",
                    "csv_analysis": module["csv_analysis"],
                    "specs": module["specs"],
                }
            )

        return api_routers

    def _generate_directories(self):
        self.file_generator.create_dir(self.paths.src)
        self.file_generator.create_dir(self.paths.db)
        self.file_generator.create_dir(self.paths.db_pipelines)
        self.file_generator.create_dir(self.paths.api)
        self.file_generator.create_dir(self.paths.api_routers)

    def _generate_project_files(self):
        self._generate_env_files()
        self._generate_makefile()
        self._generate_requirements_file()
        self._generate_database_file()
        self._generate_database_utils_file()
        self._generate_empty_init_files()
        self._generate_project_main()

    def _generate_project_main(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_project_main_template()
        self.file_generator.write_file_cache("__main__.py", content)

    def _generate_empty_init_files(self):
        """Generate __init__.py for root, src"""
        content = self.template_renderer.render_empty_init_template()
        self.file_generator.write_file("__init__.py", content)
        self.file_generator.write_file(self.paths.src / "__init__.py", content)
        self.file_generator.write_file(self.paths.db / "__init__.py", content)

    def _generate_env_files(self):
        """Generate database.py for db"""
        templates = self.template_renderer.render_env_templates()

        for template in templates:
            self.file_generator.write_file_cache(template["file_name"], template["content"])

    def _generate_makefile(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_makefile_template()
        self.file_generator.write_file_cache("Makefile", content)

    def _generate_requirements_file(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_requirements_template()
        self.file_generator.write_file_cache("requirements.in", content)

    def _generate_database_file(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_database_template()
        self.file_generator.write_file_cache(self.paths.db / "database.py", content)

    def _generate_database_utils_file(self):
        """Generate utils.py for db"""
        content = self.template_renderer.render_database_utils_template()
        self.file_generator.write_file_cache(self.paths.db / "utils.py", content)
