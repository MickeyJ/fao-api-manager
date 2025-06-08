from typing import List, Dict, Literal
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass
from .structure import Structure
from .file_system import FileSystem
from .template_renderer import TemplateRenderer
from generator.fao_reference_data_extractor import LOOKUP_MAPPINGS
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
        self.file_system = FileSystem(self.paths.project)
        self.template_renderer = TemplateRenderer(self.project_name)

        # Will hold all modules from all pipelines
        self.all_modules = []
        self.pipelines = {}

    def generate(self) -> None:
        """Main generation workflow"""
        # Step 1: Discover all modules
        self._discover_modules()

        # Step 3
        self._generate_directories()
        self._generate_files()

    def _discover_modules(self):
        """Discover modules from all pipelines"""
        from generator.fao_structure_modules import FAOStructureModules
        from generator.fao_foreign_key_mapper import FAOForeignKeyMapper

        json_cache_path = Path("./cache/fao_module_cache.json")
        cache_bust = False

        # Structure discovery
        structure_modules = FAOStructureModules(self.input_dir, LOOKUP_MAPPINGS, json_cache_path, cache_bust)
        structure_modules.run()

        # Add foreign keys
        fk_mapper = FAOForeignKeyMapper(structure_modules.results, LOOKUP_MAPPINGS, json_cache_path, cache_bust)
        enhanced_results = fk_mapper.enhance_datasets_with_foreign_keys()

        # Save and use results
        structure_modules.save()

        # Step 2: Convert to module format
        # Just use the modules directly!
        for lookup_name, lookup in enhanced_results["lookups"].items():
            self.all_modules.append(lookup)

        for dataset_name, dataset in enhanced_results["datasets"].items():
            self.all_modules.append(dataset)

        # extraction_manifest = self.scanner.create_extraction_manifest(all_zip_info)
        # self.file_system.write_json_file(self.paths.project / "extraction_manifest.json", extraction_manifest)

    def _generate_files(self):
        """Generate all pipeline and model files"""
        self._generate_project_files()
        self._group_modules_by_pipeline()

        for pipeline_name, modules in self.pipelines.items():
            self._generate_pipeline_and_models(pipeline_name, modules)
        self._generate_all_pipelines_main()
        self._generate_pipelines_init()
        self.generate_all_model_imports_file()
        self._generate_api_routers()

        # Add schema documentation generation
        self._generate_schema_documentation()

    def _group_modules_by_pipeline(self) -> None:
        """Group modules by their pipeline name"""
        for module in self.all_modules:
            pipeline_name = module["name"]

            if pipeline_name not in self.pipelines:
                self.pipelines[pipeline_name] = []
            self.pipelines[pipeline_name].append(module)

    def _generate_pipeline_and_models(self, pipeline_name: str, modules: List[Dict]):
        """Generate files for a single pipeline"""
        # Create pipeline directory
        self.file_system.create_dir(self.paths.db_pipelines / pipeline_name)

        # Generate all pipeline related files
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
        self.file_system.write_file_cache(self.paths.db_pipelines / pipeline_name / "__init__.py", content)

    def _generate_pipeline_main(self, pipeline_name, modules):
        """Generate __main__.py for pipeline"""
        # Deduplicate module names
        module_names = sorted(list(set(module["name"] for module in modules)))

        content = self.template_renderer.render_pipeline_main_template(
            pipeline_name=pipeline_name, modules=module_names
        )
        self.file_system.write_file_cache(self.paths.db_pipelines / pipeline_name / "__main__.py", content)

    def _generate_all_pipelines_main(self):
        """Generate db/pipelines/__main__.py that runs all pipelines"""
        pipeline_names = list(self.pipelines.keys())

        content = self.template_renderer.render_pipelines_main_template(pipeline_names=pipeline_names)

        self.file_system.write_file_cache(self.paths.db_pipelines / "__main__.py", content)

    def _generate_pipelines_init(self):
        """Generate db/models/__init__.py with all model imports"""

        content = self.template_renderer.render_pipelines_init_template()
        self.file_system.write_file_cache(self.paths.db_pipelines / "__init__.py", content)

    def generate_all_model_imports_file(self):
        """Generate project file with all model imports for alembic migrations"""
        imports = []
        for pipeline_name, modules in self.pipelines.items():
            for module in modules:
                imports.append(
                    {
                        "module_name": module["name"],
                        "model_name": module["model"]["model_name"],
                    }
                )

        content = self.template_renderer.render_all_model_imports_template(imports=imports)
        self.file_system.write_file_cache("all_model_imports.py", content)

    def _generate_modules_and_models(self, pipeline_name, pipeline_dir, model_dir, modules):
        """Generate all pipeline module and model files"""
        for module in modules:
            module_name = module["name"]

            module_template = (
                self.template_renderer.render_lookup_module_template
                if module["is_lookup_module"]
                else self.template_renderer.render_dataset_module_template
            )

            pipeline_content = module_template(module)
            self.file_system.write_file_cache(pipeline_dir / f"{module_name}.py", pipeline_content)

            # Generate model file (areas.py in model_dir)
            model_content = self.template_renderer.render_model_template(
                module,
                safe_index_name,
            )
            self.file_system.write_file_cache(pipeline_dir / f"{module_name}_model.py", model_content)

            # Generate analysis JSON
            self.file_system.write_json_file(
                self.paths.project / pipeline_dir / f"{module_name}.json",
                module["model"]["column_analysis"],
            )

    def _generate_api_routers(self):
        """Generate API routers for each pipeline"""
        router_groups = self._format_api_routers()

        # print(json.dumps(api_routers, indent=2))

        main_content = self.template_renderer.render_api_main_template(routers=router_groups)
        self.file_system.write_file_cache(self.paths.api / "__main__.py", main_content)

        init_content = self.template_renderer.render_api_init_template(routers=router_groups)
        self.file_system.write_file_cache(self.paths.api / "__init__.py", init_content)

        self.file_system.create_dir(self.paths.api_routers)
        content = self.template_renderer.render_empty_init_template()
        self.file_system.write_file(self.paths.api_routers / "__init__.py", content)

        for group_name, router_group in router_groups.items():
            # Sort routers by name within each group
            router_dir = self.paths.api_routers / group_name
            self.file_system.create_dir(router_dir)

            group_init_content = self.template_renderer.render_api_router_group_init_template(
                group_name=group_name,
                router_group=router_group,
            )
            self.file_system.write_file_cache(router_dir / "__init__.py", group_init_content)

            for router in router_group:
                router_content = self.template_renderer.render_api_router_template(router=router)
                self.file_system.write_file_cache(router_dir / f"{router['name']}.py", router_content)

        # # Generate __init__.py for routers
        # init_content = self.template_renderer.render_api_routers_init_template(routers=api_routers)
        # self.file_system.write_file(self.paths.api_routers / "__init__.py", init_content)

    def _format_api_routers(self) -> defaultdict:
        """Format API routers by generating __init__.py"""

        # Count modules in each group to determine
        # if they should be grouped together or in a miscellaneous group
        module_group_counts = defaultdict(int)
        for module in self.all_modules:
            module_name = module["name"]
            prefix = module_name.split("_")[0]
            module_group_counts[prefix] += 1

        router_groups = defaultdict(list)
        for module in self.all_modules:
            pipeline_name = module["name"]
            module_name = module["name"]

            group_name = module_name.split("_")[0]
            module_group_count = module_group_counts[group_name]
            if module_group_count < 2:
                group_name = "other"

            # logger.info(f" {module_group_count} modules in {group_name} group")

            router_specs = {
                "name": module_name,
                "router_group": group_name,
                "model": module["model"],
                "pipeline_name": pipeline_name,
                "router_dir": Path(f"{self.paths.api_routers}/{group_name}"),
                "router_name": f"{module_name}_router",
            }

            router_groups[group_name].append(router_specs)

        return router_groups

    def _generate_directories(self):
        self.file_system.create_dir(self.paths.src)
        self.file_system.create_dir(self.paths.db)
        self.file_system.create_dir(self.paths.db_pipelines)
        self.file_system.create_dir(self.paths.api)
        self.file_system.create_dir(self.paths.api_routers)

    def _generate_project_files(self):
        self._generate_database_file()
        self._generate_database_utils_file()
        self._generate_empty_init_files()
        self._generate_project_main()

    def _generate_project_main(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_project_main_template()
        self.file_system.write_file_cache("__main__.py", content)

    def _generate_empty_init_files(self):
        """Generate __init__.py for root, src"""
        content = self.template_renderer.render_empty_init_template()
        self.file_system.write_file("__init__.py", content)
        self.file_system.write_file(self.paths.src / "__init__.py", content)
        self.file_system.write_file(self.paths.db / "__init__.py", content)

    def _generate_database_file(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_database_template()
        self.file_system.write_file_cache(self.paths.db / "database.py", content)

    def _generate_database_utils_file(self):
        """Generate utils.py for db"""
        content = self.template_renderer.render_database_utils_template()
        self.file_system.write_file_cache(self.paths.db / "utils.py", content)

    def _generate_schema_documentation(self):
        """Generate a compact schema documentation file"""
        cache_dir = Path("./cache")
        cache_dir.mkdir(exist_ok=True)

        lines = ["# FAO Database Schema\n"]
        lines.append(f"# Generated from {len(self.all_modules)} tables\n")
        lines.append("# Format: column_name: type (nullable?) [FK->table] [idx]\n")
        lines.append("#" + "=" * 60 + "\n")

        # Group by lookup vs dataset for organization
        lookups = [m for m in self.all_modules if m["is_lookup_module"]]
        datasets = [m for m in self.all_modules if not m["is_lookup_module"]]

        # Document lookups first
        if lookups:
            lines.append("\n## LOOKUP TABLES\n")
            for module in sorted(lookups, key=lambda x: x["model"]["table_name"]):
                lines.extend(self._format_table_schema(module))

        # Then datasets
        if datasets:
            lines.append("\n## DATASET TABLES\n")
            for module in sorted(datasets, key=lambda x: x["model"]["table_name"]):
                lines.extend(self._format_table_schema(module))

        # Write to file
        schema_file = cache_dir / "db_schema.txt"
        schema_file.write_text("\n".join(lines))
        logger.info(f"📋 Generated database schema documentation: {schema_file}")

    def _format_table_schema(self, module) -> List[str]:
        """Format a single table's schema"""
        lines = []
        model = module["model"]

        # Table header
        lines.append(f"\n{model['table_name']}")
        lines.append("-" * len(model["table_name"]))

        # Always include id column first
        lines.append("  id: Integer (PK)")

        # Foreign key columns
        for fk in model.get("foreign_keys", []):
            fk_notation = f"[FK->{fk['table_name']}]"
            lines.append(f"  {fk['hash_fk_sql_column_name']}: Integer {fk_notation}")

        # Regular columns
        for col in model["column_analysis"]:
            if col["csv_column_name"] not in model.get("exclude_columns", []):
                col_line = f"  {col['sql_column_name']}: {col['inferred_sql_type']}"

                # Add size if specified
                if col.get("sql_type_size"):
                    col_line = f"  {col['sql_column_name']}: {col['inferred_sql_type']}({col['sql_type_size']})"

                # Add nullable indicator
                if col.get("nullable", True):
                    col_line += " (null)"

                # Add index indicators
                if col.get("index"):
                    col_line += " [idx]"
                if col.get("original_pk"):  # For lookups
                    col_line += " [uniq]"

                lines.append(col_line)

        # System columns
        lines.append("  created_at: DateTime")
        lines.append("  updated_at: DateTime")

        return lines
