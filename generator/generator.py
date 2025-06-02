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

# from optimizer.pattern_discovery_old import PatternDiscoveryOld
from optimizer.pipeline_specs import PipelineSpecs


@dataclass
class ProjectPath:
    project: str | Path
    src: Path = Path("src")
    db: Path = Path("src/db")
    db_models: Path = Path("src/db/models")
    db_pipelines: Path = Path("src/db/pipelines")
    api: Path = Path("src/api")
    api_routers: Path = Path("src/api/routers")


class Generator:
    def __init__(self, output_dir: str, input_dir: str):
        self.paths = ProjectPath(Path(output_dir))
        self.input_dir = input_dir
        # self.output_dir = output_dir
        # self.database_dir = f"{output_dir}/db"
        # self.pipelines_dir = f"{output_dir}/{self.database_dir}/pipelines"
        # self.api_dir = f"{output_dir}/api"
        self.structure = Structure()
        self.file_generator = FileGenerator(self.paths.project)
        self.scanner = Scanner(self.input_dir)
        self.csv_analyzer = CSVAnalyzer(
            self.structure, self.scanner, self.file_generator
        )
        self.template_renderer = TemplateRenderer()

        self.file_generator.create_dir(self.paths.src)
        self.file_generator.create_dir(self.paths.db)
        self.file_generator.create_dir(self.paths.db_models)
        self.file_generator.create_dir(self.paths.db_pipelines)
        self.file_generator.create_dir(self.paths.api)
        self.file_generator.create_dir(self.paths.api_routers)

        self.all_zip_info = self.scanner.scan_all_zips()

        # Will hold all modules from all pipelines
        self.all_modules = []
        self.pipelines = {}

    def generate(self) -> None:
        """Main generation workflow"""
        # Step 1: Discover all modules
        self._discover_modules(self.all_zip_info)

        # Step 2: Analyze CSV files in all modules
        self._analyze_modules()

        # Step 3
        self._generate_files()

    def _discover_modules(self, all_zip_info: List[Dict]):
        """Discover modules from all pipelines"""
        # Core modules
        self.pipeline_specs = PipelineSpecs(self.input_dir).create()

        core_pipeline = CorePipeline(all_zip_info, self.structure, self.pipeline_specs)
        self.all_modules.extend(core_pipeline.modules)

        # All other dataset modules
        dataset_pipelines = DatasetPipelines(
            all_zip_info, self.structure, self.pipeline_specs
        )
        self.all_modules.extend(dataset_pipelines.modules)

    def _analyze_modules(self):
        """Add CSV analysis to all modules"""
        for module in self.all_modules:
            file_info = module["file_info"]
            csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                file_info["zip_path"], file_info["csv_filename"]
            )
            module["csv_analysis"] = csv_analysis

        # print(
        #     f"Analyzed {module['file_info']['csv_filename']} - column_count: {csv_analysis['column_count']}"
        # )

    def _generate_files(self):
        """Generate all pipeline and model files"""
        self._generate_project_files()
        self._group_modules_by_pipeline()

        for pipeline_name, modules in self.pipelines.items():
            self._generate_pipeline_and_models(pipeline_name, modules)
        self._generate_all_pipelines_main()
        self._generate_models_init()

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
        self.file_generator.create_dir(self.paths.db_models / pipeline_name)

        # Generate pipeline files
        self._generate_pipeline_init(pipeline_name)
        self._generate_pipeline_main(pipeline_name, modules)
        self._generate_modules_and_models(
            self.paths.db_pipelines / pipeline_name,
            self.paths.db_models / pipeline_name,
            modules,
        )

    def _generate_pipeline_init(self, pipeline_name):
        """Generate __init__.py for pipeline"""
        content = self.template_renderer.render_init_template(
            directory_name=pipeline_name
        )
        self.file_generator.write_file(
            self.paths.db_pipelines / pipeline_name / "__init__.py", content
        )

    def _generate_pipeline_main(self, pipeline_name, modules):
        """Generate __main__.py for pipeline"""
        # Deduplicate module names
        module_names = list(set(module["module_name"] for module in modules))

        content = self.template_renderer.render_main_template(
            pipeline_name=pipeline_name, modules=module_names
        )
        self.file_generator.write_file(
            self.paths.db_pipelines / pipeline_name / "__main__.py", content
        )

    def _generate_all_pipelines_main(self):
        """Generate db/pipelines/__main__.py that runs all pipelines"""
        pipeline_names = list(self.pipelines.keys())

        content = self.template_renderer.render_pipelines_main_template(
            pipeline_names=pipeline_names
        )

        self.file_generator.write_file(self.paths.db_pipelines / "__main__.py", content)

    def _generate_models_init(self):
        """Generate db/models/__init__.py with all model imports"""
        imports = []
        for pipeline_name, modules in self.pipelines.items():
            for module in modules:
                # print(f"Adding import for {module['model_name']} from {pipeline_name}")
                import_line = f"from .{pipeline_name}.{module['module_name']} import {module['model_name']}"
                imports.append(import_line)

        content = "\n".join(sorted(imports))
        self.file_generator.create_dir(self.paths.db_models)
        self.file_generator.write_file(self.paths.db_models / "__init__.py", content)

    def _generate_modules_and_models(self, pipeline_dir, model_dir, modules):
        """Generate both pipeline modules and model files"""
        for module in modules:
            module_name = module["module_name"]

            # Extract the directory name from the ZIP name
            zip_name = module["file_info"]["zip_path"].stem  # removes .zip
            full_csv_path = f"{zip_name}/{module['file_info']['csv_filename']}"

            pipeline_content = self.template_renderer.render_module_template(
                csv_files=[full_csv_path],  # Pass full path here
                model_name=module["model_name"],
                table_name=module["table_name"],
                csv_analysis=module["csv_analysis"],
            )

            self.file_generator.write_file(
                pipeline_dir / f"{module_name}.py", pipeline_content
            )

            # Generate model file (areas.py in model_dir)
            model_content = self.template_renderer.render_model_template(
                model_name=module["model_name"],
                table_name=module["table_name"],
                csv_analysis=module["csv_analysis"],
            )
            self.file_generator.write_file(
                model_dir / f"{module_name}.py", model_content
            )

            # Generate analysis JSON
            self.file_generator.write_json_file(
                self.paths.project / pipeline_dir / f"{module_name}.json",
                module["csv_analysis"],
            )

    def _generate_project_files(self):
        self._generate_env_files()
        self._generate_makefile()
        self._generate_requirements_file()
        self._generate_database_file()
        self._generate_database_utils_file()

    def _generate_env_files(self):
        """Generate database.py for db"""
        templates = self.template_renderer.render_env_templates()

        for template in templates:
            self.file_generator.write_file(
                self.paths.src / template["file_name"], template["content"]
            )

    def _generate_makefile(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_makefile_template()
        self.file_generator.write_file(self.paths.src / "Makefile", content)

    def _generate_requirements_file(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_requirements_template()
        self.file_generator.write_file(self.paths.src / "requirements.in", content)

    def _generate_database_file(self):
        """Generate database.py for db"""
        content = self.template_renderer.render_database_template()
        self.file_generator.write_file(self.paths.db / "database.py", content)

    def _generate_database_utils_file(self):
        """Generate utils.py for db"""
        content = self.template_renderer.render_database_utils_template()
        self.file_generator.write_file(self.paths.db / "utils.py", content)
