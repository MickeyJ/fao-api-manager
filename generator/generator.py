from typing import List, Dict
from .structure import Structure
from .core_pipeline import CorePipeline
from .dataset_pipelines import DatasetPipelines
from .csv_analyzer import CSVAnalyzer
from .file_generator import FileGenerator
from .template_renderer import TemplateRenderer


class Generator:
    def __init__(self, output_dir: str = "./db"):
        self.output_dir = output_dir
        self.structure = Structure()
        self.csv_analyzer = CSVAnalyzer()
        self.file_generator = FileGenerator(output_dir)
        self.template_renderer = TemplateRenderer()

        # Will hold all modules from all pipelines
        self.all_modules = []
        self.pipelines = {}

    def generate(self, all_zip_info: List[Dict]):
        """Main generation workflow"""
        # Step 1: Discover all modules
        self._discover_modules(all_zip_info)

        # Step 2: Analyze CSV files in all modules
        self._analyze_modules()

        # Step 3
        self._generate_files()

    def _discover_modules(self, all_zip_info: List[Dict]):
        """Discover modules from all pipelines"""
        # Core modules
        core_pipeline = CorePipeline(all_zip_info, self.structure)
        self.all_modules.extend(core_pipeline.modules)

        # All other dataset modules
        dataset_pipelines = DatasetPipelines(all_zip_info, self.structure)
        self.all_modules.extend(dataset_pipelines.modules)

    def _analyze_modules(self):
        """Add CSV analysis to all modules"""
        for module in self.all_modules:
            file_info = module["file_info"]
            csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                file_info["zip_path"], file_info["csv_filename"]
            )
            module["csv_analysis"] = csv_analysis
            print(
                f"Analyzed {module['file_info']['csv_filename']} - column_count: {csv_analysis['column_count']}"
            )

    def _generate_files(self):
        """Generate all pipeline and model files"""
        # Group modules by pipeline
        self._group_modules_by_pipeline()

        self._generate_all_pipelines_main()
        self._generate_models_init()
        for pipeline_name, modules in self.pipelines.items():
            self._generate_pipeline_and_models(pipeline_name, modules)

    def _group_modules_by_pipeline(self) -> None:
        """Group modules by their pipeline name"""
        for module in self.all_modules:
            pipeline_name = module["pipeline_name"]

            if pipeline_name not in self.pipelines:
                self.pipelines[pipeline_name] = []
            self.pipelines[pipeline_name].append(module)

    def _generate_pipeline_and_models(self, pipeline_name: str, modules: List[Dict]):
        """Generate files for a single pipeline"""
        # Create pipeline directory
        pipeline_dir = self.file_generator.create_pipeline_directory(pipeline_name)
        model_dir = self.file_generator.create_models_directory(pipeline_name)

        # Generate pipeline files
        self._generate_pipeline_init(pipeline_dir, pipeline_name)
        self._generate_pipeline_main(pipeline_dir, pipeline_name, modules)
        self._generate_modules_and_models(pipeline_dir, model_dir, modules)

    def _generate_pipeline_init(self, pipeline_dir, pipeline_name):
        """Generate __init__.py for pipeline"""
        content = self.template_renderer.render_init_template(
            directory_name=pipeline_name
        )
        self.file_generator.write_file(pipeline_dir / "__init__.py", content)

    def _generate_pipeline_main(self, pipeline_dir, pipeline_name, modules):
        """Generate __main__.py for pipeline"""
        # Deduplicate module names
        module_names = list(set(module["module_name"] for module in modules))

        content = self.template_renderer.render_main_template(
            pipeline_name=pipeline_name, modules=module_names
        )
        self.file_generator.write_file(pipeline_dir / "__main__.py", content)

    def _generate_all_pipelines_main(self):
        """Generate db/pipelines/__main__.py that runs all pipelines"""
        pipeline_names = list(self.pipelines.keys())

        content = self.template_renderer.render_pipelines_main_template(
            pipeline_names=pipeline_names
        )

        pipelines_dir = self.file_generator.pipelines_dir
        self.file_generator.write_file(pipelines_dir / "__main__.py", content)

    def _generate_models_init(self):
        """Generate db/models/__init__.py with all model imports"""
        imports = []
        for pipeline_name, modules in self.pipelines.items():
            for module in modules:
                # print(f"Adding import for {module['model_name']} from {pipeline_name}")
                import_line = f"from .{pipeline_name}.{module['module_name']} import {module['model_name']}"
                imports.append(import_line)

        content = "\n".join(sorted(imports))
        models_dir = self.file_generator.create_models_directory()
        self.file_generator.write_file(models_dir / "__init__.py", content)

    def _generate_modules_and_models(self, pipeline_dir, model_dir, modules):
        """Generate both pipeline modules and model files"""
        for module in modules:
            module_name = module["module_name"]

            # Generate pipeline module (areas.py in pipeline_dir)
            pipeline_content = self.template_renderer.render_module_template(
                csv_filename=module["file_info"]["csv_filename"],
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
                pipeline_dir / f"{module_name}.json", module["csv_analysis"]
            )
