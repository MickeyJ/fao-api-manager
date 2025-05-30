from typing import List, Dict
from .structure import Structure
from .template_renderer import TemplateRenderer
from .file_generator import FileGenerator
from .core_table_handler import CoreTableHandler


class CodebaseGenerator:
    def __init__(self, output_dir: str = "./db"):
        self.output_dir = output_dir

        # Initialize components
        self.structure = Structure()
        self.template_renderer = TemplateRenderer()
        self.file_generator = FileGenerator(output_dir)
        self.core_handler = CoreTableHandler(
            self.file_generator, self.template_renderer, self.structure
        )

    def generate_all_pipelines(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate core pipeline first, then individual pipelines"""
        # Generate core tables pipeline first
        self.core_handler.generate_core_pipeline(all_zip_info, csv_analyzer)

        # Generate individual pipelines
        for zip_info in all_zip_info:
            modules = self.structure.determine_modules(zip_info["csv_files"])
            if modules:  # Only generate if there are non-core modules
                self.generate_pipeline(zip_info, csv_analyzer)

    def generate_pipeline(self, zip_info: Dict, csv_analyzer) -> None:
        """Generate a single pipeline from zip info"""
        pipeline_name = zip_info["suggested_pipeline_name"]
        modules = self.structure.determine_modules(zip_info["csv_files"])

        if not modules:
            print("  ⚠️  No non-core modules found, skipping")
            return

        # Create pipeline directory
        pipeline_dir = self.file_generator.create_pipeline_directory(pipeline_name)

        # Generate pipeline files
        self._generate_pipeline_init_file(pipeline_dir, zip_info)
        self._generate_pipeline_main_file(pipeline_dir, pipeline_name, modules)
        self._generate_pipeline_module_files(
            pipeline_dir, zip_info, modules, csv_analyzer
        )

        print(f"✅ Generated {len(modules)} modules in {pipeline_dir}")

    def generate_models(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate SQLAlchemy models - to be implemented"""
        # TODO: Implement model generation
        print("🔨 Model generation not yet implemented")

    def generate_all(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate everything: pipelines + models + future stuff"""
        self.generate_all_pipelines(all_zip_info, csv_analyzer)
        self.generate_models(all_zip_info, csv_analyzer)

    def _generate_pipeline_init_file(self, pipeline_dir, zip_info):
        """Generate __init__.py for a pipeline"""
        zip_name = zip_info["zip_name"]
        directory_name = zip_name.replace(".zip", "")

        content = self.template_renderer.render_init_template(
            directory_name=directory_name
        )

        self.file_generator.write_file(pipeline_dir / "__init__.py", content)

    def _generate_pipeline_main_file(self, pipeline_dir, pipeline_name, modules):
        """Generate __main__.py for a pipeline"""
        # Exclude utility modules from main execution
        execution_modules = [
            name
            for name in modules.keys()
            if name not in self.structure.exclude_modules
        ]

        content = self.template_renderer.render_main_template(
            pipeline_name=pipeline_name, modules=execution_modules
        )

        self.file_generator.write_file(pipeline_dir / "__main__.py", content)

    def _generate_pipeline_module_files(
        self, pipeline_dir, zip_info, modules, csv_analyzer
    ):
        """Generate individual module files for a pipeline"""
        for module_name, csv_filename in modules.items():
            # Skip utility modules
            if module_name in self.structure.exclude_modules:
                continue

            # Determine model name and table name
            table_name = self.structure.format_module_name(
                module_name, pipeline_dir.name
            )
            model_name = self.structure.format_db_model_name(table_name)

            # Analyze the CSV file
            csv_analysis = csv_analyzer.analyze_csv_from_zip(
                zip_info["zip_path"], csv_filename
            )

            # Render module content
            content = self.template_renderer.render_module_template(
                csv_filename=csv_filename,
                model_name=model_name,
                table_name=table_name,
                csv_analysis=csv_analysis,
            )

            # Write files
            self.file_generator.write_file(pipeline_dir / f"{module_name}.py", content)
            self.file_generator.write_json_file(
                pipeline_dir / f"{module_name}.json", csv_analysis
            )
