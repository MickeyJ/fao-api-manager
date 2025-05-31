from typing import List, Dict
from .structure import Structure
from .template_renderer import TemplateRenderer
from .file_generator import FileGenerator
from .core_table_handler import CoreTableHandler


class CodebaseGenerator:
    def __init__(self, output_dir: str = "./db"):
        self.output_dir = output_dir
        self.generated_models = []  # Track all generated models

        # Initialize components
        self.structure = Structure()
        self.template_renderer = TemplateRenderer()
        self.file_generator = FileGenerator(output_dir)
        self.core_handler = CoreTableHandler(
            self.file_generator, self.template_renderer, self.structure
        )

    def generate_all(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate everything: pipelines + models + future stuff"""
        self.generate_all_pipelines(all_zip_info, csv_analyzer)
        self.generate_models(all_zip_info, csv_analyzer)

    def generate_all_pipelines(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate core pipeline first, then individual pipelines"""
        # Generate core tables pipeline first
        self.core_handler.generate_core_pipeline(all_zip_info, csv_analyzer)

        # Generate individual pipelines
        for zip_info in all_zip_info:
            modules = self.structure.determine_modules(zip_info["csv_files"])
            if modules:  # Only generate if there are non-core modules
                self.generate_pipeline(zip_info, csv_analyzer)

    def generate_models(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate SQLAlchemy models from CSV analysis"""
        print("🔨 Generating SQLAlchemy models...")

        # Generate core models first
        core_files = self.core_handler.collect_core_files(all_zip_info)
        if core_files:
            self._generate_core_models(core_files, csv_analyzer)

        # Generate models for each pipeline
        for zip_info in all_zip_info:
            modules = self.structure.determine_modules(zip_info["csv_files"])
            if modules:
                self._generate_pipeline_models(zip_info, modules, csv_analyzer)

        self._generate_models_init_file()
        print("✅ Model generation complete")

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
        self._generate_pipeline_modules(pipeline_dir, zip_info, modules, csv_analyzer)

        print(f"✅ Generated {len(modules)} modules in {pipeline_dir}")

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

    def _generate_pipeline_modules(self, pipeline_dir, zip_info, modules, csv_analyzer):
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

            # Write module file
            self.file_generator.write_file(pipeline_dir / f"{module_name}.py", content)

            # Write analysis JSON
            self.file_generator.write_json_file(
                pipeline_dir / f"{module_name}.json", csv_analysis
            )

    def _generate_models_init_file(self) -> None:
        """Generate db/models/__init__.py with all model imports"""
        imports = []

        for model_info in self.generated_models:
            import_line = f"from .{model_info['pipeline_name']}.{model_info['module_name']} import {model_info['model_name']}"
            imports.append(import_line)

        content = "\n".join(sorted(imports))
        self.file_generator.write_file(
            self.file_generator.models_dir / "__init__.py", content
        )

    def _generate_pipeline_models(
        self, zip_info: Dict, modules: Dict, csv_analyzer
    ) -> None:
        """Generate models for a specific pipeline"""
        pipeline_name = zip_info["suggested_pipeline_name"]
        pipeline_models_dir = self.file_generator.create_models_directory(pipeline_name)

        model_count = 0
        for module_name, csv_filename in modules.items():
            # Skip utility modules
            if module_name in self.structure.exclude_modules:
                continue

            # Analyze CSV and generate model
            csv_analysis = csv_analyzer.analyze_csv_from_zip(
                zip_info["zip_path"], csv_filename
            )

            table_name = self.structure.format_module_name(module_name, pipeline_name)
            model_name = self.structure.format_db_model_name(table_name)

            # Render model content
            model_content = self.template_renderer.render_model_template(
                model_name=model_name, table_name=table_name, csv_analysis=csv_analysis
            )

            # Track what we generated
            self.generated_models.append(
                {
                    "pipeline_name": pipeline_name,
                    "module_name": module_name,
                    "model_name": model_name,
                }
            )

            # Write model file
            self.file_generator.write_file(
                pipeline_models_dir / f"{module_name}.py", model_content
            )
            model_count += 1

        if model_count > 0:
            print(f"✅ Generated {model_count} models for {pipeline_name}")

    def _generate_core_models(self, core_files: Dict, csv_analyzer) -> None:
        """Generate models for core tables (areas, items, currencys)"""
        core_models_dir = self.file_generator.create_models_directory("core")

        for module_name, file_info in core_files.items():
            # Analyze the CSV file
            csv_analysis = csv_analyzer.analyze_csv_from_zip(
                file_info["zip_path"], file_info["csv_filename"]
            )

            model_name = self.structure.format_db_model_name(module_name)

            # Render model content
            model_content = self.template_renderer.render_model_template(
                model_name=model_name, table_name=module_name, csv_analysis=csv_analysis
            )

            self.generated_models.append(
                {
                    "pipeline_name": "core",
                    "module_name": module_name,
                    "model_name": model_name,
                }
            )

            # Write model file
            self.file_generator.write_file(
                core_models_dir / f"{module_name}.py", model_content
            )

        print(f"✅ Generated {len(core_files)} core models")
