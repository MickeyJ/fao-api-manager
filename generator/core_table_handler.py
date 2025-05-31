from typing import Dict, List, Optional
from pathlib import Path


class CoreTableHandler:
    def __init__(self, file_generator, template_renderer, structure):
        self.file_generator = file_generator
        self.template_renderer = template_renderer
        self.structure = structure

    def collect_core_files(self, all_zip_info: List[Dict]) -> Dict[str, Dict]:
        """Collect all core CSV files from all zips"""
        core_files = {}

        for zip_info in all_zip_info:
            for csv_file in zip_info["csv_files"]:
                if self.structure.is_core_table(csv_file):
                    # Store both filename and zip path
                    core_type = self._categorize_core_file(csv_file)
                    if core_type:
                        core_files[core_type] = {
                            "csv_filename": csv_file,
                            "zip_path": zip_info["zip_path"],
                        }

        return core_files

    def generate_core_pipeline(self, all_zip_info: List[Dict], csv_analyzer) -> None:
        """Generate a single pipeline for core/shared tables"""
        core_files = self.collect_core_files(all_zip_info)

        if not core_files:
            print("  ⚠️  No core files found")
            return

        # Create core pipeline directory
        core_dir = self.file_generator.create_pipeline_directory("core_tables")

        # Generate core pipeline files
        self._generate_core_init_file(core_dir, core_files)
        self._generate_core_main_file(core_dir, core_files)
        self._generate_core_modules(core_dir, core_files, csv_analyzer)

        print(f"✅ Generated {len(core_files)} core modules")

    def _categorize_core_file(self, csv_filename: str) -> Optional[str]:
        """Determine what type of core file this is"""
        lower_file = csv_filename.lower()

        if "areacodes" in lower_file:
            return "areas"
        elif "itemcodes" in lower_file:
            return "items"
        elif "currencys" in lower_file:
            return "currencys"

        return None

    def _generate_core_init_file(self, core_dir: Path, core_files: Dict) -> None:
        """Generate __init__.py for core pipeline"""

        content = self.template_renderer.render_init_template(
            directory_name="core_tables"
        )

        self.file_generator.write_file(core_dir / "__init__.py", content)

    def _generate_core_main_file(self, core_dir: Path, core_files: Dict) -> None:
        """Generate __main__.py for core pipeline"""
        modules = list(core_files.keys())

        content = self.template_renderer.render_main_template(
            pipeline_name="core_tables", modules=modules
        )

        self.file_generator.write_file(core_dir / "__main__.py", content)

    def _generate_core_modules(
        self, core_dir: Path, core_files: Dict, csv_analyzer
    ) -> None:
        """Generate individual module files for core tables"""
        for module_name, file_info in core_files.items():
            # Analyze the CSV file
            csv_analysis = csv_analyzer.analyze_csv_from_zip(
                file_info["zip_path"], file_info["csv_filename"]
            )

            model_name = self.structure.format_db_model_name(module_name)

            # Render module content
            module_content = self.template_renderer.render_module_template(
                csv_filename=file_info["csv_filename"],
                model_name=model_name,
                table_name=module_name,
                csv_analysis=csv_analysis,
            )

            # # Write module file
            # self.file_generator.write_file(
            #     core_dir / f"{module_name}.py", module_content
            # )

            # Write analysis JSON
            self.file_generator.write_json_file(
                core_dir / f"{module_name}.json", csv_analysis
            )
