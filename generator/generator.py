from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from typing import Dict, List
import pandas as pd
from rich import print as rprint
import re, json
from . import ZIP_PATH
from .scanner import FAOZipScanner
from .csv_analyzer import CSVAnalyzer


class PipelineGenerator:

    analyzer = CSVAnalyzer()  # Create analyzer instance
    exclude_modules = []
    template_file_names = {
        "init": "__init__.py.jinja2",
        "main": "__main__.py.jinja2",
        "module": "module.py.jinja2",
    }

    def __init__(self, output_dir: str = "./db/pipelines"):
        self.output_dir = Path(output_dir)
        self.template_dir = Path("templates")

        # Set up Jinja2 with .jinja2 extension
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def generate_all_pipelines(self, all_zip_info: List[Dict]) -> None:
        """Generate core pipeline first, then individual pipelines"""
        # Generate core tables pipeline first
        self.generate_core_pipeline(all_zip_info)

        # Generate individual pipelines
        for zip_info in all_zip_info:
            modules = self._determine_modules(zip_info["csv_files"])
            if modules:  # Only generate if there are non-core modules
                self.generate_pipeline(zip_info)

    def generate_core_pipeline(self, all_zip_info: List[Dict]) -> None:
        """Generate a single pipeline for core/shared tables"""
        # print("\n🔨 Generating core tables pipeline...") # LOG

        core_dir = self.output_dir / "fao_core_tables"
        core_dir.mkdir(parents=True, exist_ok=True)

        # Collect all core CSV files from all zips
        core_files = {}
        for zip_info in all_zip_info:
            for csv_file in zip_info["csv_files"]:
                if self._is_core_table(csv_file):
                    # Store both filename and zip path
                    if "areacodes" in csv_file.lower():
                        core_files["areas"] = {
                            "csv_filename": csv_file,
                            "zip_path": zip_info["zip_path"],
                        }
                    elif "itemcodes" in csv_file.lower():
                        core_files["items"] = {
                            "csv_filename": csv_file,
                            "zip_path": zip_info["zip_path"],
                        }
                    elif "currenc" in csv_file.lower():
                        core_files["currencies"] = {
                            "csv_filename": csv_file,
                            "zip_path": zip_info["zip_path"],
                        }

        if not core_files:
            print("  ⚠️  No core files found")
            return

        # Generate core pipeline files
        self._generate_init_file(
            core_dir,
            {
                "zip_name": "core_tables",
                "csv_files": [
                    file_info["csv_filename"] for file_info in core_files.values()
                ],
            },
        )
        self._generate_main_file(core_dir, "fao_core_tables", core_files)

        # Generate module files for core tables
        template = self.jinja_env.get_template(self.template_file_names["module"])
        for module_name, file_info in core_files.items():

            # Analyze the CSV file
            csv_analysis = self.analyzer.analyze_csv_from_zip(
                file_info["zip_path"], file_info["csv_filename"]
            )
            model_name = self._snake_to_pascal(self._guess_table_name(module_name))
            content = template.render(
                csv_filename=file_info["csv_filename"],
                model_name=model_name,
                table_name=module_name,
            )

            module_file = core_dir / f"{module_name}.py"
            module_file.write_text(content, encoding="utf-8")

            json_file = core_dir / f"{module_name}.json"
            json_file.write_text(json.dumps(csv_analysis, indent=2), encoding="utf-8")
            # print(f"  📄 Generated core {module_name}.py") # LOG

        print(f"✅ Generated {len(core_files)} core modules")

    def generate_pipeline(self, zip_info: Dict) -> None:
        """Generate a complete pipeline from zip info"""
        pipeline_name = zip_info["suggested_pipeline_name"]
        pipeline_dir = self.output_dir / pipeline_name

        # print(f"\n🔨 Generating pipeline: {pipeline_name}") # LOG

        # Create directory structure
        pipeline_dir.mkdir(parents=True, exist_ok=True)

        # Analyze CSV files to determine modules needed (excluding core)
        modules = self._determine_modules(zip_info["csv_files"])

        if not modules:
            print("  ⚠️  No non-core modules found, skipping")
            return

        # Generate files
        self._generate_init_file(pipeline_dir, zip_info)
        self._generate_main_file(pipeline_dir, pipeline_name, modules)
        self._generate_module_files(pipeline_dir, zip_info, modules)

        print(f"✅ Generated {len(modules)} modules in {pipeline_dir}")

    def _is_core_table(self, csv_filename: str) -> bool:
        """Check if this is a core/shared table"""
        lower_file = csv_filename.lower()
        core_patterns = ["areacodes", "itemcodes", "currencys", "currencies"]
        return any(pattern in lower_file for pattern in core_patterns)

    def _determine_modules(self, csv_files: List[str]) -> Dict[str, str]:
        """Map CSV files to Python module names automatically, excluding core tables"""
        modules = {}

        for csv_file in csv_files:
            # Skip core tables - they go in the core pipeline
            if self._is_core_table(csv_file):
                continue

            # Extract module name from filename automatically
            module_name = self._extract_module_name(csv_file)
            if module_name:
                modules[module_name] = csv_file

        return modules

    def _extract_module_name(self, csv_filename: str) -> str:
        """Automatically extract module name from CSV filename - handles all FAO patterns"""
        # Remove .csv extension
        name = csv_filename.replace(".csv", "")

        # Handle different language versions (_E_ or _F_) and variations
        patterns = [
            r"(.+)_([EF])_All_Data_?\(Normalized\)",  # Standard pattern
            r"(.+)([EF])_All_Data\(Normalized\)",  # Missing underscore variant
            r"(.+)_([EF])_(.+)",  # Other suffixes like Elements, Flags
        ]

        for pattern in patterns:
            match = re.match(pattern, name)
            if match:
                base_name = match.group(1)
                if len(match.groups()) >= 3:
                    suffix = match.group(3)
                    # Handle specific suffixes
                    if suffix in ["Elements", "Flags", "AreaCodes", "ItemCodes"]:
                        return suffix.lower()
                    else:
                        return self._to_snake_case(suffix)
                else:
                    # Main data file
                    return self._to_snake_case(base_name)

        # Fallback - convert entire filename
        return self._to_snake_case(name)

    def _to_snake_case(self, text: str) -> str:
        """Convert text to snake_case"""
        # Handle camelCase and PascalCase
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
        # Clean up and convert to lowercase
        result = s2.replace("_", "_").replace("-", "_").lower()
        # Remove multiple underscores
        result = re.sub("_+", "_", result)
        return result.strip("_")

    def _generate_init_file(self, pipeline_dir: Path, zip_info: Dict) -> None:
        """Generate __init__.py file"""
        template = self.jinja_env.get_template(self.template_file_names["init"])

        # Extract directory name from zip
        zip_name = zip_info["zip_name"]
        directory_name = zip_name.replace(".zip", "")

        # Check if this pipeline needs currency standardization
        has_currency = any(
            "currenc" in f.lower() or "exchange" in f.lower()
            for f in zip_info["csv_files"]
        )

        content = template.render(
            directory_name=directory_name, has_currency_standardization=has_currency
        )

        (pipeline_dir / "__init__.py").write_text(content, encoding="utf-8")

    def _generate_main_file(
        self, pipeline_dir: Path, pipeline_name: str, modules: Dict
    ) -> None:
        """Generate __main__.py file"""
        template = self.jinja_env.get_template(self.template_file_names["main"])

        # Exclude utility modules from main execution
        execution_modules = [
            name for name in modules.keys() if name not in self.exclude_modules
        ]

        content = template.render(
            pipeline_name=pipeline_name, modules=execution_modules
        )

        (pipeline_dir / "__main__.py").write_text(content, encoding="utf-8")

    def _generate_module_files(
        self, pipeline_dir: Path, zip_info: Dict, modules: Dict
    ) -> None:
        """Generate individual module files (items.py, areas.py, etc.)"""
        template = self.jinja_env.get_template(self.template_file_names["module"])

        for module_name, csv_filename in modules.items():
            # Skip utility modules for now
            if module_name in self.exclude_modules:
                continue

            # Determine model name and table name
            table_name = self._guess_table_name(module_name, pipeline_dir.name)
            model_name = self._snake_to_pascal(table_name)

            # Analyze the CSV file
            csv_analysis = self.analyzer.analyze_csv_from_zip(
                zip_info["zip_path"], csv_filename
            )

            content = template.render(
                csv_filename=csv_filename,
                model_name=model_name,
                table_name=table_name,
                csv_analysis=csv_analysis,
            )

            module_file = pipeline_dir / f"{module_name}.py"
            module_file.write_text(content, encoding="utf-8")

            json_file = pipeline_dir / f"{module_name}.json"
            json_file.write_text(json.dumps(csv_analysis, indent=2), encoding="utf-8")
            # print(f"  📄 Generated {module_name}.py") # LOG

    def _snake_to_pascal(self, snake_str):
        """Convert snake_case to PascalCase"""
        return "".join(word.capitalize() for word in snake_str.split("_"))

    def _guess_model_name(self, csv_filename: str) -> str:
        """Extract model name from CSV filename"""
        # Remove .csv extension
        name = csv_filename.replace(".csv", "")

        # Split on _E_ to get the base name and suffix
        if "_E_" in name:
            base_name = name.split("_E_")[0]
            suffix = name.split("_E_")[1]

            # If it's the main data file, just use base name
            if suffix == "All_Data_(Normalized)":
                return base_name
            else:
                # For Elements, Flags, AreaCodes, etc., append to base name
                return base_name + suffix.replace("_", "")

        # Fallback if pattern doesn't match
        return name.replace("_", "")

    def _guess_table_name(self, module_name: str, pipeline_name: str = None) -> str:
        """Convert module name to likely table name"""
        if module_name in ["elements", "flags"] and pipeline_name:
            # Strip 'fao_' prefix from pipeline name
            base_name = pipeline_name.replace("fao_", "")
            return f"{base_name}_{module_name}"

        return module_name.lower()


# Usage
if __name__ == "__main__":

    scanner = FAOZipScanner(ZIP_PATH)
    generator = PipelineGenerator()

    results = scanner.scan_all_zips()
    print(f"Found {len(results)} ZIP files")

    # Generate ALL pipelines including core
    generator.generate_all_pipelines(results)
