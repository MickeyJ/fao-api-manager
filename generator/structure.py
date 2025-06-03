import re
from typing import Dict, List, Optional
from . import to_snake_case, snake_to_pascal_case


class Structure:
    def __init__(self, exclude_modules: Optional[List[str]] = None):
        """Initialize the pipeline structure with optional excluded modules."""
        self.exclude_modules = exclude_modules or []

    def build_module_spec(
        self,
        core_module_name: str,
        file_info: Dict,
        pipeline_name: str,
        pipeline_spec: Dict,
        spec: Dict,
    ) -> Dict:
        """Build a complete module specification"""

        # Format names
        module_name = self.format_module_name(
            module_name=core_module_name,
            pipeline_name=pipeline_name,
            csv_filename=file_info["csv_filename"],
            pipeline_spec=pipeline_spec,
        )
        table_name = to_snake_case(module_name)
        model_name = snake_to_pascal_case(table_name)

        return {
            "pipeline_name": pipeline_name,
            "module_name": core_module_name,
            "model_name": model_name,
            "table_name": table_name,
            "file_info": file_info,
            "specs": spec,
        }

    def determine_modules(self, csv_files: List[str], pipeline_spec: Dict) -> Dict[str, str]:
        """Map CSV files to Python module names automatically, excluding core tables"""
        modules = {}

        for csv_file in csv_files:
            # Skip core tables - they go in the core pipeline
            if self.is_core_module(csv_file, pipeline_spec):
                continue

            # Extract module name from filename automatically
            module_name = self.extract_module_name(csv_file)
            if module_name:
                modules[module_name] = csv_file

        return modules

    def extract_module_name(self, csv_filename: str) -> str:
        """Extract module name from CSV filename - enhanced processing for dataset files"""
        base_filename = csv_filename.replace(".csv", "")

        # Check if this is a main dataset file (All_Data + Normalized)
        if "all_data" in base_filename.lower() and "normalized" in base_filename.lower():
            # Enhanced processing for dataset files to preserve distinguishing info
            name = base_filename
            name = re.sub(r"_[EF]_All_Data_?\(Normalized\)$", "", name)  # Remove suffixes
            name = re.sub(r"_[EF]_All_Data\(Normalized\)$", "", name)  # Handle variations

            # Clean up special characters but preserve meaningful content
            name = re.sub(r"[()]", "", name)  # Remove parentheses
            name = re.sub(r"-+", "_", name)  # Replace hyphens with underscores
            name = re.sub(r"_+", "_", name)  # Collapse multiple underscores
            name = name.strip("_")  # Remove leading/trailing underscores

            return to_snake_case(name)
        else:
            # Handle different language versions (_E_ or _F_) and variations
            patterns = [
                r"(.+)_([EF])_All_Data_?\(Normalized\)",  # Standard pattern
                r"(.+)([EF])_All_Data\(Normalized\)",  # Missing underscore variant
                r"(.+)_([EF])_(.+)",  # Other suffixes like Elements, Flags
            ]

            for pattern in patterns:
                match = re.match(pattern, base_filename)
                if match:
                    if len(match.groups()) >= 3:
                        suffix = match.group(3)  # Other data files
                        # print(f"Match suffix: {suffix}")
                        return to_snake_case(suffix)
                    else:
                        base_name = match.group(1)  # Main data file
                        # print(f"base_name: {base_name}")
                        return to_snake_case(base_name)

            # Fallback - convert entire filename
            return to_snake_case(base_filename)

    # def extract_module_name(self, csv_filename: str) -> str:
    #     """Automatically extract module name from CSV filename - handles all FAO patterns"""
    #     base_filename = csv_filename.replace(".csv", "")

    #     # Handle different language versions (_E_ or _F_) and variations
    #     patterns = [
    #         r"(.+)_([EF])_All_Data_?\(Normalized\)",  # Standard pattern
    #         r"(.+)([EF])_All_Data\(Normalized\)",  # Missing underscore variant
    #         r"(.+)_([EF])_(.+)",  # Other suffixes like Elements, Flags
    #     ]

    #     for pattern in patterns:
    #         match = re.match(pattern, base_filename)
    #         if match:
    #             if len(match.groups()) >= 3:
    #                 suffix = match.group(3)  # Other data files
    #                 # print(f"Match suffix: {suffix}")
    #                 return to_snake_case(suffix)
    #             else:
    #                 base_name = match.group(1)  # Main data file
    #                 # print(f"base_name: {base_name}")
    #                 return to_snake_case(base_name)

    #     # Fallback - convert entire filename
    #     return to_snake_case(base_filename)

    def is_core_module(self, csv_filename: str, pipeline_spec: Dict) -> bool:
        """Check if this is a core/shared table"""
        lower_file = csv_filename.lower()
        module_name = self.extract_module_name(csv_filename)
        return module_name in pipeline_spec.get("core_file_info", {})

    def format_module_name(
        self,
        module_name: str,
        pipeline_name: str,
        csv_filename: str,
        pipeline_spec: Dict,
    ) -> str:
        """Convert module name to table name"""
        if self.is_primary_module(csv_filename) or self.is_core_module(csv_filename, pipeline_spec):
            # Primary module - no prefix needed
            return module_name.lower()

        # Secondary module - add pipeline name prefix
        base_name = pipeline_name.replace("fao_", "")
        return f"{base_name}_{module_name}"

    def is_primary_module(self, csv_filename: str) -> bool:
        """Check if this is the main All_Data file"""
        return "all_data" in csv_filename.lower() and "normalized" in csv_filename.lower()

    def cache_key_to_csv_path(self, cache_key):
        zip_name, csv_filename = cache_key.split(":", 1)
        zip_name = zip_name.replace(".zip", "")
        return f"{zip_name}/{csv_filename}"
