import re
from typing import Dict, List, Optional
from . import to_snake_case, snake_to_pascal_case


class Structure:
    def __init__(self, exclude_modules: Optional[List[str]] = None):
        """Initialize the pipeline structure with optional excluded modules."""
        self.exclude_modules = exclude_modules or []

    def determine_modules(self, csv_files: List[str]) -> Dict[str, str]:
        """Map CSV files to Python module names automatically, excluding core tables"""
        modules = {}

        for csv_file in csv_files:
            # Skip core tables - they go in the core pipeline
            if self.is_core_table(csv_file):
                continue

            # Extract module name from filename automatically
            module_name = self.extract_module_name(csv_file)
            if module_name:
                modules[module_name] = csv_file

        return modules

    def extract_module_name(self, csv_filename: str) -> str:
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
                        return to_snake_case(suffix)
                else:
                    # Main data file
                    return to_snake_case(base_name)

        # Fallback - convert entire filename
        return to_snake_case(name)

    def is_core_table(self, csv_filename: str) -> bool:
        """Check if this is a core/shared table"""
        lower_file = csv_filename.lower()
        core_patterns = ["areacodes", "itemcodes", "currencys"]
        return any(pattern in lower_file for pattern in core_patterns)

    def format_module_name(
        self, module_name: str, pipeline_name: str, csv_filename: str
    ) -> str:
        """Convert module name to likely table name"""
        if self.is_primary_module(csv_filename):
            # Primary module - no prefix needed
            return module_name.lower()

        # Secondary module - add pipeline prefix
        base_name = pipeline_name.replace("fao_", "")
        return f"{base_name}_{module_name}"

    def format_db_model_name(self, module_name: str) -> str:
        """Convert module name to SQLAlchemy model name (PascalCase)"""
        return snake_to_pascal_case(module_name)

    def is_primary_module(self, csv_filename: str) -> bool:
        """Check if this is the main All_Data file"""
        return (
            "all_data" in csv_filename.lower() and "normalized" in csv_filename.lower()
        )
