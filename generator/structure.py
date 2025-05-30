import re
from typing import Dict, List, Optional


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
                        return self._to_snake_case(suffix)
                else:
                    # Main data file
                    return self._to_snake_case(base_name)

        # Fallback - convert entire filename
        return self._to_snake_case(name)

    def is_core_table(self, csv_filename: str) -> bool:
        """Check if this is a core/shared table"""
        lower_file = csv_filename.lower()
        core_patterns = ["areacodes", "itemcodes", "currencys"]
        return any(pattern in lower_file for pattern in core_patterns)

    def format_module_name(
        self, module_name: str, pipeline_name: Optional[str] = None
    ) -> str:
        """Convert module name to likely table name"""
        if module_name in ["elements", "flags"] and pipeline_name:
            # Strip '' prefix from pipeline name
            base_name = pipeline_name.replace("", "")
            return f"{base_name}_{module_name}"

        return module_name.lower()

    def format_db_model_name(self, module_name: str) -> str:
        """Convert module name to SQLAlchemy model name (PascalCase)"""
        return self._snake_to_pascal(module_name)

    def _to_snake_case(self, text: str) -> str:
        """Convert text to snake_case"""
        # Remove parentheses and their contents first
        text = re.sub(r"\([^)]*\)", "", text)

        # Handle camelCase and PascalCase
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
        # Clean up and convert to lowercase
        result = s2.replace("_", "_").replace("-", "_").lower()
        # Remove multiple underscores
        result = re.sub("_+", "_", result)
        return result.strip("_")

    def _snake_to_pascal(self, snake_str: str) -> str:
        """Convert snake_case to PascalCase"""
        return "".join(word.capitalize() for word in snake_str.split("_"))
