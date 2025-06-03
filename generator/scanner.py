"""Scanner for FAO ZIP files
This module scans a directory for ZIP files containing FAO data, extracts their contents,
and formats the information for further processing."""

import zipfile
from pathlib import Path
from typing import List, Dict
from .structure import Structure
from .file_generator import FileGenerator
from . import ZIP_PATH, to_snake_case


class Scanner:
    """Scanner for FAO ZIP files in a specified directory"""

    def __init__(self, zip_directory: str, structure: Structure):
        self.zip_dir = Path(zip_directory)
        self.structure = structure

    def scan_all_zips(self) -> List[Dict]:
        """Scan all ZIP files and return their contents"""
        results = []

        for zip_path in self.zip_dir.glob("*.zip"):
            if self.is_zip(zip_path):
                info = self._analyze_zip(zip_path)
                results.append(info)

        return results

    def is_zip(self, zip_path: Path) -> bool:
        """Check if this looks like an FAO data zip"""
        name = zip_path.name.lower()
        return "_e_all_data" in name or "_f_all_data" in name or "faostat" in name

    def _analyze_zip(self, zip_path: Path) -> Dict:
        """Extract info about a single ZIP file"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]

        return {
            "zip_name": zip_path.name,
            "zip_path": zip_path,
            "csv_files": csv_files,
            "pipeline_name": self._format_pipeline_name(zip_path.name),
        }

    def _format_pipeline_name(self, zip_name: str) -> str:
        """Convert ZIP name to pipeline directory name"""
        # Remove .zip and common suffixes
        name = self.structure.extract_module_name(zip_name)
        name = name.replace(".zip", "")
        name = name.replace("_e_all_data_normalized", "")
        name = name.replace("_f_all_data_normalized", "")

        # Convert CamelCase to snake_case
        name = to_snake_case(name)
        return f"{name}"
