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

    def __init__(self, zip_directory: str):
        self.zip_dir = Path(zip_directory)

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
        name = zip_name.replace(".zip", "")
        name = name.replace("_E_All_Data_(Normalized)", "")
        name = name.replace("_F_All_Data_(Normalized)", "")

        # Convert CamelCase to snake_case
        name = to_snake_case(name)
        return f"{name}"


if __name__ == "__main__":
    # Test it out
    structure = Structure()
    file_generator = FileGenerator("./db")
    scanner = Scanner(ZIP_PATH)
    # Debug: show ALL zip files in directory
    print("=== ALL ZIP FILES FOUND ===")
    for zip_path in scanner.zip_dir.glob("*.zip"):
        print(f"Found: {zip_path.name}")

    print("\n=== CHECKING FAO FILTER ===")
    for zip_path in scanner.zip_dir.glob("*.zip"):
        is_fao = scanner.is_zip(zip_path)
        print(f"{zip_path.name} -> FAO: {is_fao}")

    print("\n=== FINAL RESULTS ===")
    results = scanner.scan_all_zips()
    for result in results:
        print(f"\nZIP: {result['zip_name']}")
        print(f"Pipeline name: {result['pipeline_name']}")
        print(f"CSV files: {result['csv_files']}")
