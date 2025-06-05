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

    def __init__(self, zip_directory: str | Path, structure: Structure):
        self.input_dir = Path(zip_directory)
        self.structure = structure

        self.verify_dataset_directories()

    # In generator/scanner.py, add this method to the Scanner class:

    def verify_dataset_directories(self) -> None:
        """Verify that all dataset directories exist and create them if not"""
        found_zips = list(self.input_dir.glob("*.zip"))
        found_directories = [d for d in self.input_dir.iterdir() if d.is_dir() and self.is_fao_dataset(d)]

        if len(found_zips) != len(found_directories):
            raise Exception(
                f"Missing zips or directories: {len(found_zips)} zips, {len(found_directories)} directories"
            )
        else:
            zip_dir_mismatches = []
            for zip_path in found_zips:
                if zip_path.stem not in [d.name for d in found_directories]:
                    zip_dir_mismatches.append(zip_path.stem)

            if zip_dir_mismatches:
                raise Exception(f"ZIP files do not match directories: {', '.join(zip_dir_mismatches)}")

    def create_extraction_manifest(self, all_zip_info: List[Dict]) -> Dict:
        """Create manifest of ZIP files that need to be extracted for pipeline execution"""
        import time

        manifest = {"created_at": time.time(), "extraction_root": str(self.input_dir), "extractions": []}

        for zip_info in all_zip_info:
            zip_path = zip_info["zip_path"]
            extract_dir = self.input_dir / zip_path.stem  # Remove .zip extension

            manifest["extractions"].append(
                {
                    "zip_file": zip_path.name,
                    "zip_path": str(zip_path),
                    "extract_dir": str(extract_dir),
                    "csv_files": zip_info["csv_files"],
                }
            )

        return manifest

    def scan_all_zips(self) -> List[Dict]:
        """Scan all ZIP files and return their contents"""
        results = []

        for zip_path in self.input_dir.glob("*.zip"):
            if self.is_fao_dataset(zip_path):
                info = self._analyze_zip(zip_path)
                results.append(info)

        return results

    def is_fao_dataset(self, zip_path: Path) -> bool:
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
