from pathlib import Path
from typing import Dict, Optional
import json


class FileGenerator:
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)

    def create_dir(self, dir_path: str | Path) -> Path:
        dir_path = Path(self.output_dir / dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path

    def generate_pipeline_files(
        self, pipeline_dir: Path, rendered_files: Dict[str, str]
    ) -> None:
        """Write multiple rendered files to a pipeline directory"""
        for filename, content in rendered_files.items():
            file_path = pipeline_dir / filename
            self.write_file(file_path, content)

    def write_file(self, file_path: Path | str, content: str) -> None:
        """Write content to a file"""
        file_path = Path(self.output_dir / file_path)
        file_path.write_text(content, encoding="utf-8")

    def write_json_file(self, file_path: Path, data: Dict) -> None:
        """Write dictionary data to a JSON file"""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
