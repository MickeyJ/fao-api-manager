from pathlib import Path
from typing import Dict, Optional
import json


class FileGenerator:
    def __init__(self, output_dir: str = "./db"):
        self.output_dir = Path(output_dir)
        self.models_dir = self.output_dir / "models"
        self.pipelines_dir = self.output_dir / "pipelines"

    def create_pipeline_directory(self, pipeline_name: str) -> Path:
        """Create a pipeline directory and return the Path"""
        pipeline_dir = self.pipelines_dir / pipeline_name
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        return pipeline_dir

    def create_models_directory(self, model_group_name: Optional[str] = None) -> Path:
        """Create model directory and return the Path"""
        if model_group_name:
            model_dir = self.models_dir / model_group_name
        else:
            model_dir = self.models_dir

        model_dir.mkdir(parents=True, exist_ok=True)
        return model_dir

    def generate_pipeline_files(
        self, pipeline_dir: Path, rendered_files: Dict[str, str]
    ) -> None:
        """Write multiple rendered files to a pipeline directory"""
        for filename, content in rendered_files.items():
            file_path = pipeline_dir / filename
            self.write_file(file_path, content)

    def write_file(self, file_path: Path, content: str) -> None:
        """Write content to a file"""
        file_path.write_text(content, encoding="utf-8")

    def write_json_file(self, file_path: Path, data: Dict) -> None:
        """Write dictionary data to a JSON file"""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
