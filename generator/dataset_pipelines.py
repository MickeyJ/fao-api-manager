from .structure import Structure


class DatasetPipelines:
    def __init__(self, all_zip_info, structure: Structure, pipeline_spec: dict):
        self.all_zip_info = all_zip_info
        self.structure = structure
        self.modules = []
        self.pipeline_spec = pipeline_spec
        self._collect_dataset_files()

    def _collect_dataset_files(self):
        """Collect all dataset CSV files and build module specs"""
        for zip_info in self.all_zip_info:
            pipeline_name = zip_info["pipeline_name"]

            for csv_file in zip_info["csv_files"]:
                if not self.structure.is_core_module(csv_file, self.pipeline_spec):  # Skip core files
                    module_name = self.structure.extract_module_name(csv_file)
                    if module_name:
                        zip_path = zip_info["zip_path"]
                        file_info = {
                            "csv_filename": csv_file,
                            "zip_path": zip_path,
                            "csv_files": [f"{zip_path.stem}/{csv_file}"],
                        }
                        module_spec = self.structure.build_module_spec(
                            core_module_name=module_name,
                            file_info=file_info,
                            pipeline_name=pipeline_name,
                            pipeline_spec=self.pipeline_spec,
                            spec=self.pipeline_spec["dataset_file_info"].get(module_name, {}),
                        )
                        self.modules.append(module_spec)
