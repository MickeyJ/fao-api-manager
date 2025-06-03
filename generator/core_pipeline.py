from .structure import Structure


class CorePipeline:
    def __init__(self, all_zip_info, structure: Structure, pipeline_spec: dict):
        self.all_zip_info = all_zip_info
        self.structure = structure
        self.modules = []
        self.pipeline_spec = pipeline_spec
        self._collect_core_files()

    def _collect_core_files(self) -> None:
        """Collect all core CSV files from all zips"""
        found_core_modules = set()

        for zip_info in self.all_zip_info:
            for csv_file in zip_info["csv_files"]:

                if self.structure.is_core_module(csv_file, self.pipeline_spec):
                    core_module_name = self.structure.extract_module_name(csv_file)
                    if core_module_name and core_module_name not in found_core_modules:
                        print(f"Found core module: {core_module_name} in {zip_info['zip_path']}")
                        found_core_modules.add(core_module_name)
                        zip_path = zip_info["zip_path"]
                        file_info = {
                            "csv_filename": csv_file,
                            "zip_path": zip_path,
                            "csv_files": [
                                self.structure.cache_key_to_csv_path(key)
                                for key in self.pipeline_spec["core_file_info"][core_module_name]["cache_keys"]
                            ],
                        }
                        module_spec = self.structure.build_module_spec(
                            core_module_name=core_module_name,
                            file_info=file_info,
                            pipeline_name="core",
                            pipeline_spec=self.pipeline_spec,
                            spec=self.pipeline_spec["core_file_info"].get(core_module_name, {}),
                        )
                        self.modules.append(module_spec)
