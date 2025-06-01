from .structure import Structure


class CorePipeline:
    def __init__(self, all_zip_info, structure: Structure, pipeline_specs: dict):
        self.all_zip_info = all_zip_info
        self.structure = structure
        self.modules = []
        self.pipeline_specs = pipeline_specs
        self._collect_core_files()

    def _collect_core_files(self) -> None:
        """Collect all core CSV files from all zips"""
        found_core_modules = set()

        for zip_info in self.all_zip_info:
            for csv_file in zip_info["csv_files"]:
                if self.structure.is_core_module(csv_file):
                    core_module_name = self.structure.categorize_core_file(csv_file)
                    if core_module_name and core_module_name not in found_core_modules:
                        found_core_modules.add(core_module_name)
                        file_info = {
                            "csv_filename": csv_file,
                            "zip_path": zip_info["zip_path"],
                        }
                        module_spec = self.structure.build_module_spec(
                            core_module_name, file_info, pipeline_name="core"
                        )
                        self.modules.append(module_spec)
