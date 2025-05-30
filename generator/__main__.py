from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.scanner import FAOZipScanner
from generator.codebase_generator import CodebaseGenerator
from generator.csv_analyzer import CSVAnalyzer
from . import ZIP_PATH


def general_test():
    structure = Structure()
    file_generator = FileGenerator("./test_output")

    # Example usage of extracting module name
    module_name = structure.extract_module_name(
        "Emissions_Land_Use_Fires_E_All_Data_(Normalized).csv"
    )
    print(f"Extracted module name: {module_name}")

    # Example usage of checking core table
    is_core = structure.is_core_table("Prices_E_AreaCodes.csv")
    print(f"Is core table: {is_core}")

    test_dir = file_generator.create_pipeline_directory("test_pipeline")
    print(f"test_dir: {test_dir}")


def full_generation():
    # Initialize components
    structure = Structure()
    file_generator = FileGenerator("./db")
    scanner = FAOZipScanner(ZIP_PATH)
    csv_analyzer = CSVAnalyzer()

    # Scan for FAO zips
    all_zip_info = scanner.scan_all_zips()

    # Generate codebase
    codebase_generator = CodebaseGenerator("./db")
    codebase_generator.generate_all(all_zip_info, csv_analyzer)

    print("Codebase generation complete!")


def main():
    full_generation()


if __name__ == "__main__":
    main()
