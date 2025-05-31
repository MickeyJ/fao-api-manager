from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.scanner import FAOZipScanner
from generator.csv_analyzer import CSVAnalyzer
from generator.generator import Generator
from . import ZIP_PATH


def general_test():
    scanner = FAOZipScanner(ZIP_PATH)
    generator = Generator("./db")

    all_zip_info = scanner.scan_all_zips()

    generator.generate(all_zip_info)


def full_generation():
    """Run the full generation process from scanning to file generation."""


def main():
    general_test()


if __name__ == "__main__":
    main()
