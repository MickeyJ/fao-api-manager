import argparse
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.scanner import Scanner
from generator.csv_analyzer import CSVAnalyzer
from generator.generator import Generator
from generator.data_optimizer import run_optimization_analysis
from . import ZIP_PATH


def analyze():
    """Scan for duplicate files across all ZIP files"""
    # structure = Structure()
    # file_scanner = FileGenerator("./db")
    # scanner = Scanner(ZIP_PATH)
    # csv_analyzer = CSVAnalyzer(structure, scanner, file_scanner)

    # print("Scanning for duplicate files...")
    # similar_files = csv_analyzer.analyze_files()

    # # Show results
    # for normalized_name, data in similar_files.items():
    #     print(f"\n{normalized_name}: found in {data['occurrence_count']} datasets")

    results = run_optimization_analysis(ZIP_PATH)


def generate_all():
    generator = Generator("./db", ZIP_PATH)
    generator.generate()


def main():
    parser = argparse.ArgumentParser(description="FAO data pipeline generator")
    parser.add_argument("--analyze", action="store_true", help="Analyze CSV files")
    parser.add_argument("--all", action="store_true", help="Generate all pipelines")

    args = parser.parse_args()

    if args.analyze:
        analyze()
    elif args.all:
        generate_all()
    else:
        # Default behavior
        generate_all()


if __name__ == "__main__":
    main()
