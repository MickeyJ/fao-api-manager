import argparse
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.scanner import Scanner
from generator.csv_analyzer import CSVAnalyzer
from generator.generator import Generator
from generator.pipeline_specs import PipelineSpecs

from . import ZIP_PATH


def all_csv_analysis():
    """Scan for duplicate files across all ZIP files"""
    structure = Structure()
    file_scanner = FileGenerator(output_dir="./analysis")
    scanner = Scanner(ZIP_PATH)
    csv_analyzer = CSVAnalyzer(structure, scanner, file_scanner)

    print("Scanning for duplicate files...")
    similar_files = csv_analyzer.analyze_files()

    # Show results
    for normalized_name, data in similar_files.items():
        print(f"\n{normalized_name}: found in {data['occurrence_count']} datasets")


# def optimization_analysis():
#     """Run optimization analysis on the CSV files"""
#     results = run_optimization_analysis(ZIP_PATH)



def test():
    """Test function to run the generator"""
    # generator = Generator("./db", ZIP_PATH)
    # generator.generate()
    # print("Test completed successfully.")
    raise NotImplementedError("Tests not yet implemented.")


def generate_all():
    generator = Generator("./fao", ZIP_PATH)
    generator.generate()


def main():
    parser = argparse.ArgumentParser(description="FAO data pipeline generator")
    parser.add_argument("--csv_analysis", action="store_true", help="Analyze CSV files")
    parser.add_argument("--opt_analysis", action="store_true", help="run optimization analysis")
    parser.add_argument("--test", action="store_true", help="Generate all pipelines")
    parser.add_argument("--all", action="store_true", help="Generate all pipelines")

    args = parser.parse_args()

    if args.csv_analysis:
        all_csv_analysis()
    elif args.opt_analysis:
        PipelineSpecs(ZIP_PATH).create()
    elif args.test:
        test()
    elif args.all:
        generate_all()

    else:
        # Default behavior
        raise NotImplementedError("available arguments: --analyze | --test | --all")


if __name__ == "__main__":
    main()
