import argparse, json
from pathlib import Path
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.scanner import Scanner
from generator.csv_analyzer import CSVAnalyzer
from generator.generator import Generator
from generator.pipeline_specs import PipelineSpecs
from generator.fao_reference_data_extractor import FAOReferenceDataExtractor
from generator.fao_conflict_detector import FAOConflictDetector


from . import ZIP_PATH


def all_csv_analysis():
    """Scan for duplicate files across all ZIP files"""
    structure = Structure()
    file_scanner = FileGenerator(output_dir="./analysis")
    scanner = Scanner(ZIP_PATH, structure)
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
    """Test the FAO data discovery"""
    from generator.fao_structure_modules import FAOStructureModules
    from generator.fao_foreign_key_mapper import FAOForeignKeyMapper
    from generator.fao_reference_data_extractor import LOOKUP_MAPPINGS

    json_cache_path = Path("./analysis/fao_module_cache.json")

    # extractor = FAOReferenceDataExtractor(ZIP_PATH, json_cache_path)
    # extractor.run()

    structure_modules = FAOStructureModules(ZIP_PATH, LOOKUP_MAPPINGS, json_cache_path)
    structure_modules.run()

    fk_mapper = FAOForeignKeyMapper(structure_modules.results, LOOKUP_MAPPINGS, json_cache_path)
    enhanced_datasets = fk_mapper.enhance_datasets_with_foreign_keys()

    conflict_detector = FAOConflictDetector(structure_modules.results)
    enhanced_lookups = conflict_detector.enhance_with_conflicts()

    # # Show summary
    # summary = conflict_detector.get_conflict_summary()
    # print(json.dumps(summary, indent=2))

    # # Show details for specific lookup
    # conflict_detector.show_conflict_details("area_codes", max_conflicts=5)

    # # Get summary
    # summary = conflict_detector.get_conflict_summary()
    # print(json.dumps(summary, indent=2))

    structure_modules.save()

    print(f"\n✅ Dunzo")

    # raise NotImplementedError("Tests not yet implemented.")


def process_csv():
    """Process lookups from the synthetic_lookups directory"""
    extractor = FAOReferenceDataExtractor(ZIP_PATH)
    extractor.run()


def generate_all():
    generator = Generator("./fao", ZIP_PATH)
    generator.generate()


def main():
    parser = argparse.ArgumentParser(description="FAO data pipeline generator")
    parser.add_argument("--csv_analysis", action="store_true", help="Analyze CSV files")
    parser.add_argument("--opt_analysis", action="store_true", help="run optimization analysis")
    parser.add_argument(
        "--process_csv", action="store_true", help="pre-process CSV files and create core/lookup csv files"
    )
    parser.add_argument("--test", action="store_true", help="Generate all pipelines")
    parser.add_argument("--all", action="store_true", help="Generate all pipelines")

    args = parser.parse_args()

    if args.csv_analysis:
        all_csv_analysis()
    elif args.process_csv:
        process_csv()
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
