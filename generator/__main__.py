import argparse, json
from pathlib import Path
from generator.generator import Generator
from generator.fao_reference_data_extractor import FAOReferenceDataExtractor
from . import ZIP_PATH


json_cache_path = Path("./cache/fao_module_cache.json")


def test_pre_generation():
    """Test the FAO data discovery"""
    from generator.fao_structure_modules import FAOStructureModules
    from generator.fao_foreign_key_mapper import FAOForeignKeyMapper
    from generator.fao_reference_data_extractor import LOOKUP_MAPPINGS

    # extractor = FAOReferenceDataExtractor(ZIP_PATH, json_cache_path)
    # extractor.run()

    structure_modules = FAOStructureModules(ZIP_PATH, LOOKUP_MAPPINGS, json_cache_path)
    structure_modules.run()

    fk_mapper = FAOForeignKeyMapper(structure_modules.results, LOOKUP_MAPPINGS, json_cache_path)
    enhanced_datasets = fk_mapper.enhance_datasets_with_foreign_keys()

    structure_modules.save()

    print(f"\n✅ Dunzo")

    # raise NotImplementedError("Tests not yet implemented.")


def process_csv():
    """Process lookups from the synthetic_lookups directory"""
    extractor = FAOReferenceDataExtractor(ZIP_PATH, json_cache_path)
    extractor.run()


def generate_all():
    generator = Generator("./fao", ZIP_PATH)
    generator.generate()


def main():
    parser = argparse.ArgumentParser(description="FAO data pipeline generator")

    parser.add_argument(
        "--process_csv", action="store_true", help="pre-process CSV files and create core/lookup csv files"
    )
    parser.add_argument("--pre_test", action="store_true", help="Generate all pipelines")
    parser.add_argument("--all", action="store_true", help="Generate all pipelines")

    args = parser.parse_args()

    if args.process_csv:
        process_csv()
    elif args.pre_test:
        test_pre_generation()
    elif args.all:
        generate_all()

    else:
        # Default behavior
        raise NotImplementedError("available arguments: --process_csv | --pre_test | --all")


if __name__ == "__main__":
    main()
