import argparse, json
from pathlib import Path
from generator.generator import Generator
from generator.fao_reference_data_extractor import FAOReferenceDataExtractor
from generator.fao_dataset_downloader import FAODatasetDownloader
from generator.aquastat_pre_processor import AQUASTATPreprocessor
from .logger import logger
from . import FAO_ZIP_PATH, API_OUTPUT_PATH, GRAPH_OUTPUT_PATH

assert FAO_ZIP_PATH is not None, "FAO_ZIP_PATH must be set"
assert API_OUTPUT_PATH is not None, "FAO_API_OUTPUT_PATH must be set"
assert GRAPH_OUTPUT_PATH is not None, "GRAPH_OUTPUT_PATH must be set"

json_cache_path = Path("./cache/fao_module_cache.json")


def generate_graph():
    """Generate graph database migration files"""
    # from generator.graph.graph_generator import GraphGenerator
    from generator.graph.graph_db_gen import GraphDBGen

    graph_generator = GraphDBGen(
        output_dir=Path(GRAPH_OUTPUT_PATH),
        cache_path=json_cache_path,
        config_path=Path("generator/graph/graph_db_config.yaml"),
    )
    graph_generator.generate()


def test_pre_generation():
    """Test the FAO data discovery"""
    from generator.fao_structure_modules import FAOStructureModules
    from generator.fao_foreign_key_mapper import FAOForeignKeyMapper
    from generator.fao_reference_data_extractor import REFERENCE_MAPPINGS

    # extractor = FAOReferenceDataExtractor(FAO_ZIP_PATH, json_cache_path)
    # extractor.run()

    structure_modules = FAOStructureModules(FAO_ZIP_PATH, REFERENCE_MAPPINGS, json_cache_path)
    structure_modules.run()

    fk_mapper = FAOForeignKeyMapper(structure_modules.results, REFERENCE_MAPPINGS, json_cache_path)
    enhanced_datasets = fk_mapper.enhance_datasets_with_foreign_keys()

    structure_modules.save()

    print(f"\n✅ Dunzo")

    # raise NotImplementedError("Tests not yet implemented.")


def process_aquastat():
    aquastat_file = r"C:\Users\18057\Documents\Data\fao-test-zips\all\AQUASTAT\bulk_eng(in).csv"
    aquastat_input_csv = Path(aquastat_file)
    output_dir = Path(FAO_ZIP_PATH)  # Uses the configured FAO_ZIP_PATH from generator.__init__

    if not aquastat_input_csv.exists():
        logger.error(f"Error: Input file {aquastat_input_csv} not found")
    else:
        logger.info(f"Processing AQUASTAT data from {aquastat_input_csv}")
        # Run preprocessor
        aquastat_preprocessor = AQUASTATPreprocessor(aquastat_input_csv, output_dir)
        aquastat_preprocessor.run(create_zip=True)


def process_csv():
    """Process references from the synthetic_references directory"""
    extractor = FAOReferenceDataExtractor(FAO_ZIP_PATH, json_cache_path)
    extractor.run()


def generate_all():
    generator = Generator(API_OUTPUT_PATH, FAO_ZIP_PATH)
    generator.generate()


def process_and_generate():
    """Process aquastat, references and generate codebase"""
    # process_aquastat()
    process_csv()
    generate_all()


# In generator/__main__.py, update the imports and functions:


def update_datasets():
    """Download/update FAO datasets"""
    from _fao_.src.db.database import run_with_session
    from generator.fao_dataset_downloader import FAODatasetDownloader

    def _update(db):
        downloader = FAODatasetDownloader(db, Path(FAO_ZIP_PATH))
        results = downloader.check_and_download_updates()

    run_with_session(_update)


def check_updates():
    """Check for available updates without downloading"""
    from _fao_.src.db.database import run_with_session
    from generator.fao_dataset_downloader import FAODatasetDownloader

    def _check(db):
        downloader = FAODatasetDownloader(db, Path(FAO_ZIP_PATH))
        datasets = downloader.get_dataset_status()

        logger.info("\n📊 Dataset Status:")
        for ds in datasets:
            status = "✅" if ds.is_downloaded else "❌"
            changed = "⚠️ CHANGED" if ds.has_content_changed else ""
            logger.info(f"  {status} {ds.dataset_code}: {ds.dataset_name} {changed}")
            if ds.download_date:
                logger.info(f"      Downloaded: {ds.download_date.strftime('%Y-%m-%d %H:%M')}")

    run_with_session(_check)


def dataset_status():
    """Show detailed dataset status"""
    from _fao_.src.db.database import run_with_session
    from generator.fao_dataset_downloader import FAODatasetDownloader

    def _status(db):
        downloader = FAODatasetDownloader(db, Path(FAO_ZIP_PATH))
        datasets = downloader.get_dataset_status()

        logger.info("\n📊 Detailed Dataset Status:")
        logger.info("-" * 80)

        for ds in datasets:
            logger.info(f"\n{ds.dataset_code}: {ds.dataset_name}")
            logger.info(f"  FAO Updated: {ds.fao_date_update}")
            logger.info(f"  Downloaded: {ds.download_date}")
            logger.info(f"  File Size: {ds.fao_file_size}")
            logger.info(f"  Row Count: {ds.fao_file_rows:,}")
            logger.info(f"  Content Hash: {ds.csv_content_hash[:16]}...")

            if ds.has_content_changed:
                logger.info(f"  ⚠️  CONTENT CHANGED!")
                logger.info(f"  Previous Hash: {ds.previous_csv_hash[:16]}...")

    run_with_session(_status)


def init_dataset_db(verbose=True):
    """Initialize database with existing dataset files"""
    from _fao_.src.db.database import run_with_session
    from generator.fao_dataset_downloader import FAODatasetDownloader

    def _init(db):
        downloader = FAODatasetDownloader(db, Path(FAO_ZIP_PATH))
        results = downloader.initialize_from_existing_files(verbose=verbose)

        # Show final status
        logger.info("\n📊 Final Status:")
        datasets = downloader.get_dataset_status()

        downloaded_count = sum(1 for ds in datasets if ds.is_downloaded)
        extracted_count = sum(1 for ds in datasets if ds.is_extracted)

        logger.info(f"  Total datasets tracked: {len(datasets)}")
        logger.info(f"  Downloaded: {downloaded_count}")
        logger.info(f"  Extracted: {extracted_count}")

        # If there were errors, remind about the error report
        if results.get("error_details"):
            logger.info(f"\n⚠️  There were {len(results['error_details'])} errors during processing.")
            logger.info(f"  Check initialization_errors.json for details.")

    run_with_session(_init)


def main():
    parser = argparse.ArgumentParser(description="FAO data pipeline generator")

    parser.add_argument(
        "--process_csv", action="store_true", help="pre-process CSV files and create core/reference csv files"
    )
    parser.add_argument("--pre_test", action="store_true", help="Generate all pipelines")
    parser.add_argument("--all", action="store_true", help="Generate all pipelines")
    parser.add_argument("--update_datasets", action="store_true", help="Download/update FAO datasets")
    parser.add_argument("--check_updates", action="store_true", help="Check for dataset updates")
    parser.add_argument("--dataset_status", action="store_true", help="Show detailed dataset status")
    parser.add_argument("--graph", action="store_true", help="Show detailed dataset status")
    parser.add_argument(
        "--init_datasets", action="store_true", help="Initialize database with existing downloaded datasets"
    )
    parser.add_argument(
        "--process_and_generate", action="store_true", help="Process AQUASTAT, references and generate codebase"
    )

    args = parser.parse_args()

    if args.process_csv:
        process_csv()
    elif args.pre_test:
        test_pre_generation()
    elif args.all:
        generate_all()
    elif args.update_datasets:
        update_datasets()
    elif args.check_updates:
        check_updates()
    elif args.dataset_status:
        dataset_status()
    elif args.init_datasets:
        init_dataset_db()
    elif args.graph:
        generate_graph()
    elif args.process_and_generate:
        process_and_generate()

    else:
        # Default behavior
        raise NotImplementedError("available arguments: --process_csv | --pre_test | --all")


if __name__ == "__main__":
    main()
