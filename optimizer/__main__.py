import argparse
from .pattern_discovery import PatternDiscovery


def main():
    parser = argparse.ArgumentParser(description="FAO data pattern discovery")
    parser.add_argument(
        "--zip-path", default=None, help="Path to FAO ZIP files directory"
    )

    args = parser.parse_args()

    if args.zip_path:
        zip_path = args.zip_path
    else:
        # Import the default from generator
        from generator import ZIP_PATH

        zip_path = ZIP_PATH

    discovery = PatternDiscovery(zip_path)
    patterns = discovery.discover_file_patterns()


if __name__ == "__main__":
    main()
