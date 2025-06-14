from datetime import datetime, timezone
import hashlib, json, zipfile, requests, traceback
from pathlib import Path
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from .logger import logger
from _fao_.src.db.system_models.dataset_metadata import DatasetMetadata


class FAODatasetDownloader:
    """Download and manage FAO dataset updates"""

    DATASETS_JSON_URL = "https://bulks-faostat.fao.org/production/datasets_E.json"

    def __init__(self, db_session: Session, download_dir: Path):
        self.db_session = db_session
        self.download_dir = download_dir
        self.download_dir.mkdir(exist_ok=True)

    def fetch_dataset_metadata(self) -> List[Dict]:
        """Fetch the latest dataset information from FAO"""
        logger.info(f"📡 Fetching dataset metadata from FAO...")

        response = requests.get(self.DATASETS_JSON_URL)
        response.raise_for_status()

        data = response.json()
        return data["Datasets"]["Dataset"]

    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def calculate_csv_content_hash(self, zip_path: Path) -> str:
        """Calculate hash of CSV contents within a ZIP file"""
        sha256_hash = hashlib.sha256()

        with zipfile.ZipFile(zip_path, "r") as zf:
            # Get all CSV files and sort them for consistent hashing
            csv_files = sorted([f for f in zf.namelist() if f.endswith(".csv")])

            for csv_file in csv_files:
                # Read CSV content and add to hash
                with zf.open(csv_file) as f:
                    content = f.read()
                    sha256_hash.update(content)

        return sha256_hash.hexdigest()

    def download_dataset(self, dataset_info: Dict) -> Path:
        """Download a single dataset"""
        url = dataset_info["FileLocation"]
        filename = url.split("/")[-1]
        file_path = self.download_dir / filename

        logger.info(f"📥 Downloading {filename}...")

        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Download with progress indication
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    progress = (downloaded / total_size) * 100
                    print(f"\r  Progress: {progress:.1f}%", end="")

        print()  # New line after progress
        logger.info(f"✅ Downloaded: {filename}")

        return file_path

    def update_or_create_metadata(self, dataset_info: Dict, file_path: Path) -> DatasetMetadata:
        """Update or create dataset metadata in database"""
        dataset_code = dataset_info["DatasetCode"]

        # Check if record exists
        metadata = self.db_session.query(DatasetMetadata).filter_by(dataset_code=dataset_code).first()

        if not metadata:
            metadata = DatasetMetadata(dataset_code=dataset_code)
            self.db_session.add(metadata)

        # Update with latest info
        metadata.dataset_name = dataset_info["DatasetName"]

        # Parse the date properly
        fao_date_str = dataset_info["DateUpdate"].replace("T", " ")
        if fao_date_str.endswith("+00:00"):
            fao_date_str = fao_date_str[:-6]  # Remove timezone info
        metadata.fao_date_update = datetime.fromisoformat(fao_date_str)

        metadata.fao_file_size = dataset_info["FileSize"]
        metadata.fao_file_rows = dataset_info["FileRows"]
        metadata.fao_file_location = dataset_info["FileLocation"]

        # Update our tracking
        metadata.download_date = datetime.now(timezone.utc)
        metadata.local_file_path = str(file_path)
        metadata.file_hash = self.calculate_file_hash(file_path)

        # Calculate and check CSV content hash
        new_csv_hash = self.calculate_csv_content_hash(file_path)

        # Check actual value, not the Column object
        if metadata.csv_content_hash is not None and metadata.csv_content_hash != new_csv_hash:
            metadata.has_content_changed = True
            metadata.previous_csv_hash = metadata.csv_content_hash
            logger.warning(f"⚠️  Content changed for {dataset_code}")

        metadata.csv_content_hash = new_csv_hash
        metadata.is_downloaded = True
        metadata.last_checked = datetime.now(timezone.utc)

        self.db_session.commit()
        return metadata

    def verify_and_fix_download_status(self) -> Dict[str, List[str]]:
        """Verify files marked as downloaded actually exist, fix database if not"""
        logger.info("🔍 Verifying downloaded files exist on disk...")

        results = {"missing": [], "verified": [], "fixed": []}

        downloaded_datasets = self.db_session.query(DatasetMetadata).filter_by(is_downloaded=True).all()

        for dataset in downloaded_datasets:
            if dataset.local_file_path:
                file_path = Path(dataset.local_file_path)

                if not file_path.exists():
                    # File is missing - update database
                    dataset.is_downloaded = False
                    dataset.local_file_path = None
                    dataset.file_hash = None
                    dataset.csv_content_hash = None

                    results["missing"].append(dataset.dataset_code)
                    results["fixed"].append(dataset.dataset_code)

                    logger.warning(f"📁 Missing file for {dataset.dataset_code}: {file_path}")
                    logger.info(f"   ✅ Fixed database - marked as not downloaded")
                else:
                    results["verified"].append(dataset.dataset_code)

        self.db_session.commit()

        # Summary
        logger.info(f"\n📊 File Verification Summary:")
        logger.info(f"  Verified: {len(results['verified'])}")
        logger.info(f"  Missing: {len(results['missing'])}")
        logger.info(f"  Fixed: {len(results['fixed'])}")

        return results

    def check_and_download_updates(self, force_download: bool = False) -> Dict[str, List[str]]:
        """Check for updates and download new/changed datasets"""
        logger.info("🔍 Checking for dataset updates...")

        # First, verify existing files
        if not force_download:
            verification_results = self.verify_and_fix_download_status()
            if verification_results["missing"]:
                logger.info(f"📥 Will re-download {len(verification_results['missing'])} missing files")

        results = {"downloaded": [], "skipped": [], "updated": [], "errors": []}

        try:
            datasets = self.fetch_dataset_metadata()

            for dataset_info in datasets:
                dataset_code = dataset_info["DatasetCode"]

                try:
                    # Check if we need to download
                    existing = self.db_session.query(DatasetMetadata).filter_by(dataset_code=dataset_code).first()

                    should_download = force_download or not existing or not existing.is_downloaded

                    if existing and not force_download:
                        # Check if FAO updated the file
                        fao_date_str = dataset_info["DateUpdate"].replace("T", " ")
                        if fao_date_str.endswith("+00:00"):
                            fao_date_str = fao_date_str[:-6]
                        fao_update = datetime.fromisoformat(fao_date_str)

                        if existing.fao_date_update is not None and fao_update > existing.fao_date_update:
                            should_download = True
                            logger.info(f"📅 Update available for {dataset_code}")

                    if should_download:
                        file_path = self.download_dataset(dataset_info)
                        metadata = self.update_or_create_metadata(dataset_info, file_path)

                        if existing and metadata.has_content_changed is True:
                            results["updated"].append(dataset_code)
                        else:
                            results["downloaded"].append(dataset_code)
                    else:
                        results["skipped"].append(dataset_code)

                except Exception as e:
                    logger.error(f"❌ Error processing {dataset_code}: {e}")
                    results["errors"].append(f"{dataset_code}: {str(e)}")

        except Exception as e:
            logger.error(f"❌ Failed to fetch dataset metadata: {e}")
            results["errors"].append(f"Metadata fetch: {str(e)}")

        # Summary
        logger.info(f"\n📊 Summary:")
        logger.info(f"  Downloaded: {len(results['downloaded'])}")
        logger.info(f"  Updated: {len(results['updated'])}")
        logger.info(f"  Skipped: {len(results['skipped'])}")
        logger.info(f"  Errors: {len(results['errors'])}")

        return results

    def get_dataset_status(self) -> List[DatasetMetadata]:
        """Get status of all tracked datasets"""
        return self.db_session.query(DatasetMetadata).all()

    def initialize_from_existing_files(self, verbose: bool = True) -> Dict[str, any]:
        """
        Scan download directory and FAO metadata to create a complete dataset inventory.
        Creates records for both downloaded files and available-but-not-downloaded datasets.
        """
        logger.info("🔍 Initializing complete dataset inventory...")

        results = {
            "new_records": 0,
            "updated_records": 0,
            "errors": 0,
            "skipped": 0,
            "not_downloaded": 0,
            "error_details": [],
            "processed_files": [],
        }

        # Set a fixed date for existing files
        existing_file_date = datetime(2025, 6, 1, tzinfo=timezone.utc)

        # First, fetch metadata from FAO
        try:
            fao_datasets = self.fetch_dataset_metadata()
            fao_lookup = {}
            for dataset in fao_datasets:
                filename = dataset["FileLocation"].split("/")[-1]
                fao_lookup[filename] = dataset
            logger.info(f"✅ Fetched metadata for {len(fao_lookup)} datasets from FAO")
        except Exception as e:
            error_msg = f"Could not fetch FAO metadata: {e}"
            logger.error(f"❌ {error_msg}")
            results["error_details"].append({"type": "fao_metadata_fetch", "error": str(e)})
            fao_lookup = {}

        # PHASE 1: Process all local ZIP files
        zip_files = list(self.download_dir.glob("*.zip"))
        total_files = len(zip_files)
        logger.info(f"📁 Found {total_files} local ZIP files")

        processed_filenames = set()

        for idx, zip_path in enumerate(zip_files, 1):
            try:
                filename = zip_path.name
                processed_filenames.add(filename)

                if verbose:
                    logger.info(f"\n[{idx}/{total_files}] Processing: {filename}")

                # Check if record exists by file path
                existing = self.db_session.query(DatasetMetadata).filter_by(local_file_path=str(zip_path)).first()

                if existing and existing.is_downloaded is True:
                    if verbose:
                        logger.info(f"  ✓ Already tracked: {filename}")
                    results["skipped"] += 1
                    continue

                # Create or update metadata
                if not existing:
                    metadata = DatasetMetadata()
                    self.db_session.add(metadata)
                    results["new_records"] += 1
                else:
                    metadata = existing
                    results["updated_records"] += 1

                # Set dataset code only if we have it from FAO
                if filename in fao_lookup:
                    fao_info = fao_lookup[filename]
                    metadata.dataset_code = fao_info["DatasetCode"]
                    metadata.dataset_name = fao_info["DatasetName"]

                    fao_date_str = fao_info["DateUpdate"].replace("T", " ")
                    if fao_date_str.endswith("+00:00"):
                        fao_date_str = fao_date_str[:-6]
                    metadata.fao_date_update = datetime.fromisoformat(fao_date_str)

                    metadata.fao_file_size = fao_info["FileSize"]
                    metadata.fao_file_rows = fao_info["FileRows"]
                    metadata.fao_file_location = fao_info["FileLocation"]
                else:
                    # No FAO metadata - get file info
                    metadata.dataset_name = filename.replace(".zip", "").replace("_", " ")

                    file_stats = zip_path.stat()
                    metadata.fao_file_size = f"{file_stats.st_size / 1024 / 1024:.1f}MB"
                    metadata.fao_date_update = datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc)

                    if verbose:
                        logger.warning(f"  ⚠️  No FAO metadata for {filename}")

                # Set our tracking info
                metadata.download_date = existing_file_date
                metadata.local_file_path = str(zip_path)
                metadata.is_downloaded = True

                # Calculate hashes
                try:
                    if verbose:
                        logger.info(f"  📊 Calculating file hash...")
                    metadata.file_hash = self.calculate_file_hash(zip_path)
                except Exception as e:
                    error_msg = f"Failed to calculate file hash: {e}"
                    logger.error(f"  ❌ {error_msg}")
                    results["error_details"].append({"file": filename, "error": str(e), "type": "file_hash_error"})
                    metadata.file_hash = None

                try:
                    if verbose:
                        logger.info(f"  📊 Calculating CSV content hash...")
                    metadata.csv_content_hash = self.calculate_csv_content_hash(zip_path)
                except Exception as e:
                    error_msg = f"Failed to calculate CSV hash: {e}"
                    logger.error(f"  ❌ {error_msg}")
                    results["error_details"].append({"file": filename, "error": str(e), "type": "csv_hash_error"})
                    metadata.csv_content_hash = None

                # Check if extracted
                extract_dir = zip_path.parent / zip_path.stem
                if extract_dir.exists() and extract_dir.is_dir():
                    metadata.is_extracted = True
                    if verbose:
                        logger.info(f"  ✓ Already extracted")

                metadata.last_checked = datetime.now(timezone.utc)
                self.db_session.commit()

                if verbose:
                    logger.info(f"  ✅ Processed: {filename}")

                results["processed_files"].append(
                    {"file": filename, "code": metadata.dataset_code, "status": "new" if not existing else "updated"}
                )

            except Exception as e:
                error_msg = f"Error processing {zip_path.name}: {e}"
                logger.error(f"❌ {error_msg}")
                results["errors"] += 1
                results["error_details"].append(
                    {
                        "file": zip_path.name,
                        "error": str(e),
                        "type": "processing_error",
                        "traceback": traceback.format_exc(),
                    }
                )
                self.db_session.rollback()

        # PHASE 2: Process FAO metadata for files we DON'T have
        logger.info(f"\n📡 Processing FAO metadata for non-downloaded datasets...")

        for filename, fao_info in fao_lookup.items():
            if filename in processed_filenames:
                continue

            try:
                dataset_code = fao_info["DatasetCode"]

                existing = self.db_session.query(DatasetMetadata).filter_by(dataset_code=dataset_code).first()

                if existing:
                    if verbose:
                        logger.info(f"  ✓ Already tracked (not downloaded): {dataset_code}")
                    results["skipped"] += 1
                    continue

                metadata = DatasetMetadata()
                metadata.dataset_code = dataset_code
                metadata.dataset_name = fao_info["DatasetName"]

                fao_date_str = fao_info["DateUpdate"].replace("T", " ")
                if fao_date_str.endswith("+00:00"):
                    fao_date_str = fao_date_str[:-6]
                metadata.fao_date_update = datetime.fromisoformat(fao_date_str)

                metadata.fao_file_size = fao_info["FileSize"]
                metadata.fao_file_rows = fao_info["FileRows"]
                metadata.fao_file_location = fao_info["FileLocation"]

                metadata.is_downloaded = False
                metadata.is_extracted = False
                metadata.last_checked = datetime.now(timezone.utc)

                self.db_session.add(metadata)
                self.db_session.commit()

                results["not_downloaded"] += 1
                if verbose:
                    logger.info(f"  📥 Added (not downloaded): {dataset_code} - {fao_info['DatasetName']}")

            except Exception as e:
                logger.error(f"❌ Error processing FAO metadata for {filename}: {e}")
                results["errors"] += 1
                self.db_session.rollback()

        # Summary
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Initialization Summary:")
        logger.info(f"{'='*60}")
        logger.info(f"  Local files processed: {total_files}")
        logger.info(f"  New records: {results['new_records']}")
        logger.info(f"  Updated records: {results['updated_records']}")
        logger.info(f"  Not downloaded (FAO only): {results['not_downloaded']}")
        logger.info(f"  Skipped (already tracked): {results['skipped']}")
        logger.info(f"  Errors: {results['errors']}")

        # Show error details
        if results["error_details"]:
            logger.error(f"\n❌ Error Details:")
            logger.error(f"{'='*60}")

            error_types = {}
            for error in results["error_details"]:
                error_type = error.get("type", "unknown")
                if error_type not in error_types:
                    error_types[error_type] = []
                error_types[error_type].append(error)

            for error_type, errors in error_types.items():
                logger.error(f"\n{error_type.upper()} ({len(errors)} errors):")
                for error in errors[:5]:
                    logger.error(f"  - {error.get('file', 'N/A')}: {error['error']}")
                if len(errors) > 5:
                    logger.error(f"  ... and {len(errors) - 5} more")

        if results["error_details"]:
            error_report_path = self.download_dir / "initialization_errors.json"
            with open(error_report_path, "w") as f:
                json.dump(results["error_details"], f, indent=2, default=str)
            logger.info(f"\n💾 Detailed error report saved to: {error_report_path}")

        return results
