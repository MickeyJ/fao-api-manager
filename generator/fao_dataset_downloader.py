import json
import hashlib
import zipfile
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
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

    def fetch_dataset_metadata(self) -> Dict:
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
        metadata.fao_date_update = datetime.fromisoformat(dataset_info["DateUpdate"].replace("T", " "))
        metadata.fao_file_size = dataset_info["FileSize"]
        metadata.fao_file_rows = dataset_info["FileRows"]
        metadata.fao_file_location = dataset_info["FileLocation"]

        # Update our tracking
        metadata.download_date = datetime.utcnow()
        metadata.local_file_path = str(file_path)
        metadata.file_hash = self.calculate_file_hash(file_path)

        # Calculate and check CSV content hash
        new_csv_hash = self.calculate_csv_content_hash(file_path)
        if metadata.csv_content_hash and metadata.csv_content_hash != new_csv_hash:
            metadata.has_content_changed = True
            metadata.previous_csv_hash = metadata.csv_content_hash
            logger.warning(f"⚠️  Content changed for {dataset_code}")

        metadata.csv_content_hash = new_csv_hash
        metadata.is_downloaded = True
        metadata.last_checked = datetime.utcnow()

        self.db_session.commit()
        return metadata

    def check_and_download_updates(self, force_download: bool = False) -> Dict[str, List[str]]:
        """Check for updates and download new/changed datasets"""
        logger.info("🔍 Checking for dataset updates...")

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
                        fao_update = datetime.fromisoformat(dataset_info["DateUpdate"].replace("T", " "))
                        if existing.fao_date_update and fao_update > existing.fao_date_update:
                            should_download = True
                            logger.info(f"📅 Update available for {dataset_code}")

                    if should_download:
                        file_path = self.download_dataset(dataset_info)
                        metadata = self.update_or_create_metadata(dataset_info, file_path)

                        if existing and metadata.has_content_changed:
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
