# fao/src/db/models/dataset_metadata.py
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from _fao_.src.db.database import Base


class DatasetMetadata(Base):
    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True)
    dataset_code = Column(String(10), unique=True, nullable=False)
    dataset_name = Column(String(255), nullable=False)

    # FAO metadata
    fao_date_update = Column(DateTime)
    fao_file_size = Column(String(20))
    fao_file_rows = Column(Integer)
    fao_file_location = Column(Text)

    # Our tracking
    download_date = Column(DateTime, default=datetime.utcnow)
    local_file_path = Column(Text)
    file_hash = Column(String(64))  # SHA256 hash of the ZIP
    csv_content_hash = Column(String(64))  # SHA256 hash of CSV contents

    # Status tracking
    is_downloaded = Column(Boolean, default=False)
    is_extracted = Column(Boolean, default=False)
    last_checked = Column(DateTime)

    # Change detection
    has_content_changed = Column(Boolean, default=False)
    previous_csv_hash = Column(String(64))

    def __repr__(self):
        return f"<DatasetMetadata({self.dataset_code}: {self.dataset_name})>"
