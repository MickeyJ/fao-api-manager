import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from . import logger
from .utils.dataclass import FAOLookup, LookupConflict, DatasetReference, ConflictVariation, ReferencedConflict


class FAOConflictDetector:
    """Detects conflicts and checks if they're referenced by datasets"""

    def __init__(self, structure_results: Dict, lookup_mappings: Dict):
        self.lookups = structure_results["lookups"]
        self.datasets = structure_results["datasets"]
        self.lookup_mappings = lookup_mappings
        # Get synthetic_lookups_dir from first lookup's path
        first_lookup = next(iter(self.lookups.values()))
        self.synthetic_lookups_dir = Path(first_lookup.file_path).parent
        self.synthetic_pk_counter = 900000

    def enhance_lookups_with_conflicts(self) -> Dict[str, FAOLookup]:
        """Add conflict information to existing lookup objects"""
        logger.info("🔍 Detecting conflicts in synthetic lookups...")

        total_conflicts = 0
        referenced_conflicts = 0

        for lookup_name, lookup in self.lookups.items():
            # Find the mapping for this lookup
            mapping = None
            for map_key, map_data in self.lookup_mappings.items():
                if map_data["lookup_name"] == lookup_name:
                    mapping = map_data
                    break

            if not mapping or not lookup.file_path.exists():
                continue

            if lookup.has_conflicts:
                total_conflicts += lookup.conflict_count
                referenced_conflicts += lookup.referenced_conflict_count
                logger.info(
                    f"  ⚠️  {lookup_name}: {lookup.conflict_count} conflicts "
                    f"({lookup.referenced_conflict_count} referenced by datasets)"
                )
            else:
                logger.info(f"  ✅ {lookup_name}: no conflicts")

        logger.info(
            f"✅ Conflict detection complete: {total_conflicts} total conflicts, "
            f"{referenced_conflicts} referenced by datasets"
        )
        return self.lookups

    def _read_csv_safely(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Read CSV with multiple encoding attempts"""
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                df = df.fillna("")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error reading {file_path} with {encoding}: {e}")

        logger.error(f"Failed to read {file_path} with any encoding")
        return None
