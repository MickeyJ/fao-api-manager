import logging
import json
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict

from .csv_cache import CSVCache
from generator.scanner import Scanner
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.csv_analyzer import CSVAnalyzer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PatternDiscovery:
    def __init__(self, zip_path: str, shared_cache: CSVCache | None = None):
        self.scanner = Scanner(zip_path)
        self.structure = Structure()
        self.file_generator = FileGenerator("./analysis")
        self.csv_analyzer = CSVAnalyzer(
            self.structure, self.scanner, self.file_generator
        )
        self.pattern_analysis = {}  # Store analysis results here
        self.cache = shared_cache or CSVCache()

        # Setup caching
        self.cache_file = Path("./analysis/csv_analysis_cache.json")
        self.cache_file.parent.mkdir(exist_ok=True)
        self._load_cache()

    def _load_cache(self):
        """Load existing cache from disk"""
        self.cache._load_cache()

    def _save_cache(self):
        self.cache._save_cache()

    def get_csv_analysis(self, zip_path, csv_file):
        """Get CSV analysis with caching"""
        return self.cache.get_analysis(
            zip_path, csv_file, self.csv_analyzer.analyze_csv_from_zip
        )

        return self._csv_cache[cache_key], cache_key  # Return both analysis and key

    def discover_file_patterns(self) -> Dict[str, Any]:
        """Group files by column structure and naming pattern"""
        logger.info("🔍 Starting pattern discovery...")

        file_groups = defaultdict(list)
        total_files = 0

        all_zip_info = self.scanner.scan_all_zips()

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            logger.info(
                f"📁 Processing ZIP {zip_idx}/{len(all_zip_info)}: {zip_info['pipeline_name']}"
            )

            for csv_file in zip_info["csv_files"]:
                total_files += 1
                try:
                    csv_analysis, cache_key = self.get_csv_analysis(
                        zip_info["zip_path"], csv_file
                    )

                    # Create signature
                    columns_signature = tuple(sorted(csv_analysis["columns"]))
                    file_suffix = self.extract_file_suffix(csv_file)

                    signature = (columns_signature, file_suffix)

                    file_groups[signature].append(
                        {
                            "csv_file": csv_file,
                            "dataset": zip_info["pipeline_name"],
                            "row_count": csv_analysis["row_count"],
                            "columns": csv_analysis["columns"],
                            "cache_key": cache_key,  # Store the actual cache key used
                        }
                    )

                except Exception as e:
                    logger.warning(f"⚠️ Error analyzing {csv_file}: {e}")

        logger.info(
            f"✅ Analyzed {total_files} files, found {len(file_groups)} unique patterns"
        )

        # Log cache stats
        cache_stats = self.cache.get_cache_stats()
        logger.info(f"💾 Cache now contains {len(cache_stats)} analyses")

        # Show summary
        self.print_pattern_summary(file_groups)
        self.check_unification_feasibility(file_groups)

        return {"file_groups": file_groups, "pattern_analysis": self.pattern_analysis}

    def identify_primary_key_column(
        self, columns_sig: tuple, sample_data: List[Dict]
    ) -> int | None:
        """Identify which column is likely the primary key based on data patterns"""
        if not sample_data:
            return None

        candidates = []

        for col_idx, col_name in enumerate(columns_sig):
            score = 0
            sample_values = [row.get(col_name, "") for row in sample_data[:10]]

            # Check if values are numeric
            numeric_count = 0
            for val in sample_values:
                if val and str(val).strip().isdigit():
                    numeric_count += 1

            if numeric_count > len(sample_values) * 0.8:  # 80% numeric
                score += 3

            # Check if values are short strings (codes)
            short_string_count = 0
            for val in sample_values:
                if val and len(str(val).strip()) <= 3:
                    short_string_count += 1

            if short_string_count > len(sample_values) * 0.8:  # 80% short
                score += 2

            # Bonus for being first column
            if col_idx == 0:
                score += 1

            # Check for uniqueness (if we have enough sample data)
            unique_values = set(str(v).strip() for v in sample_values if v)
            if len(unique_values) == len([v for v in sample_values if v]):
                score += 2

            candidates.append((col_idx, col_name, score))

        # Return the column with highest score
        if candidates:
            best_candidate = max(candidates, key=lambda x: x[2])
            return best_candidate[0] if best_candidate[2] > 0 else None

        return None

    def check_unification_feasibility(self, file_groups: Dict):
        """Check for value conflicts across files"""
        logger.info("\n🔍 Checking for value conflicts...")

        for (columns_sig, pattern_name), files in file_groups.items():
            if len(files) < 6:
                continue

            logger.info(f"\n📊 {pattern_name} ({len(files)} files)")

            # Collect sample data from all files
            all_sample_data = []
            files_processed = 0

            for file_info in files:
                cache_key = file_info["cache_key"]
                if self.cache.has_analysis_by_key(cache_key):
                    csv_analysis = self.cache.get_analysis_by_key(cache_key)
                    sample_rows = csv_analysis.get("sample_rows", [])
                    all_sample_data.extend(sample_rows)
                    files_processed += 1

            if not all_sample_data:
                logger.info("   ⚠️ No sample data available")
                continue

            # Identify primary key column
            pk_col_idx = self.identify_primary_key_column(columns_sig, all_sample_data)

            if pk_col_idx is None:
                logger.info("   ⚠️ Could not identify primary key column")
                self.pattern_analysis[pattern_name] = {
                    "unifiable": False,
                    "reason": "no_primary_key_identified",
                    "files": files,
                    "columns": columns_sig,
                }
                continue

            pk_col_name = columns_sig[pk_col_idx]

            # Find description column (not the PK, probably longer text)
            desc_col_idx = None
            for i, col_name in enumerate(columns_sig):
                if i != pk_col_idx:
                    desc_col_idx = i
                    break

            if desc_col_idx is None:
                logger.info("   ⚠️ Only one column found, cannot check conflicts")
                self.pattern_analysis[pattern_name] = {
                    "unifiable": False,
                    "reason": "insufficient_columns",
                    "files": files,
                    "columns": columns_sig,
                }
                continue

            desc_col_name = columns_sig[desc_col_idx]

            logger.info(
                f"   📋 Primary key: {pk_col_name}, Description: {desc_col_name}"
            )

            # Look for conflicts: same PK with different descriptions
            pk_to_descriptions = defaultdict(set)
            for row in all_sample_data:
                pk_value = str(row.get(pk_col_name, "")).strip()
                desc_value = str(row.get(desc_col_name, "")).strip()

                if pk_value and desc_value:
                    pk_to_descriptions[pk_value].add(desc_value)

            conflicts = [
                (pk, list(descs))
                for pk, descs in pk_to_descriptions.items()
                if len(descs) > 1
            ]

            self.pattern_analysis[pattern_name] = {
                "unifiable": len(conflicts) == 0,
                "primary_key_column": pk_col_name,
                "description_column": desc_col_name,
                "conflicts": conflicts,
                "files": files,
                "columns": columns_sig,
                "files_processed": files_processed,
            }

            # Report results
            if conflicts:
                logger.info(f"   ❌ {len(conflicts)} conflicts - CANNOT unify")
                logger.info(
                    f"      Same {pk_col_name} maps to different {desc_col_name}:"
                )
                for pk, descriptions in conflicts[:3]:
                    logger.info(f"      {pk_col_name} '{pk}' → {descriptions}")
            else:
                logger.info(f"   ✅ No conflicts - SAFE to unify into single table")
                logger.info(f"      Would save {files_processed - 1} redundant tables")

    def extract_file_suffix(self, csv_file: str) -> str:
        """Extract the meaningful suffix from CSV filename"""
        # Remove common patterns
        base = csv_file.replace(".csv", "")
        base = base.replace("_E_All_Data_(Normalized)", "")
        base = base.replace("_F_All_Data_(Normalized)", "")
        base = base.replace("_E_", "_")

        # Extract the last meaningful part
        parts = base.split("_")
        if len(parts) > 1:
            return parts[-1]  # e.g., "AreaCodes", "Elements", "Flags"
        return base

    def print_pattern_summary(self, file_groups: Dict):
        """Print a summary of discovered patterns"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 PATTERN DISCOVERY SUMMARY")
        logger.info("=" * 60)

        # Sort by frequency (most common patterns first)
        sorted_patterns = sorted(
            file_groups.items(), key=lambda x: len(x[1]), reverse=True
        )

        for (columns_sig, file_suffix), files in sorted_patterns[:10]:  # Top 10
            logger.info(f"\n🔹 Pattern: {file_suffix} ({len(files)} occurrences)")
            logger.info(
                f"   Columns: {list(columns_sig)[:5]}..."
            )  # Show first 5 columns
            logger.info(
                f"   Datasets: {[f['dataset'] for f in files[:3]]}..."
            )  # Show first 3 datasets


# Test it
if __name__ == "__main__":
    from generator import ZIP_PATH

    discovery = PatternDiscovery(ZIP_PATH)
    patterns = discovery.discover_file_patterns()
