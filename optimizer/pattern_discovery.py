import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

from generator.scanner import Scanner
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.csv_analyzer import CSVAnalyzer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PatternDiscovery:
    def __init__(self, zip_path: str):
        self.scanner = Scanner(zip_path)
        self.structure = Structure()
        self.file_generator = FileGenerator("./analysis")
        self.csv_analyzer = CSVAnalyzer(
            self.structure, self.scanner, self.file_generator
        )

        # Setup caching
        self.cache_file = Path("./analysis/csv_analysis_cache.json")
        self.cache_file.parent.mkdir(exist_ok=True)
        self._load_cache()

    def _load_cache(self):
        """Load existing cache from disk"""
        if self.cache_file.exists():
            with open(self.cache_file, "r") as f:
                self._csv_cache = json.load(f)
            logger.info(f"📁 Loaded {len(self._csv_cache)} cached analyses")
        else:
            self._csv_cache = {}
            logger.info("📁 No existing cache found, starting fresh")

    def _save_cache(self):
        """Save cache to disk"""
        with open(self.cache_file, "w") as f:
            json.dump(self._csv_cache, f, indent=2)

    def get_csv_analysis(self, zip_path, csv_file):
        """Get CSV analysis with incremental caching"""
        cache_key = f"{zip_path.name}:{csv_file}"

        if cache_key not in self._csv_cache:
            logger.debug(f"🔍 Analyzing {csv_file}...")
            analysis = self.csv_analyzer.analyze_csv_from_zip(zip_path, csv_file)

            # Store both the analysis AND the cache key for later lookup
            self._csv_cache[cache_key] = analysis
            self._save_cache()

        return self._csv_cache[cache_key], cache_key  # Return both analysis and key

    def discover_file_patterns(self) -> Dict[Tuple, List]:
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
        logger.info(f"💾 Cache now contains {len(self._csv_cache)} analyses")

        # Show summary
        self.print_pattern_summary(file_groups)
        self.check_unification_feasibility(file_groups)

        return file_groups

    def check_unification_feasibility(self, file_groups: Dict):
        """Check for value conflicts across files"""
        logger.info("\n🔍 Checking for value conflicts...")

        for (columns_sig, pattern_name), files in file_groups.items():
            if len(files) < 6:
                continue

            logger.info(f"\n📊 {pattern_name} ({len(files)} files)")

            # Collect all value combinations
            value_combinations = defaultdict(set)
            files_processed = 0

            for file_info in files:
                cache_key = file_info["cache_key"]
                if cache_key in self._csv_cache:
                    csv_analysis = self._csv_cache[cache_key]

                    for row in csv_analysis.get("sample_rows", []):
                        row_values = tuple(
                            str(row.get(col, "")).strip() for col in columns_sig
                        )
                        if any(row_values):
                            value_combinations[row_values].add(file_info["dataset"])

                    files_processed += 1

            # REPLACE THIS SECTION:
            # Find which column is the code vs description
            code_col_idx = None
            desc_col_idx = None

            for i, col in enumerate(columns_sig):
                if "code" in col.lower():
                    code_col_idx = i
                else:
                    desc_col_idx = i

            if code_col_idx is not None and desc_col_idx is not None:
                # Look for real conflicts: same code with different descriptions
                code_to_descriptions = defaultdict(set)
                for row_values in value_combinations.keys():
                    if len(row_values) > max(code_col_idx, desc_col_idx):
                        code = row_values[code_col_idx]
                        description = row_values[desc_col_idx]
                        if code and description:
                            code_to_descriptions[code].add(description)

                conflicts = [
                    (code, list(descs))
                    for code, descs in code_to_descriptions.items()
                    if len(descs) > 1
                ]
            else:
                conflicts = []

            # Report results
            if conflicts:
                logger.info(f"   ❌ {len(conflicts)} conflicts - CANNOT unify")
                logger.info(f"      Same code maps to different descriptions:")
                for code, descriptions in conflicts[:3]:
                    logger.info(f"      Code '{code}' → {descriptions}")
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
