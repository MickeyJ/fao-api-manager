# generator/csv_cache.py
import json
from pathlib import Path
from typing import Dict, Tuple, Any
from generator import logger


class CSVCache:
    def __init__(self, cache_file_path: str = "./cache/csv_analysis_cache.json"):
        self.cache_file = Path(cache_file_path)
        self.cache_file.parent.mkdir(exist_ok=True)
        self._cache = {}
        self._load_cache()

    def _load_cache(self):
        """Load existing cache from disk"""
        if self.cache_file.exists():
            with open(self.cache_file, "r") as f:
                self._cache = json.load(f)
            logger.info(f"📁 Loaded {len(self._cache)} cached analyses")
        else:
            self._cache = {}
            logger.info("📁 No existing cache found, starting fresh")

    def _save_cache(self):
        """Save cache to disk"""
        with open(self.cache_file, "w") as f:
            json.dump(self._cache, f, indent=2)

    def get_cache_key(self, zip_path, csv_file) -> str:
        """Generate consistent cache key"""
        return f"{zip_path.name}:{csv_file}"

    def get_analysis(self, zip_path, csv_file, analyzer_func) -> Tuple[Dict, str]:
        """Get CSV analysis with incremental caching"""
        cache_key = self.get_cache_key(zip_path, csv_file)

        if cache_key not in self._cache:
            logger.debug(f"🔍 Analyzing {csv_file}...")
            analysis = analyzer_func(zip_path, csv_file)
            self._cache[cache_key] = analysis
            self._save_cache()

        return self._cache[cache_key], cache_key

    def has_analysis(self, zip_path, csv_file) -> bool:
        """Check if analysis exists in cache"""
        cache_key = self.get_cache_key(zip_path, csv_file)
        return cache_key in self._cache

    def get_cached_analysis(self, zip_path, csv_file) -> Dict:
        """Get cached analysis without callback (assumes it exists)"""
        cache_key = self.get_cache_key(zip_path, csv_file)
        return self._cache[cache_key]

    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            "total_analyses": len(self._cache),
            "cache_file": str(self.cache_file),
            "file_exists": self.cache_file.exists(),
        }

    def has_analysis_by_key(self, cache_key: str) -> bool:
        """Check if analysis exists by cache key"""
        return cache_key in self._cache

    def get_analysis_by_key(self, cache_key: str) -> Dict:
        """Get analysis by cache key"""
        return self._cache[cache_key]
