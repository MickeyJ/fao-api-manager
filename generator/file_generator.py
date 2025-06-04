from pathlib import Path
from typing import Dict, Optional
import json, difflib


class FileGenerator:
    def __init__(self, output_dir: str | Path):
        self.output_dir = Path(output_dir)
        self.cache_dir = self.output_dir / ".generator_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._update_all = None  # Track if user selected "all" option

    def create_dir(self, dir_path: str | Path) -> Path:
        dir_path = Path(self.output_dir / dir_path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path

    def generate_pipeline_files(self, pipeline_dir: Path, rendered_files: Dict[str, str]) -> None:
        """Write multiple rendered files to a pipeline directory"""
        for filename, content in rendered_files.items():
            file_path = pipeline_dir / filename
            self.write_file(file_path, content)

    def write_file(self, file_path: Path | str, content: str) -> None:
        """Write content to a file"""
        file_path = Path(self.output_dir / file_path)
        file_path.write_text(content, encoding="utf-8")

    def write_file_cache(self, file_path: Path | str, content: str) -> None:
        """Write content to a file with caching and diff detection"""
        file_path = Path(self.output_dir / file_path)

        # Check if we have cached content
        cached_content = self._read_cache(file_path)

        if cached_content is None:
            # First time generation - write both file and cache
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")
            self._write_cache(file_path, content)
            print(f"✅ Generated: {file_path}")

        elif cached_content != content:
            # Content has changed - show diff and prompt
            self._show_diff(cached_content, content, file_path)

            if self._prompt_for_update(file_path):
                # User accepted update
                file_path.write_text(content, encoding="utf-8")
                self._write_cache(file_path, content)
                print(f"✅ Updated: {file_path}")
            else:
                # User rejected update
                print(f"⏭️  Skipped: {file_path}")

        else:
            # Content unchanged - skip silently
            pass

    def write_json_file(self, file_path: Path | str, data: Dict) -> None:
        """Write dictionary data to a JSON file"""
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _get_cache_path(self, file_path: Path | str) -> Path:
        """Get the cache file path for a given output file"""
        file_path = Path(file_path)
        # If path is relative to output_dir, preserve structure
        if file_path.is_relative_to(self.output_dir):
            relative_path = file_path.relative_to(self.output_dir)
        else:
            # If absolute path given, make it relative
            relative_path = Path(file_path.name)

        return self.cache_dir / relative_path

    def _write_cache(self, file_path: Path | str, content: str) -> None:
        """Write content to cache file"""
        cache_path = self._get_cache_path(file_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(content, encoding="utf-8")

    def _read_cache(self, file_path: Path | str) -> str | None:
        """Read cached content if it exists"""
        cache_path = self._get_cache_path(file_path)
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8")
        return None

    def _has_cache(self, file_path: Path | str) -> bool:
        """Check if cache exists for a file"""
        cache_path = self._get_cache_path(file_path)
        return cache_path.exists()

    def _show_diff(self, cached_content: str, new_content: str, file_path: Path) -> None:
        """Show diff between cached and new content"""
        print(f"\n{'='*60}")
        print(f"File: {file_path}")
        print(f"{'='*60}")

        diff = difflib.unified_diff(
            cached_content.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile="cached (original generated)",
            tofile="new (updated generation)",
            n=3,
        )

        for line in diff:
            if line.startswith("+"):
                print(f"\033[92m{line}\033[0m", end="")  # Green
            elif line.startswith("-"):
                print(f"\033[91m{line}\033[0m", end="")  # Red
            else:
                print(line, end="")

    def _prompt_for_update(self, file_path: Path) -> bool:
        """Prompt user to accept or reject update"""
        # Check if user already selected "all" option
        if self._update_all is not None:
            return self._update_all

        print(f"\nThe generated content for {file_path} has changed.")
        print("Options:")
        print("  [y] Yes      - Update this file")
        print("  [n] No       - Keep current file")
        print("  [a] All      - Update all remaining files")
        print("  [s] Skip all - Keep all remaining files")

        while True:
            choice = input("Update file? [y/n/a/s]: ").lower().strip()
            if choice in ["y", "yes"]:
                return True
            elif choice in ["n", "no"]:
                return False
            elif choice in ["a", "all"]:
                self._update_all = True
                return True
            elif choice in ["s", "skip"]:
                self._update_all = False
                return False
            print("Please enter 'y', 'n', 'a', or 's'")
