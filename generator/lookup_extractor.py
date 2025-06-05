# generator/lookup_extractor.py
import zipfile, json
from pathlib import Path
from typing import Dict, List, Set
import pandas as pd
from . import logger


class LookupExtractor:
    """Extract lookup tables from dataset files and create synthetic CSVs"""

    def __init__(self, zip_directory: str | Path):
        self.zip_dir = Path(zip_directory)
        self.analysis_dir = Path("./analysis")
        self.extracted_lookups = {}  # Will store discovered lookups

    def run(self):
        """Main entry point - extract and analyze everything"""
        logger.info("🚀 Starting lookup extraction process...")

        # Step 1: Extract all ZIPs
        self.extract_all_zips()
        self.extract_all_lookups()
        # self.test_extraction()

    def extract_all_zips(self):
        """Extract all ZIP files to their current directory"""
        for zip_path in self.zip_dir.glob("*.zip"):
            if self._is_fao_zip(zip_path):
                extract_dir = zip_path.parent / zip_path.stem

                if extract_dir.exists():
                    """If the directory already exists, we assume it's already extracted"""
                    # logger.info(f"✅ Already extracted: {zip_path.name}")
                else:
                    # logger.info(f"📦 Extracting: {zip_path.name}")
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        zf.extractall(extract_dir)

    def _is_fao_zip(self, zip_path: Path) -> bool:
        """Check if this looks like an FAO data zip"""
        name = zip_path.name.lower()
        # Extract ALL zips for now to ensure we don't miss anything
        return name.endswith(".zip")

    def extract_all_lookups(self):
        """Extract all lookup data - prioritizing values found in datasets"""
        logger.info("🔍 Starting full lookup extraction...")

        # Initialize storage
        lookup_data = {}
        for key, mapping in LOOKUP_MAPPINGS.items():
            lookup_name = mapping["lookup_name"]
            lookup_data[lookup_name] = {"core": {}, "additional": {}}

        # PHASE 1: Process dataset files FIRST (these are the values actually used)
        logger.info("\n📊 Phase 1: Extracting lookups from dataset files...")
        self._process_dataset_files(lookup_data)

        # Log what we found
        for lookup_name, data in lookup_data.items():
            if data["core"]:
                logger.info(f"  Found {len(data['core'])} {lookup_name} from datasets")

        # PHASE 2: Supplement with lookup CSV files (better descriptions, additional columns)
        logger.info("\n📋 Phase 2: Supplementing from lookup CSV files...")
        self._process_lookup_files(lookup_data)

        # Save synthetic CSV files
        self._save_synthetic_csvs(lookup_data)

        return lookup_data

    def _extract_with_additional_columns(self, df: pd.DataFrame, mapping: Dict, lookup_data: Dict, csv_file: Path):
        """Extract lookup data including additional columns"""
        lookup_name = mapping["lookup_name"]

        # Find primary key and description columns
        pk_col = None
        desc_col = None

        for pk_variation in mapping["primary_key_variations"]:
            if pk_variation in df.columns:
                pk_col = pk_variation
                break

        for desc_variation in mapping["description_variations"]:
            if desc_variation in df.columns:
                desc_col = desc_variation
                break

        if pk_col and desc_col:
            # Find which additional columns exist
            found_additional = {}
            for output_col, input_variations in mapping.get("additional_columns", {}).items():
                for variation in input_variations:
                    if variation in df.columns:
                        found_additional[output_col] = variation
                        break

            # Build column list for extraction
            extract_cols = [pk_col, desc_col] + list(found_additional.values())
            unique_data = df[extract_cols].drop_duplicates()

            new_entries = 0
            for _, row in unique_data.iterrows():
                pk_value = str(row[pk_col]).strip()
                desc_value = str(row[desc_col]).strip()

                if pk_value and pk_value != "nan":
                    # Store core data
                    if pk_value not in lookup_data[lookup_name]["core"]:
                        new_entries += 1
                    lookup_data[lookup_name]["core"][pk_value] = desc_value

                    # Store additional columns
                    if pk_value not in lookup_data[lookup_name]["additional"]:
                        lookup_data[lookup_name]["additional"][pk_value] = {}

                    for output_col, input_col in found_additional.items():
                        value = str(row[input_col]).strip()
                        if value and value != "nan":
                            lookup_data[lookup_name]["additional"][pk_value][output_col] = value

            if new_entries > 0:
                logger.debug(f"  ✓ Found {new_entries} new {lookup_name}")

    def _process_lookup_files(self, lookup_data: Dict):
        """Process dedicated lookup CSV files like AreaCodes.csv"""
        lookup_file_patterns = {
            "areas": ["areacodes", "area_codes"],
            "items": ["itemcodes", "item_codes"],
            "elements": ["elements"],
            "flags": ["flags"],
            "currencies": ["currencys", "currencies"],  # Handle typo
            "sources": ["sources"],
            "purposes": ["purposes"],
            "indicators": ["indicators"],
            "surveys": ["surveys"],
            "sex": ["sex"],
            "population_age_groups": ["population_age_group", "populationagegroup"],
            "food_groups": ["foodgroups", "food_groups"],
            "donors": ["donors"],
            "geographic_levels": ["geographiclevels", "geographic_levels"],
        }

        for extract_dir in self.zip_dir.iterdir():
            if extract_dir.is_dir() and not extract_dir.name.startswith("."):
                for csv_file in extract_dir.rglob("*.csv"):
                    file_lower = csv_file.name.lower()

                    # Check if this is a lookup file
                    for lookup_type, patterns in lookup_file_patterns.items():
                        if any(pattern in file_lower for pattern in patterns):
                            logger.info(f"  📄 Found {lookup_type} file: {csv_file.name}")
                            self._process_single_lookup_file(csv_file, lookup_type, lookup_data)
                            break

    def _process_single_lookup_file(self, csv_file: Path, lookup_type: str, lookup_data: Dict):
        """Process a single lookup CSV file"""
        # Find the mapping
        mapping = None
        for key, map_data in LOOKUP_MAPPINGS.items():
            if map_data["lookup_name"] == lookup_type:
                mapping = map_data
                break

        if not mapping:
            logger.warning(f"    ⚠️  No mapping found for {lookup_type}")
            return

        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, dtype=str, encoding=encoding)
                df.columns = df.columns.str.strip()

                # Process all rows (not just unique pairs)
                self._extract_with_additional_columns(df, mapping, lookup_data, csv_file)

                logger.info(f"    ✓ Processed {len(df)} rows")
                break

            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"    ⚠️  Error: {e}")
                break

    def _process_dataset_files(self, lookup_data: Dict):
        """Process dataset files for any additional lookup values"""
        # This is your existing extraction logic
        total_files = 0
        for extract_dir in self.zip_dir.iterdir():
            if extract_dir.is_dir() and not extract_dir.name.startswith("."):
                for csv_file in extract_dir.rglob("*.csv"):
                    # Skip if this is a lookup file
                    # Skip if this is a lookup file
                    lookup_patterns = [
                        "areacodes",
                        "area_codes",
                        "elements",
                        "flags",
                        "currencys",
                        "itemcodes",
                        "item_codes",
                        "sources",
                        "purposes",
                        "indicators",
                        "surveys",
                        "sex",
                        "population",
                        "foodgroups",
                        "food_groups",
                        "donors",
                        "geographiclevels",
                        "geographic_levels",
                    ]
                    if any(pattern in csv_file.name.lower() for pattern in lookup_patterns):
                        continue

                    # Process dataset file
                    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                    for encoding in encodings:
                        try:
                            df = pd.read_csv(csv_file, dtype=str, encoding=encoding)
                            df.columns = df.columns.str.strip()

                            for key, mapping in LOOKUP_MAPPINGS.items():
                                self._extract_with_additional_columns(df, mapping, lookup_data, csv_file)

                            total_files += 1
                            if total_files % 50 == 0:
                                logger.info(f"  ⏳ Processed {total_files} dataset files...")
                            break

                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            logger.warning(f"  ⚠️  Error: {e}")
                            break

        logger.info(f"  ✅ Processed {total_files} dataset files")

    def _save_synthetic_csvs(self, lookup_data: Dict):
        """Save extracted lookup data as synthetic CSV files"""
        output_dir = self.zip_dir / "synthetic_lookups"
        output_dir.mkdir(exist_ok=True)

        logger.info(f"\n💾 Saving synthetic lookup CSVs to {output_dir}")

        for key, mapping in LOOKUP_MAPPINGS.items():
            lookup_name = mapping["lookup_name"]
            data = lookup_data[lookup_name]

            if data["core"]:
                # Build DataFrame
                rows = []
                output_cols = mapping["output_columns"]

                for pk, desc in data["core"].items():
                    row = {output_cols["pk"]: pk, output_cols["desc"]: desc}

                    # Add additional columns
                    if pk in data["additional"]:
                        row.update(data["additional"][pk])

                    rows.append(row)

                # Create DataFrame with proper column order
                df = pd.DataFrame(rows)
                columns = [output_cols["pk"], output_cols["desc"]]

                # Add additional columns in order
                for add_col in mapping.get("additional_columns", {}).keys():
                    if add_col in df.columns:
                        columns.append(add_col)

                # Save to CSV
                output_file = output_dir / f"{lookup_name}.csv"
                df[columns].to_csv(output_file, index=False)

                logger.info(f"  ✅ Saved {lookup_name}.csv ({len(df)} rows)")
            else:
                logger.info(f"  ⏭️  Skipped {lookup_name}.csv (no data found)")


# In lookup_extractor.py, add after the class definition starts:

LOOKUP_MAPPINGS = {
    "areas": {
        "lookup_name": "areas",
        "primary_key_variations": ["Area Code"],
        "description_variations": ["Area"],
        "output_columns": {"pk": "Area Code", "desc": "Area"},
        "additional_columns": {
            "Area Code (M49)": ["Area Code (M49)", "M49 Code"],
        },
    },
    "items": {
        "lookup_name": "items",
        "primary_key_variations": ["Item Code"],
        "description_variations": ["Item"],
        "output_columns": {"pk": "Item Code", "desc": "Item"},
        "additional_columns": {
            "Item Code (CPC)": ["Item Code (CPC)", "CPC Code"],
            "Item Code (FBS)": ["Item Code (FBS)"],
            "Item Code (SDG)": ["Item Code (SDG)"],
        },
    },
    "elements": {
        "lookup_name": "elements",
        "primary_key_variations": ["Element Code"],
        "description_variations": ["Element"],
        "output_columns": {"pk": "Element Code", "desc": "Element"},
        "additional_columns": {},
    },
    "population_groups": {
        "lookup_name": "population_age_groups",
        "primary_key_variations": ["Population Age Group Code", "Population Group Code"],
        "description_variations": ["Population Age Group", "Population Group", "Population Age Group.1"],
        "output_columns": {"pk": "Population Age Group Code", "desc": "Population Age Group"},
        "additional_columns": {},
    },
    "sex": {
        "lookup_name": "sex",
        "primary_key_variations": ["Sex Code"],
        "description_variations": ["Sex"],
        "output_columns": {"pk": "Sex Code", "desc": "Sex"},
        "additional_columns": {},
    },
    "flags": {
        "lookup_name": "flags",
        "primary_key_variations": ["Flag"],
        "description_variations": ["Description"],
        "output_columns": {"pk": "Flag", "desc": "Description"},
        "additional_columns": {},
    },
    "currencies": {
        "lookup_name": "currencies",
        "primary_key_variations": ["ISO Currency Code"],
        "description_variations": ["Currency"],
        "output_columns": {"pk": "ISO Currency Code", "desc": "Currency"},
        "additional_columns": {},
    },
    "sources": {
        "lookup_name": "sources",
        "primary_key_variations": ["Source Code"],
        "description_variations": ["Source"],
        "output_columns": {"pk": "Source Code", "desc": "Source"},
        "additional_columns": {},
    },
    "surveys": {
        "lookup_name": "surveys",
        "primary_key_variations": ["Survey Code"],
        "description_variations": ["Survey"],
        "output_columns": {"pk": "Survey Code", "desc": "Survey"},
        "additional_columns": {},
    },
    "indicators": {
        "lookup_name": "indicators",
        "primary_key_variations": ["Indicator Code"],
        "description_variations": ["Indicator"],
        "output_columns": {"pk": "Indicator Code", "desc": "Indicator"},
        "additional_columns": {},
    },
    "purposes": {
        "lookup_name": "purposes",
        "primary_key_variations": ["Purpose Code"],
        "description_variations": ["Purpose"],
        "output_columns": {"pk": "Purpose Code", "desc": "Purpose"},
        "additional_columns": {},
    },
    "donors": {
        "lookup_name": "donors",
        "primary_key_variations": ["Donor Code"],
        "description_variations": ["Donor"],
        "output_columns": {"pk": "Donor Code", "desc": "Donor"},
        "additional_columns": {
            "Donor Code (M49)": ["Donor Code (M49)"],
        },
    },
    "food_groups": {
        "lookup_name": "food_groups",
        "primary_key_variations": ["Food Group Code"],
        "description_variations": ["Food Group"],
        "output_columns": {"pk": "Food Group Code", "desc": "Food Group"},
        "additional_columns": {},
    },
    "geographic_levels": {
        "lookup_name": "geographic_levels",
        "primary_key_variations": ["Geographic Level Code"],
        "description_variations": ["Geographic Level"],
        "output_columns": {"pk": "Geographic Level Code", "desc": "Geographic Level"},
        "additional_columns": {},
    },
}
