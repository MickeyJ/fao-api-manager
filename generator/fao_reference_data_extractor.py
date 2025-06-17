# generator/reference_extractor.py
import zipfile
from pathlib import Path
from typing import Dict
import pandas as pd
from generator.structure import Structure
from .logger import logger


class FAOReferenceDataExtractor:
    """Extract reference data tables from dataset files and create synthetic CSVs"""

    def __init__(self, zip_directory: str | Path, json_cache_path: Path):
        self.zip_dir = Path(zip_directory)
        self.analysis_dir = Path("./cache")
        self.json_cache_path = json_cache_path
        self.structure = Structure()
        self.extracted_references = {}  # Will store discovered references

    def run(self):
        """Main entry point - extract and analyze everything"""

        # if self.json_cache_path.exists():
        #     logger.info(f"📁 Using cached module structure from {self.json_cache_path}")
        #     return

        logger.info("🚀 Starting reference data extraction process...")

        self.extract_all_zips()
        self.create_all_synth_references()

    def extract_all_zips(self):
        """Extract all ZIP files to their current directory"""
        for zip_path in self.zip_dir.glob("*.zip"):
            if self._is_fao_zip(zip_path):
                extract_dir = zip_path.parent / zip_path.stem

                if extract_dir.exists():
                    """If the directory already exists, we assume it's already extracted"""
                    logger.info(f"✅ Already extracted: {zip_path.name}")
                else:
                    logger.info(f"📦 Extracting: {zip_path.name}")
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        zf.extractall(extract_dir)

    def _is_fao_zip(self, zip_path: Path) -> bool:
        """Check if this looks like an FAO data zip"""
        name = zip_path.name.lower()
        # Extract ALL zips for now to ensure we don't miss anything
        return name.endswith(".zip")

    def create_all_synth_references(self):
        """Extract all reference data - prioritizing values found in datasets"""
        logger.info("🔍 Starting full reference extraction...")

        # Initialize storage
        reference_data = {}
        for key, mapping in REFERENCE_MAPPINGS.items():
            reference_name = mapping["reference_name"]
            reference_data[reference_name] = {"rows": []}

        # PHASE 1: Process dataset files FIRST (these are the values actually used)
        logger.info("\n📊 Phase 1: Extracting references from all csv files...")
        self._process_all_csv_files(reference_data)

        # Log what we found
        for reference_name, data in reference_data.items():
            if data["rows"]:  # Changed from data["core"]
                logger.info(f"  Found {len(data['rows'])} rows for {reference_name}")

        # PHASE 2: Supplement with reference CSV files (better descriptions, additional columns)
        # logger.info("\n📋 Phase 2: Supplementing from reference CSV files...")
        # self._process_reference_files(reference_data)

        # Save synthetic CSV files
        self._save_synthetic_csvs(reference_data)

        return reference_data

    def _extract_with_additional_columns(self, df: pd.DataFrame, mapping: Dict, reference_data: Dict, csv_file: Path):
        """Extract reference data including additional columns"""
        reference_name = mapping["reference_name"]

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

            # Initialize rows list if not exists
            if "rows" not in reference_data[reference_name]:
                reference_data[reference_name]["rows"] = []

            new_entries = 0
            for _, row in unique_data.iterrows():
                dataset_name = self.structure.extract_module_name(csv_file.name)

                # Build row dictionary with actual column names from the mapping
                row_dict = {}

                # Primary key
                pk_value = str(row[pk_col]).strip()
                if pk_value and pk_value != "nan":
                    row_dict[mapping["output_columns"]["pk"]] = pk_value  # Use standardized name

                    # Description
                    desc_value = str(row[desc_col]).strip()
                    row_dict[mapping["output_columns"]["desc"]] = desc_value  # Use standardized name

                    # Additional columns
                    for output_col, input_col in found_additional.items():
                        value = str(row[input_col]).strip()
                        if value and value != "nan":
                            row_dict[output_col] = value

                    # ADD SOURCE DATASET COLUMN
                    row_dict["source_dataset"] = dataset_name

                    # TODO: processing for months reference file
                    # df["Month Number"] = df["Months Code"].apply(
                    #     lambda x: int(str(x)[-2:]) if str(x).startswith("70") and x != "7021" else None
                    # )
                    # df["Quarter"] = df["Month Number"].apply(lambda x: f"Q{((x-1)//3)+1}" if x else None)

                    # Add row to list
                    reference_data[reference_name]["rows"].append(row_dict)

                    new_entries += 1

            if new_entries > 0:
                logger.debug(f"  ✓ Found {new_entries} rows for {reference_name}")

    def _process_all_csv_files(self, reference_data: Dict):
        """Process dataset files for any additional reference values"""
        # This is your existing extraction logic
        total_files = 0
        for extract_dir in self.zip_dir.iterdir():
            if (
                extract_dir.is_dir()
                and not extract_dir.name.startswith(".")
                and not extract_dir.name.startswith("synthetic_references")
            ):
                logger.info(f"  📁 {extract_dir.name}")
                for csv_file in extract_dir.rglob("*.csv"):
                    dataset_file = csv_file.name.lower().find("all_data") >= 0
                    flag_file = csv_file.name.lower().find("flags") >= 0

                    # Skip if it's NEITHER a dataset NOR a flag file
                    if not dataset_file and not flag_file:
                        continue

                    # Process dataset file
                    encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                    logger.info(f"      🧻 {csv_file.name}")
                    for encoding in encodings:
                        try:

                            df = pd.read_csv(csv_file, dtype=str, encoding=encoding)
                            df.columns = df.columns.str.strip()

                            for key, mapping in REFERENCE_MAPPINGS.items():
                                self._extract_with_additional_columns(df, mapping, reference_data, csv_file)

                            total_files += 1
                            if total_files % 25 == 0:
                                logger.info(f"  ⏳ Processed {total_files} dataset files...\n")
                            break

                        except UnicodeDecodeError:
                            continue
                        except Exception as e:
                            logger.critical(f"  ⚠️  Error: {e}")
                            raise e

        logger.info(f"  ✅ Processed {total_files} dataset files")

    def _save_synthetic_csvs(self, reference_data: Dict):
        """Save extracted reference data as synthetic CSV files"""
        output_dir = self.zip_dir / "synthetic_references"
        output_dir.mkdir(exist_ok=True)

        logger.info(f"\n💾 Saving synthetic reference CSVs to {output_dir}")

        for key, mapping in REFERENCE_MAPPINGS.items():
            reference_name = mapping["reference_name"]
            data = reference_data[reference_name]

            if data["rows"]:
                df = pd.DataFrame(data["rows"])

                # Remove any duplicate columns that pandas created
                # Keep only columns without .1, .2, etc. suffixes
                original_cols = [col for col in df.columns if not any(col.endswith(f".{i}") for i in range(1, 10))]
                df = df[original_cols]

                # Rename columns to standardized output names
                output_cols = mapping["output_columns"]
                rename_map = {}

                # Find and rename the PK and description columns
                for col in df.columns:
                    if col in mapping["primary_key_variations"]:
                        rename_map[col] = output_cols["pk"]
                    elif col in mapping["description_variations"]:
                        rename_map[col] = output_cols["desc"]

                df = df.rename(columns=rename_map)

                # Drop complete duplicate rows
                df = df.drop_duplicates()

                # Order columns: PK, Description, then additional columns
                columns = [output_cols["pk"], output_cols["desc"]]

                # Add additional columns in order
                for add_col in mapping.get("additional_columns", {}).keys():
                    if add_col in df.columns:
                        columns.append(add_col)

                # Add any other columns that might exist
                for col in df.columns:
                    if col not in columns:
                        columns.append(col)

                # Save to CSV with proper column order
                output_file = output_dir / f"{reference_name}.csv"
                df[columns].to_csv(output_file, index=False)

                logger.info(f"  ✅ Saved {reference_name}.csv ({len(df)} rows)")
            else:
                logger.info(f"  ⏭️  Skipped {reference_name}.csv (no data found)")


# In reference_extractor.py, add after the class definition starts:

REFERENCE_MAPPINGS = {
    "area_codes": {
        "reference_name": "area_codes",
        "primary_key_variations": ["Area Code"],
        "description_variations": ["Area"],
        "output_columns": {"pk": "Area Code", "desc": "Area"},
        "additional_columns": {"Area Code (M49)": ["Area Code (M49)"]},
        "hash_columns": ["Area Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_area_code",
        "exception_func": "invalid_area_code",
    },
    "reporter_country_codes": {
        "reference_name": "reporter_country_codes",
        "primary_key_variations": ["Reporter Country Code"],
        "description_variations": ["Reporter Countries"],
        "output_columns": {"pk": "Reporter Country Code", "desc": "Reporter Countries"},
        "additional_columns": {"Reporter Country Code (M49)": ["Reporter Country Code (M49)"]},
        "hash_columns": ["Reporter Country Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_reporter_country_code",
        "exception_func": "invalid_reporter_country_code",
    },
    "partner_country_codes": {
        "reference_name": "partner_country_codes",
        "primary_key_variations": ["Partner Country Code"],
        "description_variations": ["Partner Countries"],
        "output_columns": {"pk": "Partner Country Code", "desc": "Partner Countries"},
        "additional_columns": {"Partner Country Code (M49)": ["Partner Country Code (M49)"]},
        "hash_columns": ["Partner Country Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_partner_country_code",
        "exception_func": "invalid_partner_country_code",
    },
    "recipient_country_codes": {
        "reference_name": "recipient_country_codes",
        "primary_key_variations": ["Recipient Country Code"],
        "description_variations": ["Recipient Country"],
        "output_columns": {"pk": "Recipient Country Code", "desc": "Recipient Country"},
        "additional_columns": {"Recipient Country Code (M49)": ["Recipient Country Code (M49)"]},
        "hash_columns": ["Recipient Country Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_recipient_country_code",
        "exception_func": "invalid_recipient_country_code",
    },
    "item_codes": {
        "reference_name": "item_codes",
        "primary_key_variations": ["Item Code"],
        "description_variations": ["Item"],
        "output_columns": {"pk": "Item Code", "desc": "Item"},
        "additional_columns": {
            "Item Code (CPC)": ["Item Code (CPC)"],
            "Item Code (FBS)": ["Item Code (FBS)"],
            "Item Code (SDG)": ["Item Code (SDG)"],
        },
        "hash_columns": ["Item Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_item_code",
        "exception_func": "invalid_item_code",
    },
    "elements": {
        "reference_name": "elements",
        "primary_key_variations": ["Element Code"],
        "description_variations": ["Element"],
        "output_columns": {"pk": "Element Code", "desc": "Element"},
        "additional_columns": {},
        "hash_columns": ["Element Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_element_code",
        "exception_func": "invalid_element_code",
    },
    "flags": {
        "reference_name": "flags",
        "primary_key_variations": ["Flag"],
        "description_variations": ["Description"],
        "output_columns": {"pk": "Flag", "desc": "Description"},
        "additional_columns": {},
        "hash_columns": ["Flag"],
        "format_methods": {
            "Flag": ["upper"],
        },
        "validation_func": "is_valid_flag",
        "exception_func": "invalid_flag",
    },
    "currencies": {
        "reference_name": "currencies",
        "primary_key_variations": ["ISO Currency Code"],
        "description_variations": ["Currency"],
        "output_columns": {"pk": "ISO Currency Code", "desc": "Currency"},
        "additional_columns": {},
        "hash_columns": ["ISO Currency Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_currency_code",
        "exception_func": "invalid_currency_code",
    },
    "sources": {
        "reference_name": "sources",
        "primary_key_variations": ["Source Code"],
        "description_variations": ["Source"],
        "output_columns": {"pk": "Source Code", "desc": "Source"},
        "additional_columns": {},
        "hash_columns": ["Source Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_source_code",
        "exception_func": "invalid_source_code",
    },
    "releases": {
        "reference_name": "releases",
        "primary_key_variations": ["Release Code"],
        "description_variations": ["Release"],
        "output_columns": {"pk": "Release Code", "desc": "Release"},
        "additional_columns": {},
        "hash_columns": ["Release Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_release_code",
        "exception_func": "invalid_release_code",
    },
    "sexs": {
        "reference_name": "sexs",
        "primary_key_variations": ["Sex Code"],
        "description_variations": ["Sex"],
        "output_columns": {"pk": "Sex Code", "desc": "Sex"},
        "additional_columns": {},
        "hash_columns": ["Sex Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_sex_code",
        "exception_func": "invalid_sex_code",
    },
    "indicators": {
        "reference_name": "indicators",
        "primary_key_variations": ["Indicator Code"],
        "description_variations": ["Indicator"],
        "output_columns": {"pk": "Indicator Code", "desc": "Indicator"},
        "additional_columns": {},
        "hash_columns": ["Indicator Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_indicator_code",
        "exception_func": "invalid_indicator_code",
    },
    "population_groups": {
        "reference_name": "population_age_groups",
        "primary_key_variations": ["Population Age Group Code", "Population Group Code"],
        "description_variations": ["Population Age Group", "Population Group", "Population Age Group.1"],
        "output_columns": {"pk": "Population Age Group Code", "desc": "Population Age Group"},
        "additional_columns": {},
        "hash_columns": ["Population Age Group Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_population_age_group_code",
        "exception_func": "invalid_population_age_group_code",
    },
    "surveys": {
        "reference_name": "surveys",
        "primary_key_variations": ["Survey Code"],
        "description_variations": ["Survey"],
        "output_columns": {"pk": "Survey Code", "desc": "Survey"},
        "additional_columns": {},
        "hash_columns": ["Survey Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_survey_code",
        "exception_func": "invalid_survey_code",
    },
    "purposes": {
        "reference_name": "purposes",
        "primary_key_variations": ["Purpose Code"],
        "description_variations": ["Purpose"],
        "output_columns": {"pk": "Purpose Code", "desc": "Purpose"},
        "additional_columns": {},
        "hash_columns": ["Purpose Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_purpose_code",
        "exception_func": "invalid_purpose_code",
    },
    "donors": {
        "reference_name": "donors",
        "primary_key_variations": ["Donor Code"],
        "description_variations": ["Donor"],
        "output_columns": {"pk": "Donor Code", "desc": "Donor"},
        "additional_columns": {
            "Donor Code (M49)": ["Donor Code (M49)"],
        },
        "hash_columns": ["Donor Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_donor_code",
        "exception_func": "invalid_donor_code",
    },
    "food_groups": {
        "reference_name": "food_groups",
        "primary_key_variations": ["Food Group Code"],
        "description_variations": ["Food Group"],
        "output_columns": {"pk": "Food Group Code", "desc": "Food Group"},
        "additional_columns": {},
        "hash_columns": ["Food Group Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_food_group_code",
        "exception_func": "invalid_food_group_code",
    },
    "geographic_levels": {
        "reference_name": "geographic_levels",
        "primary_key_variations": ["Geographic Level Code"],
        "description_variations": ["Geographic Level"],
        "output_columns": {"pk": "Geographic Level Code", "desc": "Geographic Level"},
        "additional_columns": {},
        "hash_columns": ["Geographic Level Code", "source_dataset"],
        "format_methods": {},
        "validation_func": "is_valid_geographic_level_code",
        "exception_func": "invalid_geographic_level_code",
    },
    # "months": {
    #     "reference_name": "months",
    #     "primary_key_variations": ["Months Code", "Month Code"],
    #     "description_variations": ["Months", "Month"],
    #     "output_columns": {"pk": "Month Code", "desc": "Month"},
    #     "additional_columns": {
    #         "Month Number": ["Month Number"],
    #         "Quarter": ["Quarter"],
    #     },
    #     "hash_columns": ["Month Code"],
    # },
}
