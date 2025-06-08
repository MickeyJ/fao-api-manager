# generator/aquastat_preprocessor.py
import pandas as pd
import zipfile
import re
from pathlib import Path
from typing import Optional, Tuple
from generator import logger, ZIP_PATH


class AQUASTATPreprocessor:
    """Transform AQUASTAT data to FAO-compatible format"""

    def __init__(self, input_csv_path: Path, output_base_dir: Path):
        self.input_csv = Path(input_csv_path)
        self.output_base_dir = Path(output_base_dir)
        self.dataset_name = "AQUASTAT_E_All_Data_(Normalized)"
        self.dataset_dir = self.output_base_dir / self.dataset_name

    def run(self, create_zip: bool = True) -> Path:
        """
        Transform AQUASTAT CSV to FAO format

        Returns:
            Path to the created dataset directory
        """
        logger.info(f"🚀 Starting AQUASTAT preprocessing...")
        logger.info(f"  Input: {self.input_csv}")
        logger.info(f"  Output: {self.dataset_dir}")

        # Read and transform data
        df = self._read_aquastat_data()
        df_transformed = self._transform_to_fao_format(df)

        # Create output structure
        self._create_output_structure()

        # Save transformed data
        output_csv = self.dataset_dir / f"{self.dataset_name}.csv"
        df_transformed.to_csv(output_csv, index=False)
        logger.info(f"  ✅ Saved {len(df_transformed)} rows to {output_csv.name}")

        # Optionally create ZIP
        if create_zip:
            self._create_zip_file()

        logger.info(f"✅ AQUASTAT preprocessing complete!")
        return self.dataset_dir

    def _read_aquastat_data(self) -> pd.DataFrame:
        """Read AQUASTAT CSV with proper handling"""
        try:
            # Try different encodings
            encodings = ["utf-8", "latin-1", "cp1252"]
            for encoding in encodings:
                try:
                    df = pd.read_csv(self.input_csv, encoding=encoding)
                    logger.info(f"  📖 Read {len(df)} rows with {encoding} encoding")
                    return df
                except UnicodeDecodeError:
                    continue
            raise ValueError("Could not read file with any encoding")
        except Exception as e:
            logger.error(f"Error reading AQUASTAT file: {e}")
            raise

    def _transform_to_fao_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform AQUASTAT columns to FAO standard format"""
        logger.info("  🔄 Transforming to FAO format...")

        # Handle duplicate column names
        # When pandas reads duplicate columns, it renames them with .1, .2, etc.
        logger.info(f"  Columns found: {list(df.columns)}")

        # Create new dataframe with FAO columns
        fao_df = pd.DataFrame()

        # Transform area codes with prefix
        fao_df["Area Code"] = "aquastat-" + df["REF_AREA"].astype(str)
        fao_df["Area"] = df["AREA"]

        # Handle aquastatElement columns (there are two with same name)
        # First column (position 0) is the code
        fao_df["Element Code"] = df.iloc[:, 0].astype(str)

        # Second column (position 1) is the description with unit
        element_desc_col = df.iloc[:, 1]
        fao_df["Element"], fao_df["Unit"] = zip(*element_desc_col.apply(self._extract_element_and_unit))

        # Handle timePointYears - find the first column that starts with timePointYears
        year_cols = [col for col in df.columns if col.startswith("timePointYears")]
        if year_cols:
            fao_df["Year"] = df[year_cols[0]]
        else:
            # Fallback to column position if name matching fails
            fao_df["Year"] = df.iloc[:, 4]  # Based on sample data structure

        fao_df["Year Code"] = fao_df["Year"].astype(str)
        fao_df["Value"] = df["Value"]

        # Extract flag from complex flag column
        flag_cols = [col for col in df.columns if "flagObservationStatus" in str(col)]
        if flag_cols:
            fao_df["Flag"] = df[flag_cols[0]].apply(self._extract_flag)
        else:
            fao_df["Flag"] = ""

        # Remove any rows with missing critical values
        fao_df = fao_df.dropna(subset=["Area Code", "Element Code", "Year", "Value"])

        logger.info(f"  ✅ Transformed to {len(fao_df)} rows")
        return fao_df

    def _extract_element_and_unit(self, element_str: str) -> Tuple[str, str]:
        """
        Extract element name and unit from description
        e.g., "Total dam capacity [km3]" → ("Total dam capacity", "km3")
        """
        if pd.isna(element_str):
            return "", ""

        # Look for unit in square brackets
        match = re.match(r"^(.*?)\s*\[(.*?)\]$", str(element_str).strip())
        if match:
            element = match.group(1).strip()
            unit = match.group(2).strip()
            return element, unit
        else:
            # No unit found
            return str(element_str).strip(), ""

    def _extract_flag(self, flag_str: str) -> str:
        """
        Extract single letter flag from complex flag string
        e.g., "[flagObservationStatus] E - description" → "E"
        """
        if pd.isna(flag_str) or str(flag_str).strip() == "":
            return ""

        # Look for single capital letter after the bracket part
        match = re.search(r"]\s*([A-Z])\s*[-,]", str(flag_str))
        if match:
            return match.group(1)

        # Fallback: try to find any single capital letter
        match = re.search(r"([A-Z])", str(flag_str))
        if match:
            return match.group(1)

        return ""

    def _create_output_structure(self):
        """Create the FAO-style directory structure"""
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"  📁 Created directory: {self.dataset_dir}")

    def _create_zip_file(self) -> Path:
        """Create a ZIP file matching FAO structure"""
        zip_path = self.output_base_dir / f"{self.dataset_name}.zip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            # Add the CSV file
            csv_file = self.dataset_dir / f"{self.dataset_name}.csv"
            arcname = f"{self.dataset_name}/{self.dataset_name}.csv"
            zf.write(csv_file, arcname)

        logger.info(f"  📦 Created ZIP: {zip_path.name}")
        return zip_path


def main():
    """Example usage"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python aquastat_preprocessor.py <path_to_aquastat_csv>")
        print("Example: python aquastat_preprocessor.py data/aquastat_data.csv")
        sys.exit(1)

    # Configure paths
    input_csv = Path(sys.argv[1])
    output_dir = Path(ZIP_PATH)  # Uses the configured ZIP_PATH from generator.__init__

    if not input_csv.exists():
        print(f"Error: Input file {input_csv} not found")
        sys.exit(1)

    # Run preprocessor
    preprocessor = AQUASTATPreprocessor(input_csv, output_dir)
    preprocessor.run(create_zip=True)


if __name__ == "__main__":
    main()
