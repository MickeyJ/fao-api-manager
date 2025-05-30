import zipfile
from io import StringIO
import pandas as pd
from pathlib import Path
from typing import Dict, List


class CSVAnalyzer:

    def analyze_csv_from_zip(self, zip_path: Path, csv_filename: str) -> Dict:
        """Analyze a CSV file directly from inside a ZIP"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(csv_filename) as csv_file:
                # Try different encodings
                encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                for encoding in encodings:
                    try:
                        csv_file.seek(0)  # Reset file pointer
                        csv_data = csv_file.read().decode(encoding)
                        df = pd.read_csv(StringIO(csv_data), dtype=str)

                        df.columns = df.columns.str.strip()

                        sample_size = 5
                        sample_rows = df.head(sample_size).to_dict(
                            "records"
                        )  # List of dictionaries

                        return {
                            "file_name": csv_filename,
                            "row_count": len(df),
                            "column_count": len(df.columns),
                            "columns": df.columns.tolist(),
                            "encoding_used": encoding,
                            "sample_rows": sample_rows,
                        }
                    except UnicodeDecodeError:
                        continue

                # If all encodings fail, use errors='ignore'
                csv_file.seek(0)
                csv_data = csv_file.read().decode("utf-8", errors="ignore")
                df = pd.read_csv(StringIO(csv_data), dtype=str)

                sample_size = 5
                sample_rows = df.head(sample_size).to_dict(
                    "records"
                )  # List of dictionaries

                return {
                    "file_name": csv_filename,
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "columns": df.columns.tolist(),
                    "encoding_used": "utf-8 (with errors ignored)",
                    "sample_rows": sample_rows,
                }


# Test usage
if __name__ == "__main__":
    analyzer = CSVAnalyzer()
    # Replace with actual path to test
    # result = analyzer.analyze_csv_from_zip("some_fao_file.csv")
    # print(result)
