import pandas as pd
from abc import ABC, abstractmethod
from sqlalchemy import text, func
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Dict, List, Optional, Type
from ..utils import load_csv, generate_numeric_id, calculate_optimal_chunk_size
from ..database import run_with_session
from ..system_models import PipelineProgress


class BaseETL(ABC):
    """Base class for all ETL pipelines"""

    def __init__(self, csv_path: str, model_class: Type, table_name: str):
        self.csv_path = csv_path
        self.model_class = model_class
        self.table_name = table_name

    def load(self) -> pd.DataFrame:
        """Load the CSV file - common for all pipelines"""
        return load_csv(self.csv_path)

    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare data - must be implemented by subclasses"""
        pass

    @abstractmethod
    def insert(self, df: pd.DataFrame, session: Session) -> None:
        """Insert data - different for datasets vs references"""
        pass

    def run(self, db: Session) -> None:
        """Run the complete ETL pipeline - common for all"""
        df = self.load()
        df = self.clean(df)
        self.insert(df, db)


class BaseLookupETL(BaseETL):
    """Base class for reference table ETL pipelines"""

    def __init__(self, csv_path: str, model_class: Type, table_name: str, hash_columns: List[str], pk_column: str):
        super().__init__(csv_path, model_class, table_name)
        self.hash_columns = hash_columns
        self.pk_column = pk_column

    def base_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Common cleaning for all references"""
        if df.empty:
            print(f"No {self.table_name} data to clean.")
            return df

        print(f"\nCleaning {self.table_name} data...")
        initial_count = len(df)

        # Replace 'nan' strings with None
        df = df.replace({"nan": None, "NaN": None, "NAN": None})

        # Remove duplicates
        df = df.drop_duplicates(subset=self.hash_columns, keep="first")

        # Drop rows with null PKs
        df = df.dropna(subset=[self.pk_column])

        final_count = len(df)
        print(f"  Cleaned: {initial_count} → {final_count} rows")
        return df

    def insert(self, df: pd.DataFrame, session: Session) -> None:
        """Common insert logic for references"""
        if df.empty:
            print(f"No {self.table_name} data to insert.")
            return

        print(f"\nInserting {self.table_name} data...")

        records = []
        for _, row in df.iterrows():
            record = self.build_record(row)
            record["id"] = generate_numeric_id(row.to_dict(), self.hash_columns)
            records.append(record)

        if records:
            try:
                stmt = pg_insert(self.model_class).values(records)
                stmt = stmt.on_conflict_do_nothing()
                result = session.execute(stmt)
                session.commit()
                print(f"  ✅ Inserted {result.rowcount} rows")
            except Exception as e:
                print(f"  ❌ Error during bulk insert: {e}")
                session.rollback()
                raise

        print(f"✅ {self.table_name} insert complete")

    @abstractmethod
    def build_record(self, row: pd.Series) -> Dict:
        """Build record dict from row - implement in subclass"""
        pass


class BaseDatasetETL(BaseETL):
    """Base class for dataset ETL pipelines"""

    def __init__(
        self,
        csv_path: str,
        model_class: Type,
        table_name: str,
        column_renames: Optional[Dict] = None,
        exclude_columns: Optional[List[str]] = None,
        foreign_keys: Optional[List[Dict]] = None,
    ):
        super().__init__(csv_path, model_class, table_name)
        self.column_renames = column_renames or {}
        self.exclude_columns = exclude_columns or []
        self.foreign_keys = foreign_keys or []

    def base_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Common cleaning for all datasets"""
        if df.empty:
            print(f"No {self.table_name} data to clean.")
            return df

        print(f"\nCleaning {self.table_name} data...")
        initial_count = len(df)

        # Replace 'nan' strings with None
        df = df.replace({"nan": None, "NaN": None, "NAN": None})

        # Rename columns if needed
        if self.column_renames:
            df = df.rename(columns=self.column_renames)
            print(f"  Renamed columns: {list(self.column_renames.keys())} → {list(self.column_renames.values())}")

        # Generate foreign key hash IDs
        if self.foreign_keys:
            dataset_name = self.table_name
            for fk in self.foreign_keys:
                df[fk["hash_fk_sql_column_name"]] = df[fk["reference_pk_csv_column"]].apply(
                    lambda val: (
                        generate_numeric_id(
                            {col: dataset_name if col == "source_dataset" else str(val) for col in fk["hash_columns"]},
                            fk["hash_columns"],
                        )
                        if pd.notna(val) and str(val).strip()
                        else None
                    )
                )

        # Drop excluded columns
        columns_to_drop = [col for col in self.exclude_columns if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
            print(f"  Dropped redundant columns: {columns_to_drop}")

        # Remove duplicates
        df = df.drop_duplicates()

        final_count = len(df)
        print(f"  Cleaned: {initial_count} → {final_count} rows")
        return df

    def get_resume_position(self, session) -> int:
        """Get the last successfully processed row"""
        result = session.execute(
            text(
                """
                SELECT last_row_processed 
                FROM pipeline_progress 
                WHERE table_name = :table_name 
                AND status = 'in_progress'
            """
            ),
            {"table_name": self.table_name},
        ).fetchone()

        return result[0] if result else 0

    def update_progress(self, session, last_row, total_rows, status="in_progress"):
        """Update progress tracking"""
        # Check if record exists
        progress = session.query(PipelineProgress).filter_by(table_name=self.table_name).first()

        if progress:
            # Update existing
            progress.last_row_processed = last_row
            progress.total_rows = total_rows
            progress.status = status
            progress.last_chunk_time = func.now()
        else:
            # Create new
            progress = PipelineProgress(
                table_name=self.table_name, last_row_processed=last_row, total_rows=total_rows, status=status
            )
            session.add(progress)

        session.commit()

    def insert(self, df: pd.DataFrame, session: Session) -> None:
        """Common insert logic for datasets with chunking"""
        if df.empty:
            print(f"No {self.table_name} data to insert.")
            return

        # Check for resume point
        start_row = self.get_resume_position(session)
        original_total = len(df)

        if start_row > 0:
            print(f"📍 Resuming {self.table_name} from row {start_row:,}/{original_total:,}")
            df = df.iloc[start_row:]
            if df.empty:
                print(f"✅ {self.table_name} already complete!")
                self.update_progress(session, original_total, original_total, status="completed")
                return

        # Calculate chunk size
        chunk_size = calculate_optimal_chunk_size(df, base_chunk_size=40000)
        print(f"\nInserting {self.table_name} data ({len(df):,} rows remaining)")
        print(f"  Using chunk size: {chunk_size:,} rows")

        total_inserted = 0

        for chunk_idx, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk_end = min(chunk_start + chunk_size, len(df))
            chunk_df = df.iloc[chunk_start:chunk_end]

            # Calculate absolute position
            absolute_position = start_row + chunk_end

            records = []
            for _, row in chunk_df.iterrows():
                record = self.build_record(row)
                records.append(record)

            if records:
                try:
                    stmt = pg_insert(self.model_class).values(records)
                    stmt = stmt.on_conflict_do_nothing()
                    result = session.execute(stmt)
                    session.commit()

                    total_inserted += result.rowcount

                    # Update progress after each chunk
                    self.update_progress(session, absolute_position, original_total)

                    print(
                        f"  Chunk {chunk_idx + 1}: Inserted {result.rowcount} rows "
                        + f"(Progress: {absolute_position:,}/{original_total:,} - "
                        + f"{(absolute_position/original_total*100):.1f}%)"
                    )

                except Exception as e:
                    print(f"  ❌ Error at row {absolute_position:,}: {e}")
                    session.rollback()
                    raise

        # Mark as complete
        self.update_progress(session, original_total, original_total, status="completed")
        print(f"✅ {self.table_name} complete: {total_inserted:,} rows inserted")

    @abstractmethod
    def build_record(self, row: pd.Series) -> Dict:
        """Build record dict from row - implement in subclass"""
        pass
