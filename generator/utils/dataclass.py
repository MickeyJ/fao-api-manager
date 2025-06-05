from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class ConflictVariation:
    """One variation of a conflicting row"""

    row_data: Dict
    variation_index: int
    synthetic_pk: Optional[str] = None


@dataclass
class DatasetReference:
    """How a dataset references a conflicting lookup value"""

    dataset_name: str
    expected_description: str  # What's in the dataset's descriptive column
    matched_variation_index: Optional[int] = None  # Which variation matches


@dataclass
class ReferencedConflict:
    """A lookup conflict that's actually referenced by datasets"""

    pk_value: str
    pk_column: str
    variations: List[ConflictVariation]
    dataset_references: List[DatasetReference]  # Which datasets use this PK
    needs_synthetic_pks: bool = False


@dataclass
class LookupConflict:
    """Full conflict info for a lookup table"""

    lookup_name: str
    pk_value: str
    pk_column: str
    desc_column: str
    variations: List[ConflictVariation]
    is_referenced: bool = False
    dataset_references: List[DatasetReference] = field(default_factory=list)


@dataclass
class FAOLookup:
    """Represents a lookup table (areas, items, etc.)"""

    name: str  # "areas", "items"
    primary_key: str  # "Area Code", "Item Code"
    description_col: str  # "Area", "Item"
    sql_table_name: str  # snake_case sqlalchemy table name
    sql_model_name: str  # PascalCase sqlalchemy class name
    file_path: Path
    row_count: int = 0
    columns: List[str] = field(default_factory=list)
    has_conflicts: bool = False
    conflict_count: int = 0
    conflicts: List[LookupConflict] = field(default_factory=list)


@dataclass
class ForeignKeyRelationship:
    """Represents a single FK relationship"""

    dataset_fk_csv_column: str  # "Area Code" - FK column name in dataset CSV
    dataset_fk_sql_column: str  # "area_code" - FK column name in dataset table
    lookup_sql_table: str  # "area_codes" - lookup table name in database
    lookup_sql_model: str  # "AreaCodes" - SQLAlchemy model class name
    lookup_pk_csv_column: str  # "Area Code" - PK column name in lookup CSV
    lookup_pk_sql_column: str  # "area_code" - PK column name in lookup table


@dataclass
class FAODataset:
    """Represents a dataset (prices, production, etc.)"""

    name: str
    directory: Path
    sql_table_name: str  # snake_case sqlalchemy table name
    sql_model_name: str  # PascalCase sqlalchemy class name
    main_data_file: Path
    row_count: int = 0
    columns: List[str] = field(default_factory=list)
    foreign_keys: List[ForeignKeyRelationship] = field(default_factory=list)
    exclude_columns: List[str] = field(default_factory=list)
    sql_all_columns: str = ""  # Columns after exclusions
