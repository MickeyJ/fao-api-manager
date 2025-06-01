import re
import logging

SMALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\small"
MEDIUM_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\medium"
LARGE_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\large"
ALL_ZIP_EXAMPLE = r"C:\Users\18057\Documents\Data\fao-test-zips\all"

ZIP_PATH = ALL_ZIP_EXAMPLE

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def to_snake_case(text: str) -> str:
    """Convert text to snake_case"""
    # Remove parentheses and their contents
    text = re.sub(r"\([^)]*\)", "", text)

    # Handle camelCase and PascalCase
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", text)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # Clean up and convert to lowercase
    result = s2.replace("-", "_").lower()
    # Remove multiple underscores
    result = re.sub("_+", "_", result)
    return result.strip("_")


def snake_to_pascal_case(snake_str: str) -> str:
    """Convert snake_case to PascalCase"""
    return "".join(word.capitalize() for word in snake_str.split("_"))


class ForeignKeyRule:
    def __init__(self, model_name: str, column_name: str):
        self.model_name = model_name
        self.column_name = column_name


class ColumnRule:
    def __init__(
        self,
        name: str,
        sql_type: str | None = None,
        nullable: bool | None = None,
        is_primary_key: bool = False,
        foreign_key_model_name: str | None = None,
    ):
        self.name = name
        self.sql_type = sql_type
        self.nullable = nullable
        self.is_primary_key = is_primary_key
        self.foreign_key_model_name = foreign_key_model_name
        self.is_foreign_key = foreign_key_model_name is not None


class ModelRule:
    def __init__(
        self,
        model_name: str,
        primary_key: str = "",
        foreign_keys: list | None = None,
        unique_constraints: list | None = None,  # [["col1", "col2"], ["col3"]]
        indexes: list | None = None,  # ["col1", "col2"]
        column_rules: list[ColumnRule] | None = None,  # List of ColumnRule objects
    ):
        self.model_name = model_name
        self.use_as_primary_key = primary_key
        self.foreign_keys = foreign_keys or []  # Avoid mutable default
        self.unique_constraints = unique_constraints or []  # Avoid mutable default
        self.indexes = indexes or []  # Avoid mutable default
        self.column_rules = column_rules or []


class PipelineRule:
    def __init__(self, name: str, skip_models: list = [], chunk_size: int = 5000):
        self.name = name
        self.skip_models = skip_models or []
        self.chunk_size = chunk_size
