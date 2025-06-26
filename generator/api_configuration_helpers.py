# generator/api_configuration_helpers.py
from dataclasses import dataclass, field
from typing import Optional, List, Any, Dict
from enum import Enum


from dataclasses import dataclass, field
from typing import Optional, List, Any, Dict, Set
from enum import Enum


class FilterType(Enum):
    """Types of filters that can be applied to parameters"""

    EXACT = "exact"
    LIKE = "like"
    MULTI = "multi"
    RANGE_MIN = "range_min"
    RANGE_MAX = "range_max"


@dataclass
class ParameterConfig:
    """Configuration for an API endpoint parameter"""

    name: str
    type: str
    description: str

    # Query parameter settings
    query_param: bool = True
    default: Optional[Any] = None
    constraints: Optional[str] = None

    # Filter settings
    filter_type: Optional[FilterType] = None
    filter_column: Optional[str] = None
    filter_model: Optional[str] = None
    joins_table: Optional[str] = None
    range_group: Optional[str] = None

    # Validation
    validation_func: Optional[str] = None

    # Parameter categories
    is_standard: bool = False
    is_option: bool = False

    def __post_init__(self):
        """Set filter_column to name if not specified"""
        if self.filter_type and not self.filter_column:
            self.filter_column = self.name.replace("_min", "").replace("_max", "").replace("_exact", "")


@dataclass
class EndpointParameters:
    """Container for all parameters of an endpoint"""

    standard: List[ParameterConfig] = field(default_factory=list)
    filters: List[ParameterConfig] = field(default_factory=list)
    options: List[ParameterConfig] = field(default_factory=list)

    def all_params(self) -> List[ParameterConfig]:
        """Get all parameters in order"""
        return self.standard + self.filters + self.options

    def get_validation_params(self) -> List[ParameterConfig]:
        """Get parameters that need validation"""
        return [p for p in self.filters if p.validation_func]

    def get_join_tables(self) -> List[str]:
        """Get unique tables that need to be joined"""
        return list(set(p.joins_table for p in self.filters if p.joins_table))


@dataclass
class ImportConfig:
    """Import configuration for a module"""

    module_path: str
    import_name: str
    pipeline_name: Optional[str] = None  # For model imports


@dataclass
class RouterImports:
    """All imports needed for a router"""

    models: List[ImportConfig] = field(default_factory=list)
    validations: List[str] = field(default_factory=list)  # Just function names
    exceptions: List[str] = field(default_factory=list)  # Just function names


def generate_imports(
    project_name: str, module: Dict[str, Any], param_configs: EndpointParameters, reference_modules: dict
) -> RouterImports:
    """Generate all imports needed for the router"""
    imports = RouterImports()

    # Track what we've already added
    seen_models = set()
    seen_validations = set()
    seen_exceptions = set()

    # Always include year validation if we have year fields
    # has_year = any(p.name in ["year", "year_min", "year_max"] for p in param_configs.filters)
    # if has_year:
    #     seen_validations.add("is_valid_year_range")
    #     seen_exceptions.add("invalid_year_range")

    # Process foreign keys for model imports
    for fk in module["model"].get("foreign_keys", []):
        if fk["table_name"] not in seen_models:
            imports.models.append(
                ImportConfig(
                    module_path=f"{project_name}.src.db.pipelines.{fk['pipeline_name']}.{fk['table_name']}_model",
                    import_name=fk["model_name"],
                    pipeline_name=fk["pipeline_name"],
                )
            )
            seen_models.add(fk["table_name"])

    # Process parameters for validation/exception imports
    for param in param_configs.filters:
        if param.validation_func and param.validation_func not in seen_validations:
            # Add validation function
            seen_validations.add(param.validation_func)

            # Add corresponding exception (derive from validation func name)
            exception_name = param.validation_func.replace("is_valid_", "invalid_")
            if exception_name not in seen_exceptions:
                seen_exceptions.add(exception_name)

    # Check if any column names match reference module keys
    # (for cases where column validation is needed even without explicit validation_func)
    for ref_key, ref_data in reference_modules.items():
        ref_column = ref_data["model"]["pk_sql_column_name"]

        # Check if this reference column is used in parameters
        param_names = [p.filter_column for p in param_configs.filters if p.filter_column]
        if ref_column in param_names:
            validation_func = f"is_valid_{ref_column}"
            if validation_func not in seen_validations:
                seen_validations.add(validation_func)
                seen_exceptions.add(f"invalid_{ref_column}")

    # Convert sets to sorted lists
    imports.validations = sorted(seen_validations)
    imports.exceptions = sorted(seen_exceptions)

    return imports


def generate_parameter_configs(module: Dict[str, Any]) -> EndpointParameters:
    """Generate parameter configurations for a module's API endpoint"""
    params = EndpointParameters()

    # Get the default model name for this module
    default_model = module["model"]["model_name"]
    is_reference_module = module.get("is_reference_module", False)

    # Identify code columns
    code_columns = set()

    if is_reference_module:
        # For reference modules, the PK is the code column
        pk_column = module["model"].get("pk_sql_column_name")
        if pk_column:
            code_columns.add(pk_column)

    # For all modules, foreign keys are code columns
    for fk in module["model"].get("foreign_keys", []):
        code_columns.add(fk["sql_column_name"])

    # Add standard parameters
    params.standard = [
        ParameterConfig(
            name="limit",
            type="int",
            default=100,
            constraints="ge=0, le=10000",
            description="Maximum records to return",
            is_standard=True,
            query_param=False,
        ),
        ParameterConfig(
            name="offset",
            type="int",
            default=0,
            constraints="ge=0",
            description="Number of records to skip",
            is_standard=True,
            query_param=False,
        ),
    ]

    # Add foreign key parameters (for datasets only)
    if not is_reference_module:
        for fk in module["model"].get("foreign_keys", []):
            # Code parameter - exact match with validation, supports multiple values
            params.filters.append(
                ParameterConfig(
                    name=fk["sql_column_name"],
                    type="Optional[Union[str, List[str]]]",  # Support both single and multiple
                    description=f"Filter by {fk['sql_column_name']} code (comma-separated for multiple)",
                    filter_type=FilterType.MULTI,
                    filter_model=fk["model_name"],
                    filter_column=fk["sql_column_name"],
                    validation_func=f"is_valid_{fk['sql_column_name']}",
                    joins_table=fk["hash_fk_sql_column_name"],
                )
            )

            # Description filter if available - partial match, no validation
            if fk.get("reference_description_column"):
                params.filters.append(
                    ParameterConfig(
                        name=fk["reference_description_column"],
                        type="Optional[str]",
                        description=f"Filter by {fk['reference_description_column']} description (partial match)",
                        filter_type=FilterType.LIKE,
                        filter_model=fk["model_name"],
                        filter_column=fk["reference_description_column"],
                        joins_table=fk["hash_fk_sql_column_name"],
                    )
                )

    # Add column-based parameters
    for column in module["model"].get("column_analysis", []):
        if column["sql_column_name"] in ["id", "created_at", "updated_at"]:
            continue
        if column.get("csv_column_name") in module["model"].get("exclude_columns", []):
            continue

        col_name = column["sql_column_name"]
        col_type = column["inferred_sql_type"]
        is_code_column = col_name in code_columns

        if col_name == "year":
            # Year is always exact match
            params.filters.extend(
                [
                    ParameterConfig(
                        name="year",
                        type="Optional[int]",
                        description="Filter by exact year",
                        filter_type=FilterType.EXACT,
                        filter_model=default_model,
                        filter_column="year",
                    ),
                    ParameterConfig(
                        name="year_min",
                        type="Optional[int]",
                        description="Minimum year",
                        filter_type=FilterType.RANGE_MIN,
                        filter_model=default_model,
                        filter_column="year",
                        range_group="year",
                    ),
                    ParameterConfig(
                        name="year_max",
                        type="Optional[int]",
                        description="Maximum year",
                        filter_type=FilterType.RANGE_MAX,
                        filter_model=default_model,
                        filter_column="year",
                        range_group="year",
                    ),
                ]
            )

        elif col_type in ["Float", "Integer"]:
            # Numeric fields always get exact match with range options
            base_type = "Optional[Union[float, int]]" if col_type == "Float" else "Optional[int]"
            params.filters.extend(
                [
                    ParameterConfig(
                        name=col_name,
                        type=base_type,
                        description=f"Exact {col_name.replace('_', ' ')}",
                        filter_type=FilterType.EXACT,
                        filter_model=default_model,
                        filter_column=col_name,
                    ),
                    ParameterConfig(
                        name=f"{col_name}_min",
                        type=base_type,
                        description=f"Minimum {col_name.replace('_', ' ')}",
                        filter_type=FilterType.RANGE_MIN,
                        filter_model=default_model,
                        filter_column=col_name,
                        range_group=col_name,
                    ),
                    ParameterConfig(
                        name=f"{col_name}_max",
                        type=base_type,
                        description=f"Maximum {col_name.replace('_', ' ')}",
                        filter_type=FilterType.RANGE_MAX,
                        filter_model=default_model,
                        filter_column=col_name,
                        range_group=col_name,
                    ),
                ]
            )

        elif col_type == "SmallInteger" and col_name != "year":
            params.filters.append(
                ParameterConfig(
                    name=col_name,
                    type="Optional[int]",
                    description=f"Filter by {col_name.replace('_', ' ')}",
                    filter_type=FilterType.EXACT,
                    filter_model=default_model,
                    filter_column=col_name,
                )
            )

        elif col_type == "String":
            if is_code_column:
                # Code columns: exact match, validation, support multiple values
                validation_func = None
                if is_reference_module:
                    # Reference tables need validation for their own PK
                    validation_func = f"is_valid_{col_name}"

                params.filters.append(
                    ParameterConfig(
                        name=col_name,
                        type="Optional[Union[str, List[str]]]",
                        description=f"Filter by {col_name.replace('_', ' ')} (comma-separated for multiple)",
                        filter_type=FilterType.MULTI,
                        filter_model=default_model,
                        filter_column=col_name,
                        validation_func=validation_func,
                    )
                )
            else:
                # Text columns: partial match only, no validation
                params.filters.append(
                    ParameterConfig(
                        name=col_name,
                        type="Optional[str]",
                        description=f"Filter by {col_name.replace('_', ' ')} (partial match)",
                        filter_type=FilterType.LIKE,
                        filter_model=default_model,
                        filter_column=col_name,
                    )
                )

    # Add option parameters
    params.options = [
        ParameterConfig(
            name="fields",
            type="Optional[List[str]]",
            description="Comma-separated list of fields to return",
            is_option=True,
            query_param=True,
        ),
        ParameterConfig(
            name="sort",
            type="Optional[List[str]]",
            description="Sort fields (e.g., 'year:desc,value:asc')",
            is_option=True,
            query_param=True,
        ),
    ]

    year_params = [p for p in params.filters if p.range_group == "year"]
    if year_params:
        # Mark one of the range params (e.g., the max) with range validation
        for p in year_params:
            if p.filter_type == FilterType.RANGE_MAX:
                break

    return params
