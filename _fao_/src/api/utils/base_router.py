# fao/src/api/utils/base_router.py
import math
from typing import Dict, Any, List, Optional, Tuple, Type, Union
from fastapi import Response, Request
from sqlalchemy.orm import Session
from abc import ABC, abstractmethod

# Import utilities
from _fao_.src.api.utils.query_helpers import QueryBuilder, AggregationType
from _fao_.src.api.utils.response_helpers import PaginationBuilder, ResponseFormatter
from _fao_.src.api.utils.parameter_parsers import (
    parse_sort_parameter,
    parse_fields_parameter,
    parse_aggregation_parameter,
)

from _fao_.src.core.validation import (
    is_valid_sort_direction,
    is_valid_range,
    validate_fields_exist,
)

from _fao_.src.core.exceptions import (
    invalid_parameter,
    invalid_range,
    incompatible_parameters,
)


class BaseRouterHandler(ABC):
    """Base handler for all API endpoints with common functionality"""

    def __init__(
        self, db: Session, model: Type, model_name: str, table_name: str, request: Request, response: Response, config
    ):
        self.db = db
        self.model = model
        self.model_name = model_name
        self.table_name = table_name
        self.all_data_fields = self._get_all_data_fields()
        self.all_parameter_fields = self._get_all_parameter_fields()
        self.request = request
        self.response = response
        self.config = config
        self.query_builder: QueryBuilder
        self.requested_fields: Optional[List[str]] = None

    @abstractmethod
    def _get_all_data_fields(self) -> set:
        """Define which fields are allowed in the API response"""
        pass

    @abstractmethod
    def _get_all_parameter_fields(self) -> set:
        """Define which fields are allowed in the API response"""
        pass

    @abstractmethod
    def initialize_query_builder(self) -> None:
        """Initialize the QueryBuilder with the model and optional joined columns"""
        self.query_builder = QueryBuilder(self.model)
        pass

    @abstractmethod
    def apply_filters_from_config(self, params: Dict[str, Any]) -> int:
        """Apply filters based on the configuration"""
        pass

    def clean_param(self, param: Any, filter_type: str) -> Any:
        """Clean a single parameter based on its type"""
        if isinstance(param, str):
            if not param.strip():
                return None
            if filter_type == "multi":
                return [v.strip() for v in param.split(",") if v.strip()]
            return param.strip()
        elif isinstance(param, list):
            cleaned_list = [v for v in param if v and v.strip()]
            return cleaned_list if cleaned_list else None
        return param

    def validate_fields_and_sort_parameters(
        self, fields: Optional[List[str]], sort: Optional[List[str]]
    ) -> Tuple[Optional[List[str]], List[Tuple[str, str]]]:
        """Validate fields and sort parameters together.

        Sort fields must be included in the response fields.
        """
        # First validate fields
        self.requested_fields = self.validate_fields_parameter(fields)

        # Then validate sort
        if not sort:
            return self.requested_fields, []

        self.sort_columns = self.validate_sort_parameter(sort)

        return self.requested_fields, self.sort_columns

    def validate_sort_parameter(self, sort: Optional[List[str]]) -> List[Tuple]:
        """Validate and parse sort parameter"""
        if not sort:
            return []

        sort_columns = []
        parsed_sort = parse_sort_parameter(sort)
        response_fields = self.requested_fields if self.requested_fields else self.all_data_fields

        for column_name, direction in parsed_sort:
            if not is_valid_sort_direction(direction):
                raise invalid_parameter(
                    params="sort",
                    value=f"{column_name}:{direction}",
                    reason=f"Invalid sort direction: {direction}. Use 'asc' or 'desc'",
                )

            # Check if sort field is in the response
            if column_name not in response_fields:
                if self.requested_fields:
                    # User specified fields but sort isn't in them
                    raise incompatible_parameters(
                        params=["fields", "sort"],
                        values=[self.requested_fields, column_name],
                        reason=f"Cannot sort by '{column_name}' - field not included in response. Add it to fields parameter or remove from sort.",
                    )
                else:
                    # Field doesn't exist at all
                    raise invalid_parameter(
                        params="sort", value=column_name, reason=f"Sort field not found in data: {column_name}."
                    )

            # For now, just store the field name - we'll resolve the actual column later
            sort_columns.append((column_name, direction))

        return sort_columns

    def validate_fields_parameter(self, fields: Optional[List[str]]) -> Optional[List[str]]:
        """Validate and parse fields parameter"""
        if not fields:
            return None

        fields = parse_fields_parameter(fields)

        invalid_fields = validate_fields_exist(fields, self.all_data_fields)

        if invalid_fields:
            raise invalid_parameter(
                params="fields", value=invalid_fields, reason=f"Invalid fields requested: {', '.join(invalid_fields)}"
            )

        return fields

    def validate_range(self, min_val: Any, max_val: Any, param_name: str) -> None:
        """Validate a range parameter"""
        if min_val is not None and max_val is not None:
            if not is_valid_range(min_val, max_val):
                raise invalid_range(
                    params=[f"{param_name}_min", f"{param_name}_max"],
                    values=[min_val, max_val],
                )

    def validate_filter_parameters(self, params: Dict[str, Any], db: Session) -> None:
        """Validate all filter parameters based on configuration"""

        # First, validate ranges
        validated_ranges = set()
        for range_config in self.config.range_configs:
            param_name = range_config["param_name"]
            if param_name in validated_ranges:
                continue

            min_val = params.get(f"{param_name}_min")
            max_val = params.get(f"{param_name}_max")

            if min_val and max_val and not is_valid_range(min_val, max_val):
                raise invalid_range(params=[f"{param_name}_min", f"{param_name}_max"], values=[min_val, max_val])
            validated_ranges.add(param_name)

        # Then validate individual parameters
        for filter_config in self.config.filter_configs:
            if not filter_config.get("validation_func"):
                continue

            param_name = filter_config["name"]
            param_value = params.get(param_name)

            if not param_value:
                continue

            validation_func = filter_config["validation_func"]
            exception_func = filter_config["exception_func"]

            if filter_config["filter_type"] == "multi":
                # Only validate single values, not comma-separated lists
                if isinstance(param_value, str) and "," not in param_value:
                    if not validation_func(param_value, db):
                        exception_func(param_value)
                elif isinstance(param_value, list) and len(param_value) == 1:
                    if not validation_func(param_value[0], db):
                        exception_func(param_value[0])
            else:
                # Regular validation
                if not validation_func(param_value, db):
                    exception_func(param_value)

    # In base_router.py
    def apply_basic_filters(self, params: Dict[str, Any]) -> int:
        """Apply filters for columns that exist directly on the model"""
        filter_count = 0

        # Get only filters that don't require joins
        basic_filters = [f for f in self.config.filter_configs if not f.get("joins_table")]

        for filter_config in basic_filters:
            param_value = params.get(filter_config["name"])
            if not param_value:
                continue

            # Get the column from the main model
            column = getattr(self.model, filter_config["filter_column"])
            self._apply_single_filter(column, param_value, filter_config["filter_type"])

            filter_count += 1

        # Handle range filters
        for range_config in self.config.range_configs:
            min_param = f"{range_config['param_name']}_min"
            max_param = f"{range_config['param_name']}_max"
            min_val = params.get(min_param)
            max_val = params.get(max_param)

            if min_val is not None or max_val is not None:
                column = getattr(self.model, range_config["filter_column"])
                self.query_builder.add_range_filter(column, min_val, max_val)
                filter_count += 1

        return filter_count

    def _apply_single_filter(self, column, param_value, filter_type: str):
        """Apply a single filter based on its type"""
        if filter_type == "multi":
            # Handle both single string and list of strings
            if isinstance(param_value, str):
                values = [v.strip() for v in param_value.split(",") if v.strip()]
            else:
                values = param_value

            if len(values) == 1:
                # Single value - use exact match
                self.query_builder.add_filter(column, values[0], exact=True)
            else:
                # Multiple values - use IN clause
                self.query_builder.add_multi_filter(column, values)

        elif filter_type == "like":
            self.query_builder.add_filter(column, param_value)

        elif filter_type == "exact":
            self.query_builder.add_filter(column, param_value, exact=True)

    def get_default_sort(self) -> List[Tuple[str, str]]:
        """Get default sort order"""
        if hasattr(self.model, "year"):
            return [("year", "desc")]
        else:
            return [("id", "asc")]

    def _sanitize_float_value(self, value: Any) -> Any:
        """Sanitize float values to ensure JSON compliance"""
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return None
        return value

    def filter_response_data(self, results: List, requested_fields: Optional[List[str]] = None) -> List[Dict]:
        """Format query results based on requested fields"""
        data = []
        for result in results:
            response_fields = {}

            for field in self.all_data_fields:
                if not requested_fields or field in requested_fields:
                    if hasattr(result, field):
                        value = getattr(result, field)
                        # Sanitize float values to prevent JSON serialization errors
                        response_fields[field] = self._sanitize_float_value(value)

            sorted_fields = dict(sorted(response_fields.items()))
            data.append(sorted_fields)

        return data

    def setup_aggregation(self, group_by: Union[str, List[str]], aggregations: Union[str, List[str]]):
        """Setup handler for aggregation mode with validation"""
        self.is_aggregation = True

        # Parse group_by
        if isinstance(group_by, list):
            self.group_fields = group_by
        else:
            self.group_fields = [f.strip() for f in group_by.split(",")]

        # Parse aggregations
        if isinstance(aggregations, list):
            agg_strings = aggregations
        else:
            agg_strings = [a.strip() for a in aggregations.split(",")]

        self.agg_configs = []
        for agg_str in agg_strings:
            try:
                agg_config = parse_aggregation_parameter(agg_str)

                # Validate aggregation function
                try:
                    AggregationType(agg_config["function"])
                except ValueError:
                    raise invalid_parameter(
                        "aggregations",
                        agg_str,
                        f"Invalid aggregation function '{agg_config['function']}'. Valid functions: {', '.join([e.value for e in AggregationType])}",
                    )

                # Validate field exists
                if agg_config["field"] not in self.all_data_fields:
                    raise invalid_parameter(
                        "aggregations", agg_str, f"Cannot aggregate '{agg_config['field']}' - field does not exist"
                    )

                # Check if field is numeric (for most aggregations)
                if self._is_numeric_function(agg_config["function"]) and not self._is_numeric_field(
                    agg_config["field"]
                ):
                    raise invalid_parameter(
                        "aggregations",
                        agg_str,
                        f"Cannot {agg_config['function']} non-numeric field '{agg_config['field']}'",
                    )

                # Check if field is nullable numeric (for some aggregations) TODO: filter out null values in query
                if self._is_numeric_function(agg_config["function"]) and self._is_nullable_numeric_field(
                    agg_config["field"]
                ):
                    raise invalid_parameter(
                        "aggregations",
                        agg_str,
                        f"Cannot aggregate '{agg_config['function']}' on '{agg_config['field']}'. '{agg_config['field']}' can be null. Will be fixed in a future update",
                    )

                # Check if aggregating a grouped field
                if agg_config["field"] in self.group_fields:
                    raise invalid_parameter(
                        "aggregations",
                        agg_str,
                        f"Cannot aggregate '{agg_config['field']}' - it's already in group_by. Remove from group_by or choose a different field",
                    )

                self.agg_configs.append(agg_config)

            except (ValueError, KeyError, IndexError):
                # These are actual parsing errors
                raise invalid_parameter(
                    "aggregations",
                    agg_str,
                    "Invalid aggregation format. Expected 'field:function' or 'field:function:alias'",
                )
        # Validate group fields exist
        for field in self.group_fields:
            if field not in self.all_data_fields:
                raise invalid_parameter("group_by", field, f"Invalid group by field: {field}")

    def _is_numeric_field(self, field: str) -> bool:
        """Check if a field is numeric based on config"""
        if hasattr(self.config, "field_metadata"):
            field_info = self.config.field_metadata.get(field, {})
            return field_info.get("is_numeric", False)

        # Fallback for reference tables without metadata
        return field in {"value", "year", "quantity", "price"} or field.endswith("_id")

    def _is_nullable_numeric_field(self, field: str) -> bool:
        """Check if a field is numeric based on config"""
        if hasattr(self.config, "field_metadata"):
            field_info = self.config.field_metadata.get(field, {})
            return field_info.get("is_numeric", False) and field_info.get("nullable", False)

        return False

    def _is_numeric_function(self, function: str):
        return function in [
            AggregationType.SUM.value,
            AggregationType.AVG.value,
            AggregationType.MIN.value,
            AggregationType.MAX.value,
            AggregationType.STDDEV.value,
            AggregationType.VARIANCE.value,
            AggregationType.MEDIAN.value,
        ]

    def get_aggregation_response_fields(self) -> List[str]:
        """Get available fields for aggregation response"""
        fields = self.group_fields.copy()
        fields.extend([agg["alias"] for agg in self.agg_configs])
        return fields

    def format_aggregation_results(self, results: List) -> List[Dict]:
        """Format aggregation query results"""

        print(f"Formatting with group_fields: {self.group_fields}")
        print(f"Formatting with agg_configs: {self.agg_configs}")
        print(f"First result row: {results[0] if results else 'No results'}")

        data = []
        for row in results:
            response_fields = {}

            # Add group by fields
            for i, field in enumerate(self.group_fields):
                response_fields[field] = row[i]

            # Add aggregation results
            for j, agg_config in enumerate(self.agg_configs):
                response_fields[agg_config["alias"]] = row[len(self.group_fields) + j]

            sorted_fields = dict(sorted(response_fields.items()))
            data.append(sorted_fields)

        return data

    def build_response(
        self,
        request,
        response,
        data: List[Dict],
        total_count: int,
        limit: int,
        offset: int,
        filter_count: int,
        **params,
    ) -> Dict:
        """Build standardized API response"""
        pagination = PaginationBuilder.build_pagination_meta(total_count, limit, offset)

        # Collect parameters for links
        all_params = {k: v for k, v in params.items() if v is not None}
        all_params.update({"limit": limit, "offset": offset})

        links = PaginationBuilder.build_links(str(request.url), total_count, limit, offset, all_params)

        ResponseFormatter.set_pagination_headers(response, total_count, limit, offset, links)

        return ResponseFormatter.format_data_response(data, pagination, links, filter_count)
