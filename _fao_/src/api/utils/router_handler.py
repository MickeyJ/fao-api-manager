# fao/src/api/utils/dataset_router_handler.py
from typing import Dict, List, Set, Any, Optional
from .base_router import BaseRouterHandler


class RouterHandler(BaseRouterHandler):
    """Handler for dataset routers with foreign key relationships"""

    def __init__(self, db, model, model_name, table_name, request, response, config):
        self.config = config
        super().__init__(db, model, model_name, table_name, request, response, config)
        self.initialize_query_builder()

    def _get_all_data_fields(self) -> Set[str]:
        """Get all fields from column analysis"""
        return set(self.config.all_data_fields)

    def _get_all_parameter_fields(self) -> Set[str]:
        """Get all fields from column analysis"""
        return set(self.config.all_parameter_fields)

    def initialize_query_builder(self) -> None:
        """Initialize the QueryBuilder with the model and optional joined columns"""
        super().initialize_query_builder()

        for filter in self.config.filter_configs:
            if "joins_table" in filter:
                if not self.query_builder.is_joined(filter["joins_table"]):
                    self.query_builder.add_join(filter["join_model"], filter["join_condition"], filter["filter_column"])

    def apply_filters_from_config(self, params: Dict[str, Any]) -> int:
        return self.apply_all_filters(params)

    def apply_all_filters(self, params: Dict[str, Any]) -> int:
        """Apply all filters including those requiring joins"""
        # First apply basic filters on direct columns
        filter_count = self.apply_basic_filters(params)

        # Then apply filters that need joins
        join_filters = [f for f in self.config.filter_configs if f.get("joins_table")]

        for filter_config in join_filters:
            param_value = params.get(filter_config["name"])
            if not param_value:
                continue

            column = getattr(filter_config["filter_model"], filter_config["filter_column"])
            self._apply_single_filter(column, param_value, filter_config["filter_type"])

            filter_count += 1

        return filter_count
