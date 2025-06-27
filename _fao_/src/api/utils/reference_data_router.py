# fao/src/api/utils/reference_data_router.py
from typing import Dict, List, Set, Any
from .base_router import BaseRouterHandler
from _fao_.src.api.utils.query_helpers import QueryBuilder


class ReferenceRouterHandler(BaseRouterHandler):
    """Handler for reference data routers - simpler than dataset routers"""

    def __init__(self, db, model, model_name, table_name, request, response, config):
        self.config = config
        super().__init__(db, model, model_name, table_name, request, response, config)
        self.initialize_query_builder()

    def _get_all_data_fields(self) -> Set[str]:
        """Get all fields from column analysis"""
        return set(self.config.all_data_fields)

    def _get_all_parameter_fields(self) -> Set[str]:
        """Get all parameter fields"""
        return set(self.config.all_parameter_fields)

    def initialize_query_builder(self) -> None:
        """Initialize the QueryBuilder - reference tables don't need joins"""
        super().initialize_query_builder()

    def apply_filters_from_config(self, params: Dict[str, Any]) -> int:
        """Reference tables only need basic filtering"""
        return self.apply_basic_filters(params)
