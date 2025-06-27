# fao/src/api/utils/query_helpers.py (expanded)
from typing import Any, Set, List, Dict, Union, Tuple, Type
from sqlalchemy import Numeric, select, Select, func, or_, and_, Column
from sqlalchemy.orm import Query, DeclarativeBase
from sqlalchemy.sql import ColumnElement
from enum import Enum


class AggregationType(Enum):
    """Supported aggregation types."""

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"

    # Statistical
    STDDEV = "stddev"
    VARIANCE = "variance"
    MEDIAN = "median"  # Not all DBs support

    # String aggregations
    STRING_AGG = "string_agg"  # Concatenate strings

    # Conditional
    COUNT_IF = "count_if"  # Count with condition
    SUM_IF = "sum_if"  # Sum with condition


class QueryBuilder:
    """Helper class to build SQLAlchemy queries with filters and pagination."""

    def __init__(self, Table: Type[DeclarativeBase]):
        self.Table = Table
        self.query = select(self.Table)
        self._aggregations = []
        self._group_by = []
        self._joined_tables: Set[str] = set()  # Track joined tables
        self._joined_columns = []  # Track columns added from joins
        self._column_mapping = []

        # Proper field name to column mapping
        self._field_to_column: Dict[str, ColumnElement] = {}

        # Initialize with main table columns
        for col in Table.__table__.columns:
            self._field_to_column[col.name] = col

    def add_join(
        self, join_model: Type[DeclarativeBase], local_fk_column: Column, column_to_add: str
    ) -> "QueryBuilder":
        join_key = local_fk_column.key

        if join_key not in self._joined_tables:
            self.query = self.query.join(join_model, local_fk_column == join_model.id)

            add_columns = [col.name for col in join_model.__table__.columns]

            # Track everything properly
            current_index = len(self._column_mapping) + 1
            for col_name in add_columns:
                col_obj = getattr(join_model, col_name)
                self.query = self.query.add_columns(col_obj)
                self._column_mapping.append((current_index, col_name))

                # This is the key - maintain the mapping
                self._field_to_column[col_name] = col_obj

                current_index += 1

            self._joined_tables.add(join_key)

        return self

    def is_joined(self, join_key: str) -> bool:
        """Check if a table has already been joined."""
        return join_key in self._joined_tables

    def add_filter(self, column, value: Any, exact: bool = False) -> "QueryBuilder":
        """Add a single filter to the query."""
        if value is not None:
            if isinstance(value, str) and not exact:
                self.query = self.query.where(column.ilike(f"%{value}%"))
            else:
                self.query = self.query.where(column == value)
        return self

    def add_multi_filter(self, column, values: Union[str, List]) -> "QueryBuilder":
        """Add filter for multiple values (e.g., '102,489' or [102, 489])."""
        if values:
            if isinstance(values, str):
                values = [v.strip() for v in values.split(",")]
            # Convert to appropriate type based on column type
            if hasattr(column.type, "python_type"):
                values = [column.type.python_type(v) for v in values]
            self.query = self.query.where(column.in_(values))
        return self

    def add_range_filter(self, column, min_val: Any = None, max_val: Any = None) -> "QueryBuilder":
        """Add range filter for numeric columns."""
        if min_val is not None:
            self.query = self.query.where(column >= min_val)
        if max_val is not None:
            self.query = self.query.where(column <= max_val)
        return self

    def add_aggregation(
        self, column: ColumnElement, agg_type: AggregationType, alias: str | None = None, round_to: Union[str, int] = ""
    ) -> "QueryBuilder":
        """Add aggregation to the query."""

        agg_funcs = {
            AggregationType.SUM: func.sum,
            AggregationType.AVG: func.avg,
            AggregationType.MIN: func.min,
            AggregationType.MAX: func.max,
            AggregationType.COUNT: func.count,
            AggregationType.COUNT_DISTINCT: lambda col: func.count(func.distinct(col)),
            AggregationType.STRING_AGG: lambda col: func.string_agg(col, ", "),
            AggregationType.STDDEV: func.stddev_pop,  # or stddev_samp
            AggregationType.VARIANCE: func.var_pop,  # or var_samp
            # MEDIAN is tricky - not standard SQL
            AggregationType.MEDIAN: lambda col: func.percentile_cont(0.5).within_group(col),  # PostgreSQL only
        }

        agg_func = agg_funcs[agg_type](column)
        round_to_n = int(round_to) if round_to else 2

        # Apply rounding for numeric aggregations
        if agg_type == AggregationType.AVG:
            agg_func = func.round(func.cast(agg_func, Numeric), round_to_n)
        elif agg_type == AggregationType.SUM:
            agg_func = func.round(func.cast(agg_func, Numeric), round_to_n)
        elif agg_type == AggregationType.STDDEV:
            agg_func = func.round(func.cast(agg_func, Numeric), round_to_n)
        elif agg_type == AggregationType.VARIANCE:
            agg_func = func.round(func.cast(agg_func, Numeric), round_to_n)
        elif agg_type == AggregationType.MEDIAN:
            agg_func = func.round(func.cast(agg_func, Numeric), round_to_n)

        if alias:
            agg_func = agg_func.label(alias)

        self._aggregations.append(agg_func)
        return self

    def add_grouping(self, columns: List[Column]) -> "QueryBuilder":
        """Add GROUP BY clause."""
        self._group_by.extend(columns)
        return self

    def apply_aggregations(self) -> "QueryBuilder":
        """Apply aggregations and grouping to the query."""
        if self._aggregations:
            # Replace select with aggregation columns
            select_columns = self._group_by + self._aggregations
            self.query = self.query.with_only_columns(*select_columns)

            if self._group_by:
                self.query = self.query.group_by(*self._group_by)
        return self

    def add_ordering(self, sort_fields: List[Tuple[str, str]]) -> "QueryBuilder":
        """Add ordering to the query.
        Args:
            sort_fields: List of (field_name, direction) tuples
        """
        for field_name, direction in sort_fields:
            if field_name not in self._field_to_column:
                raise ValueError(f"Cannot sort by '{field_name}' - field not available in query")

            column = self._field_to_column[field_name]
            self.query = self.query.order_by(column.desc() if direction == "desc" else column)

        return self

    def get_count(self, db) -> int:
        """Get total count for pagination."""
        # For aggregated queries, we need to count the groups
        if self._group_by:
            count_query = select(func.count()).select_from(self.query.subquery())
        else:
            count_query = select(func.count()).select_from(self.query.subquery())
        return db.execute(count_query).scalar() or 0

    def paginate(self, limit: int, offset: int) -> "QueryBuilder":
        """Add pagination to the query."""
        if limit > 0:
            self.query = self.query.limit(limit).offset(offset)
        return self

    def execute(self, db):
        """Execute the query and return results."""
        rows = db.execute(self.query).all()

        # For aggregated queries, return raw rows
        if self._aggregations:
            return rows

        # For regular queries, parse to HybridResult objects
        return self.parse_results(rows)

    def parse_results(self, rows):
        """Convert Row results to dictionaries with all columns."""
        # If no additional columns were added, return ORM objects
        if not self._column_mapping:
            return [row[0] for row in rows]

        # Otherwise, create hybrid objects
        results = []
        for row in rows:
            # Create a wrapper that combines ORM object and additional columns
            class HybridResult:
                def __init__(self, orm_obj, row_data, column_mapping):
                    self._orm_obj = orm_obj
                    self._row_data = row_data
                    self._column_mapping = column_mapping

                def __getattr__(self, name):
                    # First try the ORM object
                    if hasattr(self._orm_obj, name):
                        return getattr(self._orm_obj, name)

                    # Then check our additional columns
                    for index, col_name in self._column_mapping:
                        if col_name == name and index < len(self._row_data):
                            return self._row_data[index]

                    raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

                def __hasattr__(self, name):
                    if hasattr(self._orm_obj, name):
                        return True
                    return any(col_name == name for _, col_name in self._column_mapping)

            results.append(HybridResult(row[0], row, self._column_mapping))

        return results
