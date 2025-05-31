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


class GenerationRules:
    def __init__(self):
        self.models = [
            ModelRule(model_name="areas", primary_key="area_code"),
            ModelRule(model_name="items", primary_key="item_code"),
            ModelRule(
                model_name="prices",
                foreign_keys=[
                    ForeignKeyRule("items", "item_code"),
                    ForeignKeyRule("areas", "area_code"),
                ],
            ),
        ]
        # Create lookup dict for easier access
        self._rules_by_model = {rule.model_name: rule for rule in self.models}

    def get_rule(self, model_name: str) -> ModelRule | None:
        return self._rules_by_model.get(model_name)


generation_rules = GenerationRules()
