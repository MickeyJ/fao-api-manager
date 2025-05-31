from . import ForeignKeyRule, ColumnRule, ModelRule, PipelineRule


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
