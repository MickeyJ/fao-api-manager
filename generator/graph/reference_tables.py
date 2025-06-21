# Define which reference tables can influence relationship types
RELATIONSHIP_REFERENCE_TABLES = {
    "elements": {
        "table": "elements",
        "id_column": "element_code_id",
        "code_column": "element_code",
        "name_column": "element",
        "inference_method": "infer_relationship_from_element",
    },
    "indicators": {
        "table": "indicators",
        "id_column": "indicator_code_id",
        "code_column": "indicator_code",
        "name_column": "indicator",
        "inference_method": "infer_relationship_from_indicator",
    },
    "food_values": {
        "table": "food_values",
        "id_column": "food_value_code_id",
        "code_column": "food_value_code",
        "name_column": "food_value",
        "inference_method": "infer_relationship_from_food_value",
    },
    "industries": {
        "table": "industries",
        "id_column": "industry_code_id",
        "code_column": "industry_code",
        "name_column": "industry",
        "inference_method": "infer_relationship_from_industry",
    },
    "factors": {
        "table": "factors",
        "id_column": "factor_code_id",
        "code_column": "factor_code",
        "name_column": "factor",
        "inference_method": "infer_relationship_from_factor",
    },
}
