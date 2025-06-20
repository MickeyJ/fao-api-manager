from typing import List, Dict, Optional
from sqlalchemy import text

from _fao_.src.db.database import get_db
from generator.logger import logger

from .relationship_type_tables import RELATIONSHIP_REFERENCE_TABLES


class RelationshipIdentifier:
    """
    This class is used to determine the type of relationship based on the provided parameters.
    """

    def get_table_elements(self, table_name: str) -> List[Dict]:
        """
        Query the database to get elements for a specific dataset
        """

        # Get database session using the generator pattern
        db_gen = get_db()
        db = next(db_gen)

        try:
            # Get all elements for this dataset
            query = text(
                """
                SELECT DISTINCT 
                    e.id as element_code_id,
                    e.element_code,
                    e.element,
                    e.source_dataset
                FROM elements e
                WHERE e.source_dataset = :table_name
                ORDER BY e.element_code
            """
            )

            result = db.execute(query, {"table_name": table_name})

            # Convert to list of dicts
            elements = [dict(row) for row in result.mappings().all()]

            logger.info(f"  Found {len(elements)} elements for {table_name}")

        except Exception as e:
            logger.error(f"Failed to query elements for {table_name}: {e}")
            elements = []
        finally:
            # Properly close the generator
            try:
                next(db_gen)
            except StopIteration:
                pass

        return elements

    def get_table_indicators(self, table_name: str) -> List[Dict]:
        """
        Query the database to get indicators for a specific dataset
        """

        # Get database session using the generator pattern
        db_gen = get_db()
        db = next(db_gen)

        try:
            # Get all indicators for this dataset
            query = text(
                """
                SELECT DISTINCT 
                    i.id as indicator_code_id,
                    i.indicator_code,
                    i.indicator,
                    i.source_dataset
                FROM indicators i
                WHERE i.source_dataset = :table_name
                ORDER BY i.indicator_code
            """
            )

            result = db.execute(query, {"table_name": table_name})

            # Convert to list of dicts
            indicators = [dict(row) for row in result.mappings().all()]

            logger.info(f"  Found {len(indicators)} indicators for {table_name}")

        except Exception as e:
            logger.error(f"Failed to query indicators for {table_name}: {e}")
            indicators = []
        finally:
            # Properly close the generator
            try:
                next(db_gen)
            except StopIteration:
                pass

        return indicators

    def get_reference_data_for_table(self, table_name: str, reference_type: str) -> List[Dict]:
        """
        Generic method to query any reference table for a dataset
        """
        ref_config = RELATIONSHIP_REFERENCE_TABLES.get(reference_type)
        if not ref_config:
            logger.error(f"Unknown reference type: {reference_type}")
            return []

        db_gen = get_db()
        db = next(db_gen)

        try:
            query = text(
                f"""
                SELECT DISTINCT 
                    r.id as {ref_config['id_column']},
                    r.{ref_config['code_column']},
                    r.{ref_config['name_column']},
                    r.source_dataset
                FROM {ref_config['table']} r
                WHERE r.source_dataset = '{table_name}'
                ORDER BY r.{ref_config['code_column']}
            """
            )

            result = db.execute(query)
            items = [dict(row) for row in result.mappings().all()]

            logger.info(f"  Found {len(items)} {reference_type} for {table_name}")
            return items

        except Exception as e:
            logger.error(f"Failed to query {reference_type} for {table_name}: {e}")
            return []
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def determine_reference_types_for_table(self, table_name: str, foreign_keys: List[Dict]) -> List[str]:
        """
        Determine which reference types a table uses based on foreign keys
        """
        reference_types = []
        fk_tables = [fk["table_name"] for fk in foreign_keys]

        # Check each possible reference type
        for ref_type, config in RELATIONSHIP_REFERENCE_TABLES.items():
            if config["table"] in fk_tables:
                reference_types.append(ref_type)

        return reference_types

    def group_by_relationship_type(
        self, table_name: str, reference_items: List[Dict], pattern_info: Dict, reference_type: str
    ) -> Dict[str, Dict]:
        """Generic method to group any reference type by relationship"""
        relationship_groups = {}
        ref_config = RELATIONSHIP_REFERENCE_TABLES[reference_type]

        # Get the inference method dynamically
        inference_method = getattr(self, ref_config["inference_method"])

        # Create key names based on reference type
        codes_key = f"{reference_type[:-1]}_codes"  # e.g., "element_codes", "indicator_codes"
        items_key = reference_type  # e.g., "elements", "indicators"

        for item in reference_items:
            rel_info = inference_method(item[ref_config["name_column"]], item[ref_config["code_column"]], table_name)

            if rel_info:
                rel_type = rel_info["type"]

                # Initialize the relationship group if it doesn't exist
                if rel_type not in relationship_groups:
                    relationship_groups[rel_type] = {
                        "type": rel_type,
                        "source_fk": pattern_info["source_fk"],
                        "target_fk": pattern_info["target_fk"],
                        "source_node": pattern_info["source_node"],
                        "target_node": pattern_info["target_node"],
                        codes_key: [],  # Dynamic key name
                        items_key: [],  # Dynamic key name
                        "properties": {},
                    }

                relationship_groups[rel_type][codes_key].append(item[ref_config["code_column"]])
                relationship_groups[rel_type][items_key].append(item)
                relationship_groups[rel_type]["properties"] = rel_info.get("properties", {})

        return relationship_groups

    def group_elements_by_relationship(
        self, table_name: str, elements: List[Dict], pattern_info: Dict
    ) -> Dict[str, Dict]:
        """
        Group elements by their inferred relationship type
        """
        relationship_groups = {}

        for element in elements:
            rel_info = self.infer_relationship_from_element(element["element"], element["element_code"], table_name)

            if rel_info:
                rel_type = rel_info["type"]

                # Initialize the relationship group if it doesn't exist
                if rel_type not in relationship_groups:
                    relationship_groups[rel_type] = {
                        "type": rel_type,
                        "source_fk": pattern_info["source_fk"],
                        "target_fk": pattern_info["target_fk"],
                        "source_node": pattern_info["source_node"],
                        "target_node": pattern_info["target_node"],
                        "element_codes": [],
                        "elements": [],
                        "properties": {},
                    }

                relationship_groups[rel_type]["element_codes"].append(element["element_code"])
                relationship_groups[rel_type]["elements"].append(element)
                relationship_groups[rel_type]["properties"] = rel_info.get("properties", {})

        return relationship_groups

    def group_indicators_by_relationship(
        self, table_name: str, indicators: List[Dict], pattern_info: Dict
    ) -> Dict[str, Dict]:
        """
        Group indicators by their inferred relationship type
        """
        relationship_groups = {}

        for indicator in indicators:
            rel_info = self.infer_relationship_from_indicator(
                indicator["indicator"], indicator["indicator_code"], table_name
            )

            if rel_info:
                rel_type = rel_info["type"]

                # Initialize the relationship group if it doesn't exist
                if rel_type not in relationship_groups:
                    relationship_groups[rel_type] = {
                        "type": rel_type,
                        "source_fk": pattern_info["source_fk"],
                        "target_fk": pattern_info["target_fk"],
                        "source_node": pattern_info["source_node"],
                        "target_node": pattern_info["target_node"],
                        "indicator_codes": [],
                        "indicators": [],
                        "properties": {},
                    }

                relationship_groups[rel_type]["indicator_codes"].append(indicator["indicator_code"])
                relationship_groups[rel_type]["indicators"].append(indicator)
                relationship_groups[rel_type]["properties"] = rel_info.get("properties", {})

        return relationship_groups

    def determine_relationship_pattern(self, table_name: str, foreign_keys: List[Dict]) -> Optional[Dict]:
        """Determine relationship patterns for a dataset"""
        fk_tables = [fk["table_name"] for fk in foreign_keys]

        # Single foreign key patterns (only area_codes)
        if len(fk_tables) == 1 and "area_codes" in fk_tables:
            # Special handling for specific tables
            if "temperature" in table_name:
                return {
                    "pattern": "country_climate",
                    "relationships": [
                        {
                            "type": "EXPERIENCES",
                            "source_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                            ),
                            "source_node": "AreaCode",
                            "target_node": "Climate",  # Virtual node
                            "virtual_target": True,
                        }
                    ],
                }
            elif "aquastat" in table_name:
                return {
                    "pattern": "country_water",
                    "relationships": [
                        {
                            "type": "MANAGES",
                            "source_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                            ),
                            "source_node": "AreaCode",
                            "target_node": "WaterResources",  # Virtual node
                            "virtual_target": True,
                        }
                    ],
                }
            else:
                return {
                    "pattern": "country_only",
                    "relationships": "dynamic",  # Will be determined by element analysis
                    "source_fk": next(
                        fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                    ),
                    "source_node": "AreaCode",
                }

        # Country-Currency pattern (exchange rates)
        elif "area_codes" in fk_tables and "currencies" in fk_tables:
            return {
                "pattern": "country_currency",
                "relationships": [
                    {
                        "type": "EXCHANGES",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "currencies"
                        ),
                        "source_node": "AreaCode",
                        "target_node": "Currency",
                    }
                ],
            }

        # Country-Indicator pattern (employment, trade indicators)
        elif "area_codes" in fk_tables and "indicators" in fk_tables:
            # Check if it also has item_codes (3-way relationship)
            if "item_codes" in fk_tables:
                return {
                    "pattern": "country_item_indicator",
                    "relationships": "dynamic",  # Complex pattern, needs element analysis
                    "source_fk": next(
                        fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                    ),
                    "target_fk": next(
                        fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "item_codes"
                    ),
                    "indicator_fk": next(
                        fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "indicators"
                    ),
                    "source_node": "AreaCode",
                    "target_node": "ItemCode",
                    "indicator_node": "Indicator",
                }
            else:
                return {
                    "pattern": "country_indicator",
                    "relationships": [
                        {
                            "type": "MEASURES",
                            "source_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                            ),
                            "target_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "indicators"
                            ),
                            "source_node": "AreaCode",
                            "target_node": "Indicator",
                        }
                    ],
                }

        # Survey-based patterns
        elif "surveys" in fk_tables:
            # Survey-Indicator pattern
            if "indicators" in fk_tables and "geographic_levels" not in fk_tables:
                return {
                    "pattern": "survey_indicator",
                    "relationships": [
                        {
                            "type": "MEASURES",
                            "source_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "surveys"
                            ),
                            "target_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "indicators"
                            ),
                            "source_node": "Survey",
                            "target_node": "Indicator",
                        }
                    ],
                }
            # Survey-Food Group pattern (dietary surveys)
            elif "food_groups" in fk_tables:
                return {
                    "pattern": "survey_food_group",
                    "relationships": [
                        {
                            "type": "MEASURES",
                            "source_fk": next(
                                fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "surveys"
                            ),
                            "target_fk": next(
                                fk["hash_fk_sql_column_name"]
                                for fk in foreign_keys
                                if fk["table_name"] == "food_groups"
                            ),
                            "source_node": "Survey",
                            "target_node": "FoodGroup",
                        }
                    ],
                }

        # Recipient-Item pattern (food aid)
        elif "recipient_country_codes" in fk_tables and "item_codes" in fk_tables:
            return {
                "pattern": "recipient_item",
                "relationships": [
                    {
                        "type": "RECEIVES",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"]
                            for fk in foreign_keys
                            if fk["table_name"] == "recipient_country_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "item_codes"
                        ),
                        "source_node": "RecipientCountryCode",
                        "target_node": "ItemCode",
                    }
                ],
            }

        # Bilateral trade pattern
        elif "reporter_country_codes" in fk_tables and "partner_country_codes" in fk_tables:
            return {
                "pattern": "bilateral",
                "relationships": [
                    {
                        "type": "TRADES",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"]
                            for fk in foreign_keys
                            if fk["table_name"] == "reporter_country_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"]
                            for fk in foreign_keys
                            if fk["table_name"] == "partner_country_codes"
                        ),
                        "source_node": "ReporterCountryCode",
                        "target_node": "PartnerCountryCode",
                    }
                ],
            }

        # Country-Item pattern (most common)
        elif "area_codes" in fk_tables and "item_codes" in fk_tables:
            return {
                "pattern": "country_item",
                "relationships": "dynamic",  # Will be determined by element analysis
                "source_fk": next(
                    fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                ),
                "target_fk": next(
                    fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "item_codes"
                ),
                "source_node": "AreaCode",
                "target_node": "ItemCode",
            }

        # Country-Purpose pattern (for investment data)
        elif "area_codes" in fk_tables and "purposes" in fk_tables:
            return {
                "pattern": "country_purpose",
                "relationships": [
                    {
                        "type": "MEASURES",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "purposes"
                        ),
                        "source_node": "AreaCode",
                        "target_node": "Purpose",
                    }
                ],
            }

        # Country-Donor pattern
        elif "area_codes" in fk_tables and "donors" in fk_tables:
            return {
                "pattern": "country_donor",
                "relationships": [
                    {
                        "type": "RECEIVES_FROM",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "area_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "donors"
                        ),
                        "source_node": "AreaCode",
                        "target_node": "Donor",
                    }
                ],
            }

        # Recipient-Donor pattern
        elif "recipient_country_codes" in fk_tables and "donors" in fk_tables:
            return {
                "pattern": "recipient_donor",
                "relationships": [
                    {
                        "type": "RECEIVES_FROM",
                        "source_fk": next(
                            fk["hash_fk_sql_column_name"]
                            for fk in foreign_keys
                            if fk["table_name"] == "recipient_country_codes"
                        ),
                        "target_fk": next(
                            fk["hash_fk_sql_column_name"] for fk in foreign_keys if fk["table_name"] == "donors"
                        ),
                        "source_node": "RecipientCountryCode",
                        "target_node": "Donor",
                    }
                ],
            }

        return None

    def infer_relationship_from_food_value(self, food_value_name: str, food_value_code: str, table_name: str) -> Dict:
        """Food values are end points in the value chain"""
        table_lower = table_name.lower()

        # Table context determines relationship type
        if "value_shares" in table_lower:
            return {
                "type": "MEASURES",  # All value shares are measurements
                "properties": {
                    "category": "value_share",
                    "value_type": "food_value",
                    "food_value_code": food_value_code,
                    "food_value": food_value_name,
                },
            }

        return {
            "type": "PRODUCES",  # Industries produce value for these categories
            "properties": {
                "value_category": "home" if "At Home" in food_value_name else "away_from_home",
                "food_value_code": food_value_code,
                "food_value": food_value_name,
            },
        }

    def infer_relationship_from_industry(self, industry_name: str, industry_code: str, table_name: str) -> Dict:
        """Industries use factors to create value"""
        return {
            "type": "USES",  # Industries use factors
            "properties": {
                "industry_type": self._categorize_industry(industry_name),
                "industry_code": industry_code,
                "industry": industry_name,
            },
        }

    def infer_relationship_from_factor(self, factor_name: str, factor_code: str, table_name: str) -> Dict:
        """Factors contribute to production"""
        factor_lower = factor_name.lower()

        if "labour" in factor_lower:
            rel_type = "EMPLOYS"  # Special case for labor
        else:
            rel_type = "USES"  # General case for other factors

        return {
            "type": rel_type,
            "properties": {
                "factor_type": self._categorize_factor(factor_name),
                "factor_code": factor_code,
                "factor": factor_name,
            },
        }

    def infer_relationship_from_indicator(self, indicator_name: str, indicator_code: str, table_name: str) -> Dict:
        """
        Infer relationship type from indicator (instead of element)
        """
        indicator_lower = indicator_name.lower()
        table_lower = table_name.lower()

        # Trade indicators
        if "trade" in table_lower:
            if "dependency" in indicator_lower:
                return {
                    "type": "DEPENDS_ON",
                    "properties": {
                        "measure": "import_dependency",
                        "indicator_code": indicator_code,
                        "indicator": indicator_name,
                    },
                }
            elif "self-sufficiency" in indicator_lower:
                return {
                    "type": "PRODUCES",
                    "properties": {
                        "measure": "self_sufficiency",
                        "indicator_code": indicator_code,
                        "indicator": indicator_name,
                    },
                }
            elif "comparative advantage" in indicator_lower:
                return {
                    "type": "COMPETES",
                    "properties": {
                        "measure": "comparative_advantage",
                        "indicator_code": indicator_code,
                        "indicator": indicator_name,
                    },
                }
            else:
                return {
                    "type": "MEASURES",
                    "properties": {
                        "category": "trade_indicator",
                        "indicator_code": indicator_code,
                        "indicator": indicator_name,
                    },
                }

        # Default for other indicator-based tables
        return {
            "type": "MEASURES",
            "properties": {
                "category": "indicator",
                "indicator_code": indicator_code,
                "indicator": indicator_name,
            },
        }

    def infer_relationship_from_element(self, element_name: str, element_code: str, table_name: str) -> Dict:
        """
        Infer relationship type using table context first, then element details
        """
        element_lower = element_name.lower()
        table_lower = table_name.lower()

        # TRADE context tables
        if "trade" in table_lower:
            # Check for nutrient-specific trade
            nutrient = self._categorize_nutrient_type(element_lower)
            if nutrient and nutrient != "other":
                return {
                    "type": "TRADES",
                    "properties": {
                        "flow": "import" if "import" in element_lower else "export",
                        "content_type": "nutrient",
                        "nutrient": nutrient,
                        "measure": self._categorize_measure(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                # Standard trade
                if "import" in element_lower or "export" in element_lower:
                    flow = "import" if "import" in element_lower else "export"
                else:
                    flow = "unspecified"

                return {
                    "type": "TRADES",
                    "properties": {
                        "flow": flow,
                        "measure": self._categorize_measure(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # PRODUCTION context tables
        elif "production" in table_lower:
            return {
                "type": "PRODUCES",
                "properties": {
                    "measure": self._categorize_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # EMISSIONS context tables (enhanced)
        elif "emissions" in table_lower:
            source = self._categorize_emission_source(element_lower, table_lower)
            return {
                "type": "EMITS",
                "properties": {
                    "source": source,
                    "gas_type": self._categorize_gas_type(element_lower),
                    "category": self._categorize_emission_category(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # WATER/AQUASTAT context tables (enhanced)
        elif "aquastat" in table_lower:
            if "withdrawal" in element_lower:
                return {
                    "type": "WITHDRAWS",  # Changed from USES
                    "properties": {
                        "resource": "water",
                        "purpose": self._categorize_water_purpose(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            elif "area equipped" in element_lower:
                return {
                    "type": "EQUIPS",  # New relationship type
                    "properties": {
                        "infrastructure": "irrigation",
                        "irrigation_type": self._categorize_irrigation_type(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            elif "population" in element_lower and "access" in element_lower:
                return {
                    "type": "HAS_ACCESS",  # New relationship type
                    "properties": {
                        "resource": "safe_water",
                        "population_type": "rural" if "rural" in element_lower else "urban",
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                return {
                    "type": "MEASURES",
                    "properties": {
                        "category": "water_infrastructure",
                        "subcategory": self._categorize_water_subcategory(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # INPUTS context tables
        elif "inputs" in table_lower or "fertilizer" in table_lower:
            if "import" in element_lower or "export" in element_lower:
                return {
                    "type": "TRADES",
                    "properties": {
                        "flow": "import" if "import" in element_lower else "export",
                        "commodity_type": "agricultural_inputs",
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                return {
                    "type": "USES",
                    "properties": {
                        "resource": "fertilizer" if "fertilizer" in table_lower else "inputs",
                        "measure": self._categorize_measure(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # PRICE context tables
        elif "price" in table_lower:
            return {
                "type": "MEASURES",
                "properties": {
                    "category": "price",
                    "price_type": "producer" if "producer" in element_lower else "consumer",
                    "currency": "USD" if "us$" in element_lower or "usd" in element_lower else "local",
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # INVESTMENT context tables (enhanced)
        elif "investment" in table_lower or "capital" in table_lower:
            # Check if it's actual investment flow vs measurement
            if "share" in element_lower or "orientation" in element_lower or "gross fixed capital" in element_lower:
                return {
                    "type": "INVESTS",  # New relationship type
                    "properties": {
                        "measure": self._categorize_investment_measure(element_lower),
                        "currency": "USD" if "us$" in element_lower else "local",
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                return {
                    "type": "MEASURES",
                    "properties": {
                        "category": "financial",
                        "flow_type": self._categorize_investment_type(table_lower, element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # FOOD BALANCE/SUPPLY tables (enhanced)
        elif "food_balance" in table_lower or "sua" in table_lower or "supply" in table_lower:
            if "import" in element_lower or "export" in element_lower:
                return {
                    "type": "TRADES",
                    "properties": {
                        "flow": "import" if "import" in element_lower else "export",
                        "measure": "quantity",
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            elif "production" in element_lower:
                return {
                    "type": "PRODUCES",
                    "properties": {"measure": "quantity", "element_code": element_code, "element": element_name},
                }
            elif "supply" in element_lower and any(
                nutrient in element_lower for nutrient in ["fat", "protein", "vitamin"]
            ):
                return {
                    "type": "SUPPLIES",  # New relationship type
                    "properties": {
                        "nutrient": self._categorize_nutrient_type(element_lower),
                        "measure": "quantity",
                        "unit": self._categorize_unit(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            elif "feed" in element_lower or "food" in element_lower:
                return {
                    "type": "USES",
                    "properties": {
                        "resource": "food_supply",
                        "purpose": "feed" if "feed" in element_lower else "food",
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                return {
                    "type": "MEASURES",
                    "properties": {
                        "category": "food_balance",
                        "measure": self._categorize_measure(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # EMPLOYMENT/RESEARCH context tables (new)
        elif "employment" in table_lower or "researchers" in table_lower or "asti" in table_lower:
            return {
                "type": "EMPLOYS",
                "properties": {
                    "role": "researcher" if "researcher" in element_lower else "worker",
                    "measure": self._categorize_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # HOUSEHOLD/DIETARY SURVEYS (new)
        elif "household" in table_lower or "dietary" in table_lower:
            return {
                "type": "CONSUMES",
                "properties": {
                    "category": "dietary",
                    "measure": self._categorize_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # Default fallback - MEASURES
        else:
            return {
                "type": "MEASURES",
                "properties": {
                    "category": "general",
                    "measure": self._categorize_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

    def _categorize_measure(self, element_lower: str) -> str:
        """Extract what's being measured from element name"""
        if "value" in element_lower:
            return "value"
        elif "quantity" in element_lower:
            return "quantity"
        elif "area" in element_lower:
            return "area"
        elif "yield" in element_lower:
            return "yield"
        elif "weight" in element_lower:
            return "weight"
        elif "content" in element_lower:
            return "content"
        elif "density" in element_lower:
            return "density"
        elif "intensity" in element_lower:
            return "intensity"
        elif any(nutrient in element_lower for nutrient in ["protein", "fat", "vitamin", "calcium"]):
            return "nutrients"
        elif "per capita" in element_lower:
            return "per_capita"
        elif "share" in element_lower or "percentage" in element_lower or "%" in element_lower:
            return "percentage"
        else:
            return "other"

    def _categorize_gas_type(self, element_lower: str) -> str:
        """Extract greenhouse gas type from element name"""
        if "co2" in element_lower:
            return "CO2"
        elif "n2o" in element_lower:
            return "N2O"
        elif "ch4" in element_lower:
            return "CH4"
        else:
            return "unspecified"

    def _categorize_water_purpose(self, element_lower: str) -> str:
        """Extract water use purpose from element name"""
        if "agriculture" in element_lower or "irrigation" in element_lower:
            return "agriculture"
        elif "industrial" in element_lower:
            return "industrial"
        elif "municipal" in element_lower:
            return "municipal"
        elif "cooling" in element_lower:
            return "thermoelectric_cooling"
        else:
            return "general"

    def _categorize_water_subcategory(self, element_lower: str) -> str:
        """Extract water infrastructure subcategory"""
        if "equipped" in element_lower:
            return "equipped_area"
        elif "irrigated" in element_lower:
            return "irrigated_area"
        elif "groundwater" in element_lower:
            return "groundwater"
        elif "surface" in element_lower:
            return "surface_water"
        elif "wastewater" in element_lower:
            return "wastewater"
        elif "drainage" in element_lower:
            return "drainage"
        else:
            return "general"

    def _categorize_investment_type(self, table_lower: str, element_lower: str) -> str:
        """Extract investment/financial flow type"""
        if "credit" in table_lower:
            return "credit"
        elif "foreign" in table_lower:
            return "foreign_direct_investment"
        elif "government" in table_lower:
            return "government_expenditure"
        elif "assistance" in table_lower:
            return "development_assistance"
        elif "capital stock" in table_lower:
            return "capital_stock"
        else:
            return "general_investment"

    def _categorize_nutrient_type(self, element_lower: str) -> str | None:
        """Extract nutrient type from element name"""
        nutrients = {
            "vitamin a": "vitamin_a",
            "thiamin": "thiamin",
            "riboflavin": "riboflavin",
            "niacin": "niacin",
            "vitamin c": "vitamin_c",
            "phosphorus": "phosphorus",
            "protein": "protein",
            "fat": "fat",
            "carbohydrate": "carbohydrate",
            "calcium": "calcium",
            "iron": "iron",
            "zinc": "zinc",
            "folate": "folate",
            "vitamin b": "vitamin_b",
            "fiber": "fiber",
        }
        for nutrient, code in nutrients.items():
            if nutrient in element_lower:
                return code
        return None

    def _categorize_emission_source(self, element_lower: str, table_lower: str) -> str:
        """Determine emission source with more detail"""
        if "livestock" in element_lower or "livestock" in table_lower:
            return "livestock"
        elif "crops" in element_lower or "crops" in table_lower:
            return "crops"
        elif "energy" in element_lower or "energy" in table_lower:
            return "energy"
        elif "manure" in element_lower:
            return "manure"
        elif "organic soils" in element_lower or "drained" in table_lower:
            return "organic_soils"
        elif "pre" in table_lower and "post" in table_lower:
            return "pre_post_production"
        elif "agriculture" in table_lower:
            return "agriculture"
        else:
            return "other"

    def _categorize_emission_category(self, element_lower: str) -> str:
        """Extract emission category"""
        if "total" in element_lower:
            return "total"
        elif "direct" in element_lower:
            return "direct"
        elif "indirect" in element_lower:
            return "indirect"
        elif "energy use" in element_lower:
            return "energy_use"
        elif "volatilises" in element_lower:
            return "volatilization"
        elif "applied" in element_lower:
            return "applied"
        else:
            return "general"

    def _categorize_irrigation_type(self, element_lower: str) -> str:
        """Extract irrigation type from element"""
        if "localized" in element_lower:
            return "localized"
        elif "sprinkler" in element_lower:
            return "sprinkler"
        elif "surface" in element_lower:
            return "surface"
        elif "full control" in element_lower:
            return "full_control"
        elif "groundwater" in element_lower:
            return "groundwater"
        elif "drainage" in element_lower:
            return "drainage"
        else:
            return "general"

    def _categorize_investment_measure(self, element_lower: str) -> str:
        """Extract investment measure type"""
        if "gross fixed capital" in element_lower:
            return "gross_fixed_capital_formation"
        elif "orientation index" in element_lower:
            return "agriculture_orientation_index"
        elif "share" in element_lower and "gdp" in element_lower:
            return "share_of_gdp"
        elif "share" in element_lower and "value added" in element_lower:
            return "share_of_value_added"
        elif "share" in element_lower:
            return "share_of_investment"
        else:
            return "value"

    def _categorize_unit(self, element_lower: str) -> str:
        """Extract unit from element name"""
        if "g/capita/day" in element_lower:
            return "g/capita/day"
        elif "mg/capita/day" in element_lower:
            return "mg/capita/day"
        elif "mcg/capita/day" in element_lower:
            return "mcg/capita/day"
        elif "kcal/capita/day" in element_lower:
            return "kcal/capita/day"
        elif "us$" in element_lower or "usd" in element_lower:
            return "USD"
        elif "local currency" in element_lower:
            return "local_currency"
        elif "tonnes" in element_lower:
            return "tonnes"
        elif "hectares" in element_lower:
            return "hectares"
        elif "per 100,000" in element_lower:
            return "per_100000"
        elif "%" in element_lower or "percentage" in element_lower:
            return "percentage"
        else:
            return "unknown"

    def _categorize_industry(self, industry_name: str) -> str:
        """Categorize industry into sectors"""
        industry_lower = industry_name.lower()

        if "agriculture" in industry_lower or "forestry" in industry_lower or "fishing" in industry_lower:
            return "primary"
        elif "manufacture" in industry_lower or "processing" in industry_lower:
            return "secondary"
        elif "retail" in industry_lower or "wholesale" in industry_lower or "trade" in industry_lower:
            return "tertiary_trade"
        elif "transportation" in industry_lower or "storage" in industry_lower:
            return "tertiary_logistics"
        elif "accommodation" in industry_lower or "service" in industry_lower:
            return "tertiary_services"
        elif "total" in industry_lower:
            return "aggregate"
        else:
            return "other"

    def _categorize_factor(self, factor_name: str) -> str:
        """Categorize production factors"""
        factor_lower = factor_name.lower()

        if "labour" in factor_lower or "labor" in factor_lower:
            return "labor"
        elif "taxes" in factor_lower:
            return "fiscal"
        elif "import" in factor_lower:
            return "trade"
        elif "operating surplus" in factor_lower or "profit" in factor_lower:
            return "capital_return"
        elif "capital" in factor_lower:
            return "capital"
        elif "total" in factor_lower:
            return "aggregate"
        else:
            return "other"


relationship_identifier = RelationshipIdentifier()
