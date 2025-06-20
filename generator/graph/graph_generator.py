import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from generator.logger import logger
from generator import singularize, snake_to_pascal_case


@dataclass
class GraphMigration:
    """Represents a single migration file to generate"""

    filename: str
    migration_type: str  # 'node' or 'relationship'
    content: str
    pipeline_name: str


@dataclass
class ProjectPaths:
    project: Path
    db: Path = Path("db")
    db_pipelines: Path = Path("db/pipelines")
    api: Path = Path("api")
    api_routers: Path = Path("api/routers")


class GraphGenerator:
    def __init__(self, output_dir: str | Path, cache_path: str | Path):
        self.project_name = "fao_graph"
        self.output_dir = Path(output_dir) / self.project_name
        self.paths = ProjectPaths(self.output_dir)
        self.file_system = FileSystem(self.paths.project, "_fao_graph_")
        self.cache_path = cache_path
        self.migrations = []

        self.template_dir = Path("generator/graph/templates")
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def generate(self):
        """Main generation workflow"""
        logger.info("Starting graph database generation...")

        # Load the cached module data
        modules = self._load_cache_data()

        # Process each module
        for module in modules:
            if module.get("is_reference_module"):
                self._generate_node_migration(module)
            else:
                self._analyze_for_relationships(module)

        # Write all migrations
        self._write_migrations()
        self.file_system.copy_static_files()

        logger.success(f"Generated {len(self.migrations)} migration files")

    def _load_cache_data(self) -> List[Dict]:
        """Load modules from the cache"""
        with open(self.cache_path, "r") as f:
            data = json.load(f)

        # Combine references and datasets
        all_modules = []
        if "references" in data:
            all_modules.extend(data["references"].values())
        if "datasets" in data:
            all_modules.extend(data["datasets"].values())

        return all_modules

    def _generate_node_migration(self, module: Dict):
        """Generate all files for a reference node pipeline"""
        module_name = module["name"]
        node_name = singularize(module_name)
        node_label = singularize(module["model"]["model_name"])

        pipeline_dir = self.output_dir / self.paths.db_pipelines / node_name
        self.file_system.create_dir(pipeline_dir)

        # 1. Generate .cypher.sql file
        node_template = self.jinja_env.get_template("reference_node.cypher.sql.jinja2")
        node_content = node_template.render(project_name=self.project_name, module=module, node_name=node_label)
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}.cypher.sql", node_content)

        # 2. Generate indexes SQL file
        index_template = self.jinja_env.get_template("reference_node_indexes.sql.jinja2")
        index_content = index_template.render(
            project_name=self.project_name, module=module, node_name=node_name, node_label=node_label
        )
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}_indexes.sql", index_content)

        # 3. Generate verification SQL file
        verify_template = self.jinja_env.get_template("reference_node_verify.cypher.sql.jinja2")
        verify_content = verify_template.render(project_name=self.project_name, node_label=node_label)
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}_verify.cypher.sql", verify_content)

        # 4. Generate migrator Python class
        migrator_template = self.jinja_env.get_template("graph_migrator.py.jinja2")
        migrator_content = migrator_template.render(
            project_name=self.project_name,
            module=module,
            node_name=node_name,
            migration_class_name=f"{node_label}Migrator",
            migration_type="node",
            node_label=node_label,
        )
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}.py", migrator_content)

        # 5. Generate __main__.py
        main_template = self.jinja_env.get_template("graph_pipeline_main.py.jinja2")
        main_content = main_template.render(
            project_name=self.project_name, pipeline_name=node_name, class_name=f"{node_label}Migrator"
        )
        self.file_system.write_file_cache(pipeline_dir / "__main__.py", main_content)

        # 6. Generate __init__.py
        init_content = f'"""Migration pipeline for {node_name}"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"  Generated pipeline: {node_name}/")

    def _analyze_for_relationships(self, module: dict):
        """Analyze dataset module for potential relationships"""
        table_name = module["name"]
        foreign_keys = module["model"].get("foreign_keys", [])

        logger.info(f"Analyzing {table_name} for relationships... {len(foreign_keys)} foreign keys found")

        # Only create relationships if we have 2+ foreign keys
        if len(foreign_keys) >= 2:
            self._generate_relationship_migration(module)

    def _generate_relationship_migration(self, module: Dict):
        """Generate relationship creation for dataset modules"""
        table_name = module["name"]
        foreign_keys = module["model"].get("foreign_keys", [])

        # Get relationship pattern
        pattern_info = self._determine_relationship_pattern(table_name, foreign_keys)

        if not pattern_info:
            logger.warning(f"  ⚠️ Could not determine pattern for {table_name}")
            return

        # For dynamic patterns, analyze elements to determine relationship types
        if pattern_info.get("relationships") == "dynamic":
            # Simulate element data (in production, this would query the database)
            element_data = self._simulate_table_elements(table_name)

            # Group elements by relationship type
            relationship_groups = self._group_elements_by_relationship(table_name, element_data, pattern_info)

            # Generate a migration for each relationship type found
            for rel_type, rel_info in relationship_groups.items():
                self._generate_single_relationship_migration(module, rel_info, pattern_info)
        else:
            # Fixed relationship pattern (like bilateral trade)
            for rel_info in pattern_info["relationships"]:
                self._generate_single_relationship_migration(module, rel_info, pattern_info)

    def _generate_single_relationship_migration(self, module: Dict, rel_info: Dict, pattern_info: Dict):
        """Generate migration files for a specific relationship type"""
        table_name = module["name"]
        pipeline_name = f"{table_name}_{rel_info['type'].lower()}"
        pipeline_dir = self.output_dir / self.paths.db_pipelines / pipeline_name
        self.file_system.create_dir(pipeline_dir)

        # Prepare context for templates
        context = {
            "project_name": self.project_name,
            "module": module,
            "table_name": table_name,
            "relationship": rel_info,
            "pipeline_name": pipeline_name,
            "migration_class_name": f"{snake_to_pascal_case(table_name)}{rel_info['type'].title()}Migrator",
        }

        # 1. Generate relationship .cypher.sql file
        rel_template = self.jinja_env.get_template("dataset_relationship.cypher.sql.jinja2")
        rel_content = rel_template.render(**context)
        self.file_system.write_file_cache(pipeline_dir / f"{pipeline_name}.cypher.sql", rel_content)

        # 2. Generate indexes
        index_template = self.jinja_env.get_template("relationship_indexes.sql.jinja2")
        index_content = index_template.render(**context)
        self.file_system.write_file_cache(pipeline_dir / f"{pipeline_name}_indexes.sql", index_content)

        # 3. Generate verification
        verify_template = self.jinja_env.get_template("relationship_verify.cypher.sql.jinja2")
        verify_content = verify_template.render(**context)
        self.file_system.write_file_cache(pipeline_dir / f"{pipeline_name}_verify.cypher.sql", verify_content)

        # 4. Generate migrator Python class
        migrator_template = self.jinja_env.get_template("dataset_migrator.py.jinja2")
        migrator_content = migrator_template.render(**context)
        self.file_system.write_file_cache(pipeline_dir / f"{pipeline_name}.py", migrator_content)

        # 5. Generate __main__.py
        main_template = self.jinja_env.get_template("graph_pipeline_main.py.jinja2")
        main_content = main_template.render(
            project_name=self.project_name, pipeline_name=pipeline_name, class_name=context["migration_class_name"]
        )
        self.file_system.write_file_cache(pipeline_dir / "__main__.py", main_content)

        # 6. Generate __init__.py
        init_content = f'"""Migration pipeline for {table_name} {rel_info["type"]} relationships"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(
            f"  Generated relationship pipeline: {pipeline_name}/ ({rel_info['type']} with {len(rel_info.get('element_codes', []))} elements)"
        )

    def _determine_relationship_pattern(self, table_name: str, foreign_keys: List[Dict]) -> Optional[Dict]:
        """Determine relationship patterns for a dataset"""
        fk_tables = [fk["table_name"] for fk in foreign_keys]

        # Bilateral trade pattern
        if "reporter_country_codes" in fk_tables and "partner_country_codes" in fk_tables:
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

    def _simulate_table_elements(self, table_name: str) -> List[Dict]:
        """
        Simulate element data for a table
        In production, this would query the database
        """
        # Common element patterns by table type
        element_patterns = {
            "trade": [
                {"element_code": "5610", "element": "Import quantity"},
                {"element_code": "5622", "element": "Import value"},
                {"element_code": "5910", "element": "Export quantity"},
                {"element_code": "5922", "element": "Export value"},
            ],
            "production": [
                {"element_code": "5510", "element": "Production"},
                {"element_code": "5419", "element": "Yield"},
                {"element_code": "5312", "element": "Area harvested"},
            ],
            "emissions": [
                {"element_code": "7231", "element": "Emissions (CO2)"},
                {"element_code": "7230", "element": "Emissions (N2O)"},
                {"element_code": "7229", "element": "Emissions (CH4)"},
            ],
            "inputs": [
                {"element_code": "5157", "element": "Agricultural Use"},
                {"element_code": "5159", "element": "Use per area of cropland"},
            ],
            "prices": [
                {"element_code": "5530", "element": "Producer Price (LCU/tonne)"},
                {"element_code": "5532", "element": "Producer Price (USD/tonne)"},
            ],
            "food_balance": [
                {"element_code": "5511", "element": "Production"},
                {"element_code": "5611", "element": "Import quantity"},
                {"element_code": "5911", "element": "Export quantity"},
                {"element_code": "5142", "element": "Food"},
                {"element_code": "5521", "element": "Feed"},
            ],
        }

        # Match table name to pattern
        table_lower = table_name.lower()
        for pattern_key, elements in element_patterns.items():
            if pattern_key in table_lower:
                return elements

        # Default elements
        return [{"element_code": "5110", "element": "Value"}]

    def _group_elements_by_relationship(
        self, table_name: str, elements: List[Dict], pattern_info: Dict
    ) -> Dict[str, Dict]:
        """
        Group elements by their inferred relationship type
        """
        relationship_groups = {}

        for element in elements:
            rel_info = self._infer_relationship_from_element(element["element"], element["element_code"], table_name)

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

    def _infer_relationship_from_element(self, element_name: str, element_code: str, table_name: str) -> Optional[Dict]:
        """
        Infer relationship type using table context first, then element details
        """
        element_lower = element_name.lower()
        table_lower = table_name.lower()

        # TRADE context tables
        if "trade" in table_lower:
            if "import" in element_lower or "export" in element_lower:
                flow = "import" if "import" in element_lower else "export"
            else:
                flow = "unspecified"

            return {
                "type": "TRADES",
                "properties": {
                    "flow": flow,
                    "measure": self._extract_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # PRODUCTION context tables
        elif "production" in table_lower:
            return {
                "type": "PRODUCES",
                "properties": {
                    "measure": self._extract_measure(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # EMISSIONS context tables
        elif "emissions" in table_lower:
            source = "livestock" if "livestock" in table_lower else "crops" if "crops" in table_lower else "other"
            return {
                "type": "EMITS",
                "properties": {
                    "source": source,
                    "gas_type": self._extract_gas_type(element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # WATER/AQUASTAT context tables
        elif "aquastat" in table_lower:
            if "withdrawal" in element_lower:
                return {
                    "type": "USES",
                    "properties": {
                        "resource": "water",
                        "purpose": self._extract_water_purpose(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }
            else:
                return {
                    "type": "MEASURES",
                    "properties": {
                        "category": "water_infrastructure",
                        "subcategory": self._extract_water_subcategory(element_lower),
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
                        "measure": self._extract_measure(element_lower),
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
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # INVESTMENT context tables
        elif "investment" in table_lower or "credit" in table_lower:
            return {
                "type": "MEASURES",
                "properties": {
                    "category": "financial",
                    "flow_type": self._extract_investment_type(table_lower, element_lower),
                    "element_code": element_code,
                    "element": element_name,
                },
            }

        # FOOD BALANCE/SUPPLY tables
        elif "food_balance" in table_lower or "sua" in table_lower:
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
            elif "feed" in element_lower or "food" in element_lower or "supply" in element_lower:
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
                        "measure": self._extract_measure(element_lower),
                        "element_code": element_code,
                        "element": element_name,
                    },
                }

        # Default fallback
        return None

    def _extract_measure(self, element_lower: str) -> str:
        """Extract what's being measured from element name"""
        if "value" in element_lower:
            return "value"
        elif "quantity" in element_lower:
            return "quantity"
        elif "area" in element_lower:
            return "area"
        elif "yield" in element_lower:
            return "yield"
        elif any(nutrient in element_lower for nutrient in ["protein", "fat", "vitamin", "calcium"]):
            return "nutrients"
        else:
            return "other"

    def _extract_gas_type(self, element_lower: str) -> str:
        """Extract greenhouse gas type from element name"""
        if "co2" in element_lower:
            return "CO2"
        elif "n2o" in element_lower:
            return "N2O"
        elif "ch4" in element_lower:
            return "CH4"
        else:
            return "unspecified"

    def _extract_water_purpose(self, element_lower: str) -> str:
        """Extract water use purpose from element name"""
        if "agriculture" in element_lower or "irrigation" in element_lower:
            return "agriculture"
        elif "industrial" in element_lower:
            return "industrial"
        elif "municipal" in element_lower:
            return "municipal"
        else:
            return "general"

    def _extract_water_subcategory(self, element_lower: str) -> str:
        """Extract water infrastructure subcategory"""
        if "equipped" in element_lower:
            return "equipped_area"
        elif "irrigated" in element_lower:
            return "irrigated_area"
        elif "groundwater" in element_lower:
            return "groundwater"
        elif "surface" in element_lower:
            return "surface_water"
        else:
            return "general"

    def _extract_investment_type(self, table_lower: str, element_lower: str) -> str:
        """Extract investment/financial flow type"""
        if "credit" in table_lower:
            return "credit"
        elif "foreign" in table_lower:
            return "foreign_direct_investment"
        elif "government" in table_lower:
            return "government_expenditure"
        elif "assistance" in table_lower:
            return "development_assistance"
        else:
            return "general_investment"

    def _write_migrations(self):
        """Write all migration files to disk"""
        # This method is no longer needed since we write files directly
        pass
