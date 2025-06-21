from pathlib import Path
from typing import Dict, List, Optional
import yaml
import json
from dataclasses import dataclass
from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from generator.logger import logger
from generator import singularize, make_migration_class_name, to_snake_case

from .project_paths import ProjectPaths
from .template_config import template_config


@dataclass
class NodeConfig:
    table: str
    label: str
    properties: List[str]
    indexes: List[str]


@dataclass
class RelationshipConfig:
    type: str
    source_table: str
    source_fk: str
    source_label: str
    target_table: str
    target_fk: str
    target_label: str
    filters: List[Dict]
    properties_from_row: List[str]


class GraphDBGen:
    """YAML configuration driven graph generator"""

    def __init__(self, output_dir: Path, cache_path: Path, config_path: Path, static_files_dir: str | Path):
        self.project_name = "fao_graph"
        self.output_dir = Path(output_dir) / self.project_name
        self.paths = ProjectPaths(self.output_dir)
        self.cache_path = cache_path
        self.config_path = config_path

        # Load data
        self.cache_data = self._load_cache()
        self.config = self._load_config()
        self.flags = self._load_flags()

        # Setup file system
        self.file_system = FileSystem(self.output_dir, static_files_dir)

        # Setup Jinja
        self.template_dir = Path("generator/graph/templates")
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.jinja_env.filters["snake_case"] = to_snake_case

    def _load_cache(self) -> Dict:
        """Load the module cache data"""
        with open(self.cache_path, "r") as f:
            return json.load(f)

    def _load_config(self) -> Dict:
        """Load the YAML configuration"""
        with open(self.config_path, "r") as f:
            return yaml.safe_load(f)

    def _load_flags(self) -> List[Dict]:
        return self._query_reference_data("flags", "flags")

    def format_pipeline_name(self, table_name: str, rel_type: str) -> str:
        """Format pipeline names to snake_case"""
        return f"{table_name}__{rel_type}".lower()

    def generate(self):
        """Main generation workflow"""
        logger.info("Starting config-driven graph generation...")

        # Create project structure
        self._create_project_structure()

        # Generate nodes
        self._generate_nodes()

        # Generate relationships
        self._generate_relationships()

        # Generate global indexes
        self._generate_global_indexes()

        # Generate orchestration script
        self._generate_orchestration_script()

        # Copy static files
        self.file_system.copy_static_files()

        logger.success("Graph generation complete!")

    def _generate_orchestration_script(self):
        """Generate a master script to run all migrations in order"""

        # Collect all pipelines
        node_pipelines = []
        relationship_pipelines = []

        for node_config in self.config["nodes"]:
            node_pipelines.append({"name": singularize(node_config["table"]), "label": node_config["label"]})

        for table_name, rel_configs in self.config["relationships"].items():
            for rel_config in rel_configs:
                relationship_pipelines.append(
                    {
                        "name": self.format_pipeline_name(table_name, rel_config["type"]),
                        "type": rel_config["type"],
                        "table": table_name,
                    }
                )

        context = {
            "project_name": self.project_name,
            "node_pipelines": node_pipelines,
            "relationship_pipelines": relationship_pipelines,
        }

        template = self.jinja_env.get_template(template_config.orchestrate_migration_template)
        content = template.render(**context)

        self.file_system.write_file_cache(self.paths.db / template_config.orchestrate_migration_filename(), content)

    def _create_project_structure(self):
        """Create the project directory structure"""
        dirs = [
            self.paths.db,
            self.paths.db_pipelines,
            self.paths.api,
            self.paths.core,
        ]
        for dir_path in dirs:
            self.file_system.create_dir(dir_path)

    def _generate_nodes(self):
        """Generate node migrations from config"""
        for node_config in self.config["nodes"]:
            table_name = node_config["table"]

            # Find module in cache
            module = None
            if table_name in self.cache_data.get("references", {}):
                module = self.cache_data["references"][table_name]
            elif table_name in self.cache_data.get("datasets", {}):
                module = self.cache_data["datasets"][table_name]

            if not module:
                logger.warning(f"Table {table_name} not found in cache, skipping")
                continue

            self._generate_node_migration(module, node_config)

    def _generate_node_migration(self, module: Dict, config: Dict):
        """Generate migration files for a node"""
        table_name = config["table"]
        node_name = singularize(table_name)
        pipeline_dir = self.output_dir / self.paths.db_pipelines / node_name
        self.file_system.create_dir(pipeline_dir)

        # Just get the columns from cache - THAT'S IT
        columns = module["model"]["column_analysis"]

        # Get non-excluded columns
        properties = []
        for column in columns:
            properties.append(column["sql_column_name"])

        # Contexts
        context = {
            "project_name": self.project_name,
            "table": table_name,
            "table_name": table_name,
            "node_label": config["label"],
            "properties": properties,
            "migration_query_filename": template_config.node_migration_filename(node_name),
            "verify_query_filename": template_config.node_verify_filename(node_name),
            "index_query_filename": template_config.node_indexes_filename(node_name),
            "pipeline_name": node_name,
            "migration_type": "node",
            "migration_class_name": f"{config['label']}Migrator",
            "description": f"{config['label']} nodes from {table_name}",
        }

        # Generate files
        files_to_generate = [
            (template_config.node_migration_template, template_config.node_migration_filename(node_name)),
            (template_config.node_verify_template, template_config.node_verify_filename(node_name)),
            (template_config.node_indexes_template, template_config.node_indexes_filename(node_name)),
            (template_config.migrator_template, template_config.migrator_filename(node_name)),
            (template_config.pipeline_main_template, template_config.pipeline_main_filename()),
        ]

        for template_name, output_name in files_to_generate:
            template = self.jinja_env.get_template(template_name)
            content = template.render(**context)
            self.file_system.write_file_cache(pipeline_dir / output_name, content)

        # Create __init__.py
        init_content = f'"""Migration pipeline for {config["label"]} nodes"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"Generated node pipeline: {node_name}/")

    def _generate_relationships(self):
        """Generate relationship migrations from config"""
        for table_name, rel_configs in self.config["relationships"].items():
            # Find module in cache
            if table_name not in self.cache_data.get("datasets", {}):
                logger.warning(f"Dataset {table_name} not found in cache, skipping")
                continue

            module = self.cache_data["datasets"][table_name]

            for rel_config in rel_configs:
                self._generate_relationship_migration(module, table_name, rel_config)

    def _generate_relationship_migration(self, module: Dict, table_name: str, config: Dict):
        """Generate migration files for a relationship"""
        rel_type = config["type"].lower()
        pipeline_name = self.format_pipeline_name(table_name, rel_type)
        pipeline_dir = self.paths.db_pipelines / pipeline_name
        self.file_system.create_dir(pipeline_dir)

        # Extract filter info
        filter_values = []
        filter_description = ""
        for filter_rule in config.get("filters", []):
            if filter_rule["field"] == "element_code":
                filter_values = filter_rule["values"]
                filter_description = filter_rule.get("description", "")

        # Get column info from cache
        column_analysis = module["model"]["column_analysis"]
        foreign_keys = module["model"]["foreign_keys"]
        exclude_columns = module["model"].get("exclude_columns", [])

        # Find the actual FK column names for source and target
        source_fk = None
        target_fk = None

        for fk in foreign_keys:
            if fk["table_name"] == config["source"]["table"]:
                source_fk = fk["hash_fk_sql_column_name"]
            elif fk["table_name"] == config["target"]["table"]:
                target_fk = fk["hash_fk_sql_column_name"]

        include_columns = self._determine_properties_from_cache(module, config, source_fk, target_fk)

        # print(f"{table_name} include_columns: {include_columns}")

        # Build list of actual SQL columns (including FK columns)
        sql_columns = []

        # Add FK columns (these replaced the excluded columns)
        for fk in foreign_keys:
            sql_columns.append(fk["hash_fk_sql_column_name"])

        # Add regular columns
        for col in column_analysis:
            if col["csv_column_name"] not in exclude_columns:
                sql_columns.append(col["sql_column_name"])

        # Determine which properties are actually available
        available_properties = []

        for prop in config.get("properties_from_row", []):
            if prop in sql_columns:
                available_properties.append(prop)
            elif prop == "flag" and any(fk["table_name"] == "flags" for fk in foreign_keys):
                # Special case for flag - we'll join to get the actual flag value
                available_properties.append(prop)

        # Check what time/value columns exist
        has_year = "year" in include_columns.get("properties", [])
        has_value = "value" in include_columns.get("properties", [])

        # Build relationship info
        relationship_info = {
            "type": config["type"],
            "source_fk": source_fk,
            "source_label": config["source"]["node_label"],
            "target_fk": target_fk,
            "target_label": config["target"]["node_label"],
            "filters": config.get("filters", []),
            "properties_from_row": include_columns.get("properties", []),
            "has_year": has_year,
            "has_value": has_value,
            "rel_props": include_columns.get("properties", []),
            "identifiers": include_columns.get("identifiers", []),
        }

        # Prepare contexts for different templates
        sql_context = {
            "project_name": self.project_name,
            "table_name": table_name,
            "relationship": relationship_info,
        }

        python_context = {
            "project_name": self.project_name,
            "pipeline_name": pipeline_name,
            "migration_query_filename": template_config.relationship_migration_filename(pipeline_name),
            "verify_query_filename": template_config.relationship_verify_filename(pipeline_name),
            "migration_type": "relationship",
            "table_name": table_name,
            "relationship_type": config["type"],
            "relationship": relationship_info,
            "filter_values": filter_values,
            "filter_description": filter_description,
            "rel_props": include_columns.get("properties", []),
            "identifiers": include_columns.get("identifiers", []),
            "migration_class_name": make_migration_class_name(table_name, config["type"]),
            "description": f"{config['type']} relationships from {table_name}",
        }

        files_to_generate = [
            (
                template_config.relationship_migration_template,
                template_config.relationship_migration_filename(pipeline_name),
                sql_context,
            ),
            (
                template_config.relationship_verify_template,
                template_config.relationship_verify_filename(pipeline_name),
                sql_context,
            ),
            (template_config.migrator_template, template_config.migrator_filename(pipeline_name), python_context),
            (template_config.pipeline_main_template, template_config.pipeline_main_filename(), python_context),
        ]

        for template_name, output_name, context in files_to_generate:
            template = self.jinja_env.get_template(template_name)
            content = template.render(**context)
            self.file_system.write_file_cache(pipeline_dir / output_name, content)

        # Create __init__.py
        init_content = f'"""Migration pipeline for {table_name} {config["type"]} relationships"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"Generated relationship pipeline: {pipeline_name}/")

    def _generate_global_indexes(self):
        """Generate a single file with strategic indexes"""
        # Collect all relationship types and their properties
        rel_types = {}

        for table_name, configs in self.config["relationships"].items():
            # Get the module to check actual columns
            if table_name in self.cache_data.get("datasets", {}):
                module = self.cache_data["datasets"][table_name]
                column_names = [col["sql_column_name"] for col in module["model"]["column_analysis"]]

                for config in configs:
                    rel_type = config["type"]
                    if rel_type not in rel_types:
                        rel_types[rel_type] = {"has_year": False, "has_months": False, "tables": []}

                    # Check what columns this relationship actually uses
                    if "year" in column_names:
                        rel_types[rel_type]["has_year"] = True
                    if "months_code" in column_names:
                        rel_types[rel_type]["has_months"] = True
                    rel_types[rel_type]["tables"].append(table_name)

        context = {"project_name": self.project_name, "relationship_types": rel_types}

        template = self.jinja_env.get_template(template_config.global_indexes_template)
        content = template.render(**context)

        self.file_system.write_file_cache(self.paths.db / template_config.global_indexes_filename(), content)

    def _determine_properties_from_cache(
        self, module: Dict, config: dict, source_fk: str | None, target_fk: str | None
    ) -> dict:
        """Determine actual properties based on what exists in the table"""
        columns = {}
        columns["identifiers"] = {}

        property_groups = config.get("include_property_groups", [])

        # Collect regular columns
        for column in module["model"]["column_analysis"]:
            if column["csv_column_name"] not in module["model"]["exclude_columns"]:
                columns[column["sql_column_name"]] = column["sql_column_name"]

        # Collect foreign key columns
        for column in module["model"]["foreign_keys"]:
            fk_column_name = column["hash_fk_sql_column_name"]
            if fk_column_name not in [source_fk, target_fk]:
                columns["identifiers"][fk_column_name] = column

        properties = []

        for group in property_groups:
            if group == "time_columns":
                # Check what time columns actually exist
                for col in ["year", "months", "date", "period"]:
                    if col in columns:
                        properties.append(col)

            elif group == "value_columns":
                # Check what value columns exist
                for col in ["value", "quantity", "amount", "price"]:
                    if col in columns:
                        properties.append(col)

            elif group == "metadata":
                # Standard metadata columns
                for col in ["unit", "note"]:
                    if col in columns:
                        properties.append(col)

        return {
            "properties": properties,
            "identifiers": columns["identifiers"],
        }

    def _query_reference_data(self, table_name: str, dataset_table_name: str) -> List[Dict]:
        """Query reference data from database dynamically"""
        from _fao_.src.db.database import get_db
        from sqlalchemy import text

        db_gen = get_db()
        db = next(db_gen)

        try:
            # Build dynamic query based on configuration
            query = text(
                f"""
                SELECT DISTINCT 
                    *
                FROM {table_name} r
                WHERE r.source_dataset = '{dataset_table_name}'
            """
            )

            result = db.execute(query, {"table_name": table_name})
            items = [dict(row) for row in result.mappings().all()]

            logger.debug(f"    Got {len(items)} {dataset_table_name} records from {table_name} table")
            return items

        except Exception as e:
            logger.error(f"Failed to query {table_name}: {e}")
            return []
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
