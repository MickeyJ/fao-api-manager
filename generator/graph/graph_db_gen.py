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
        self.relationship_type_properties = {}

        # Load data
        self.cache_data = self._load_cache()
        self.yml_config = self._load_config()
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

    def _add_relationship_type_properties(self, rel_type: str, include_property: str):
        """Add a include_property to the relationship type properties"""
        if rel_type not in self.relationship_type_properties:
            self.relationship_type_properties[rel_type] = {
                "_self_": "_self_",
            }
        if include_property not in self.relationship_type_properties[rel_type]:
            self.relationship_type_properties[rel_type][include_property] = include_property

    def _get_node_source_datasets(self) -> Dict[str, list[str]]:
        node_source_datasets = {}
        for table_name, yml_relationship_configs in self.yml_config["relationships"].items():
            # Find json_module in cache
            if table_name not in self.cache_data.get("datasets", {}):
                continue
            # collect source_datasets per node
            for yml_relationship_config in yml_relationship_configs:
                source_node = yml_relationship_config["source"]["node_label"]
                target_node = yml_relationship_config["target"]["node_label"]
                if source_node not in node_source_datasets:
                    node_source_datasets[source_node] = [table_name]
                else:
                    node_source_datasets[source_node].append(table_name)

                if target_node not in node_source_datasets:
                    node_source_datasets[target_node] = [table_name]
                else:
                    node_source_datasets[target_node].append(table_name)

        return node_source_datasets

    def generate(self):
        """Main generation workflow"""
        logger.info("Starting yml_config-driven graph generation...")

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

        for node_config in self.yml_config["nodes"]:
            node_pipelines.append({"name": singularize(node_config["table"]), "label": node_config["label"]})

        for table_name, yml_relationship_configs in self.yml_config["relationships"].items():
            for yml_relationship_config in yml_relationship_configs:
                relationship_pipelines.append(
                    {
                        "name": self.format_pipeline_name(table_name, yml_relationship_config["type"]),
                        "type": yml_relationship_config["type"],
                        "table": table_name,
                    }
                )

        context = {
            "project_name": self.project_name,
            "node_pipelines": node_pipelines,
            "relationship_pipelines": relationship_pipelines,
            "yaml_config_path": self.config_path.name,
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
        """Generate node migrations from yml_config"""
        node_source_datasets = self._get_node_source_datasets()
        for node_config in self.yml_config["nodes"]:
            table_name = node_config["table"]

            # Find module in cache
            json_module = None
            if table_name in self.cache_data.get("references", {}):
                json_module = self.cache_data["references"][table_name]
            elif table_name in self.cache_data.get("datasets", {}):
                json_module = self.cache_data["datasets"][table_name]

            if not json_module:
                logger.warning(f"Table {table_name} not found in cache, skipping")
                continue

            self._generate_node_migration(json_module, node_config, node_source_datasets[node_config["label"]])

    def _generate_node_migration(
        self, json_module: Dict, yml_node_config: Dict, source_datasets: List[str] | None = None
    ):
        """Generate migration files for a node"""
        table_name = yml_node_config["table"]
        node_name = singularize(table_name)
        pipeline_dir = self.output_dir / self.paths.db_pipelines / node_name
        self.file_system.create_dir(pipeline_dir)

        # Just get the columns from cache - THAT'S IT
        columns = json_module["model"]["column_analysis"]

        # Get non-excluded columns
        properties = []
        for column in columns:
            properties.append(column["sql_column_name"])

        # Contexts
        context = {
            "project_name": self.project_name,
            "table": table_name,
            "table_name": table_name,
            "node_label": yml_node_config["label"],
            "properties": properties,
            "primary_property": json_module["model"]["pk_sql_column_name"],
            "migration_query_filename": template_config.node_migration_filename(node_name),
            "verify_query_filename": template_config.node_verify_filename(node_name),
            "index_query_filename": template_config.node_indexes_filename(node_name),
            "pipeline_name": node_name,
            "migration_type": "node",
            "migration_class_name": f"{yml_node_config['label']}Migrator",
            "description": f"{yml_node_config['label']} nodes from {table_name}",
            "source_datasets": source_datasets,
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
        init_content = f'"""Migration pipeline for {yml_node_config["label"]} nodes"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"Generated node pipeline: {node_name}/")

    def _generate_relationships(self):
        """Generate relationship migrations from yml_config"""

        for table_name, yml_relationship_configs in self.yml_config["relationships"].items():
            # Find json_module in cache
            if table_name not in self.cache_data.get("datasets", {}):
                logger.error(f"Dataset {table_name} not found in cache, skipping")
                continue

            json_module = self.cache_data["datasets"][table_name]

            for yml_relationship_config in yml_relationship_configs:
                self._generate_relationship_migration(json_module, table_name, yml_relationship_config)

    def _generate_relationship_migration(self, json_module: Dict, table_name: str, yml_relationship_config: Dict):
        """Generate migration files for a relationship"""
        rel_type = yml_relationship_config["type"].lower()
        batch_size = yml_relationship_config.get("batch_size", self.yml_config["settings"]["batch_size"])
        pipeline_name = self.format_pipeline_name(table_name, rel_type)
        pipeline_dir = self.paths.db_pipelines / pipeline_name
        self.file_system.create_dir(pipeline_dir)

        include_properties, source_fk, target_fk = self._determine_properties_from_cache(
            json_module, yml_relationship_config
        )

        # print(f"{table_name} include_columns: {include_columns}")

        # Check what time/value columns exist
        has_year = "year" in include_properties
        has_value = "value" in include_properties

        # Build relationship info
        relationship_info = {
            "type": yml_relationship_config["type"],
            "source_fk": source_fk,
            "source_label": yml_relationship_config["source"]["node_label"],
            "target_fk": target_fk,
            "target_label": yml_relationship_config["target"]["node_label"],
            "filters": yml_relationship_config.get("filters", []),
            "has_year": has_year,
            "has_value": has_value,
            "include_properties": include_properties,
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
            "total_rows_query_filename": template_config.relationship_total_rows_filename(pipeline_name),
            "verify_query_filename": template_config.relationship_verify_filename(pipeline_name),
            "migration_type": "relationship",
            "table_name": table_name,
            "relationship_type": yml_relationship_config["type"],
            "relationship": relationship_info,
            "migration_class_name": make_migration_class_name(table_name, yml_relationship_config["type"]),
            "description": f"{yml_relationship_config['type']} relationships from {table_name}",
            "batch_size": batch_size,
        }

        files_to_generate = [
            (
                template_config.relationship_migration_template,
                template_config.relationship_migration_filename(pipeline_name),
                sql_context,
            ),
            (
                template_config.relationship_total_rows_template,
                template_config.relationship_total_rows_filename(pipeline_name),
                {**sql_context, "count_only": True},
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
        init_content = f'"""Migration pipeline for {table_name} {yml_relationship_config["type"]} relationships"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"Generated relationship pipeline: {pipeline_name}/")

    def _generate_global_indexes(self):
        """Generate a single file with strategic indexes"""

        # Create python file for relationship type properties
        rel_type_py_context = {
            "relationship_type_properties": self.relationship_type_properties,
        }
        template = self.jinja_env.get_template(template_config.rel_type_props_template)
        content = template.render(**rel_type_py_context)
        self.file_system.write_file_cache(self.paths.db / template_config.rel_type_props_filename(), content)

        indexes_dir = self.paths.db / "indexes"
        self.file_system.create_dir(indexes_dir)

        for rel_type, properties in self.relationship_type_properties.items():
            for property in properties:
                rel_type_sql_context = {
                    "project_name": self.project_name,
                    "rel_type": rel_type,
                    "property": property,
                }
                template = self.jinja_env.get_template(template_config.rel_type_property_indexes_template)
                content = template.render(**rel_type_sql_context)
                self.file_system.write_file_cache(
                    indexes_dir / template_config.rel_type_property_indexes_filename(rel_type, property), content
                )

    def _determine_properties_from_cache(
        self, json_module: Dict, yml_relationship_config: dict
    ) -> tuple[Dict, str, str]:
        """Determine actual include_properties based on what exists in the table"""
        include_properties = {}
        source_fk: str = ""
        target_fk: str = ""

        column_analysis = json_module["model"]["column_analysis"]
        foreign_keys = json_module["model"]["foreign_keys"]
        exclude_columns = json_module["model"]["exclude_columns"]
        exclude_properties = yml_relationship_config.get("exclude_properties", [])

        # Collect regular columns [value, year, etc.]
        for column in column_analysis:
            csv_column_name = column["csv_column_name"]
            sql_column_name = column["sql_column_name"]
            if csv_column_name not in exclude_columns and sql_column_name not in exclude_properties:
                include_properties[sql_column_name] = column
                include_properties[sql_column_name]["is_foreign_key"] = False
                self._add_relationship_type_properties(yml_relationship_config["type"], sql_column_name)

        # Collect foreign key columns [item_code_id, element_code_id, etc.]
        for column in foreign_keys:
            fk_column_name = column["hash_fk_sql_column_name"]
            fk_table_name = column["table_name"]

            if fk_table_name == yml_relationship_config["source"]["table"]:
                source_fk = fk_column_name
            elif fk_table_name == yml_relationship_config["target"]["table"]:
                target_fk = fk_column_name

            if fk_column_name not in [source_fk, target_fk] and fk_column_name not in exclude_properties:
                description_column = column["reference_description_column"]
                if description_column in self.yml_config["settings"]["replace_columns"]:
                    description_column = self.yml_config["settings"]["replace_columns"][
                        column["reference_description_column"]
                    ]
                    column["column_as"] = description_column
                include_properties[fk_column_name] = column
                include_properties[fk_column_name]["is_foreign_key"] = True

                self._add_relationship_type_properties(yml_relationship_config["type"], column["sql_column_name"])
                self._add_relationship_type_properties(yml_relationship_config["type"], description_column)

        return (include_properties, source_fk, target_fk)

    def _query_reference_data(self, table_name: str, dataset_table_name: str) -> List[Dict]:
        """Query reference data from database dynamically"""
        from sqlalchemy import text
        from _fao_graph_.db.db_connections import db_connections

        try:
            with db_connections.pg_session() as session:
                # Build dynamic query based on configuration
                query = text(
                    f"""
                    SELECT DISTINCT 
                        *
                    FROM {table_name} r
                    WHERE r.source_dataset = '{dataset_table_name}'
                    """
                )

                result = session.execute(query)
                items = [dict(row) for row in result.mappings().all()]

                logger.debug(f"    Got {len(items)} {dataset_table_name} records from {table_name} table")
                return items

        except Exception as e:
            logger.error(f"Failed to query {table_name}: {e}")
            return []
