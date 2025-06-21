from pathlib import Path
from typing import Dict, List, Optional
import yaml
import json
from dataclasses import dataclass
from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from generator.logger import logger
from generator import singularize, make_migration_class_name, to_snake_case


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


@dataclass
class ProjectPaths:
    project: Path
    core: Path = Path("core")
    db: Path = Path("db")
    db_pipelines: Path = Path("db/pipelines")
    api: Path = Path("api")
    api_routers: Path = Path("api/routers")


class GraphDBGen:
    """YAML configuration driven graph generator"""

    def __init__(self, output_dir: Path, cache_path: Path, config_path: Path):
        self.project_name = "fao_graph"
        self.output_dir = Path(output_dir) / self.project_name
        self.paths = ProjectPaths(self.output_dir)
        self.cache_path = cache_path
        self.config_path = config_path

        # Load data
        self.cache_data = self._load_cache()
        self.config = self._load_config()

        # Setup file system
        self.file_system = FileSystem(self.output_dir, "_fao_graph_")

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

        template = self.jinja_env.get_template("orchestrate_migration.py.jinja2")
        content = template.render(**context)

        self.file_system.write_file_cache(self.output_dir / "db" / "orchestrate_migration.py", content)

    def _create_project_structure(self):
        """Create the project directory structure"""
        dirs = [
            self.output_dir / self.paths.db,
            self.output_dir / self.paths.db_pipelines,
            self.output_dir / self.paths.api,
            self.output_dir / self.paths.core,
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
        pipeline_dir = self.output_dir / "db" / "pipelines" / node_name
        self.file_system.create_dir(pipeline_dir)

        # Prepare contexts for different templates
        sql_context = {
            "project_name": self.project_name,
            "table": table_name,
            "node_label": config["label"],
            "properties": config.get("properties", []),
            "indexes": config.get("indexes", []),
        }

        python_context = {
            "project_name": self.project_name,
            "pipeline_name": node_name,
            "pipeline_type": "node",
            "table_name": table_name,
            "node_label": config["label"],
            "migration_class_name": f"{config['label']}Migrator",
            "description": f"{config['label']} nodes from {table_name}",
        }

        # Generate all files
        files_to_generate = [
            ("yaml_node_migration.cypher.sql.jinja2", f"{node_name}.cypher.sql", sql_context),
            ("yaml_node_indexes.sql.jinja2", f"{node_name}_indexes.sql", sql_context),
            ("yaml_node_verify.cypher.sql.jinja2", f"{node_name}_verify.cypher.sql", sql_context),
            ("yaml_migrator.py.jinja2", f"{node_name}.py", python_context),
            ("yaml_pipeline_main.py.jinja2", "__main__.py", python_context),
        ]

        for template_name, output_name, context in files_to_generate:
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
        pipeline_dir = self.output_dir / self.paths.db_pipelines / pipeline_name
        self.file_system.create_dir(pipeline_dir)

        # Extract filter info
        filter_values = []
        filter_description = ""
        for filter_rule in config.get("filters", []):
            if filter_rule["field"] == "element_code":
                filter_values = filter_rule["values"]
                filter_description = filter_rule.get("description", "")

        relationship_info = {
            "type": config["type"],
            "source_fk": config["source"]["foreign_key"],
            "source_label": config["source"]["node_label"],
            "target_fk": config["target"]["foreign_key"],
            "target_label": config["target"]["node_label"],
            "filters": config.get("filters", []),
            "properties_from_row": config.get("properties_from_row", []),
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
            "pipeline_type": "relationship",
            "table_name": table_name,
            "relationship_type": config["type"],
            "relationship": relationship_info,
            "filter_values": filter_values,
            "filter_description": filter_description,
            "migration_class_name": make_migration_class_name(table_name, config["type"]),
            "description": f"{config['type']} relationships from {table_name}",
        }

        # Generate all files
        files_to_generate = [
            ("yaml_relationship_migration.cypher.sql.jinja2", f"{pipeline_name}.cypher.sql", sql_context),
            ("yaml_relationship_verify.cypher.sql.jinja2", f"{pipeline_name}_verify.cypher.sql", sql_context),
            ("yaml_migrator.py.jinja2", f"{pipeline_name}.py", python_context),
            ("yaml_pipeline_main.py.jinja2", "__main__.py", python_context),
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
        # Collect all relationship types
        rel_types = set()
        for _, configs in self.config["relationships"].items():
            for config in configs:
                rel_types.add(config["type"])

        context = {"project_name": self.project_name, "relationship_types": sorted(rel_types)}

        template = self.jinja_env.get_template("yaml_global_indexes.sql.jinja2")
        content = template.render(**context)

        self.file_system.write_file_cache(self.output_dir / self.paths.db / "create_indexes.sql", content)
