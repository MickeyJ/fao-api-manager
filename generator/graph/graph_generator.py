# generator/graph/graph_generator.py
import json
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from generator.logger import logger
from generator import singularize


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
        node_name_pascal = singularize(module["model"]["model_name"])

        pipeline_dir = self.output_dir / self.paths.db_pipelines / node_name
        self.file_system.create_dir(pipeline_dir)

        # 1. Generate .cypher.sql file
        node_template = self.jinja_env.get_template("reference_node.cypher.sql.jinja2")
        node_content = node_template.render(project_name=self.project_name, module=module, node_name=node_name_pascal)
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}.cypher.sql", node_content)

        # 2. Generate indexes SQL file
        index_template = self.jinja_env.get_template("reference_node_indexes.sql.jinja2")
        index_content = index_template.render(
            project_name=self.project_name, module=module, node_name=node_name, node_name_pascal=node_name_pascal
        )
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}_indexes.sql", index_content)

        # 3. Generate verification SQL file
        verify_template = self.jinja_env.get_template("reference_node_verify.cypher.sql.jinja2")
        verify_content = verify_template.render(project_name=self.project_name, node_name_pascal=node_name_pascal)
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}_verify.cypher.sql", verify_content)

        # 4. Generate migrator Python class
        migrator_template = self.jinja_env.get_template("graph_migrator.py.jinja2")
        migrator_content = migrator_template.render(
            project_name=self.project_name,
            module=module,
            node_name=node_name,
            migration_class_name=f"{node_name_pascal}Migrator",
            migration_type="node",
        )
        self.file_system.write_file_cache(pipeline_dir / f"{node_name}.py", migrator_content)

        # 5. Generate __main__.py
        main_template = self.jinja_env.get_template("graph_pipeline_main.py.jinja2")
        main_content = main_template.render(
            project_name=self.project_name, pipeline_name=node_name, class_name=f"{node_name_pascal}Migrator"
        )
        self.file_system.write_file_cache(pipeline_dir / "__main__.py", main_content)

        # 6. Generate __init__.py
        init_content = f'"""Migration pipeline for {node_name}"""'
        self.file_system.write_file_cache(pipeline_dir / "__init__.py", init_content)

        logger.info(f"  Generated pipeline: {node_name}/")

    def _analyze_for_relationships(self, module: Dict):
        """Analyze dataset module for potential relationships"""
        table_name = module["name"]
        foreign_keys = module.get("foreign_keys", [])

        # Only create relationships if we have 2+ foreign keys
        if len(foreign_keys) >= 2:
            self._generate_relationship_migration(module)

    def _generate_relationship_migration(self, module: Dict):
        """Generate relationship creation for dataset modules"""
        table_name = module["name"]
        foreign_keys = module.get("foreign_keys", [])

        # Determine relationship type from table name
        rel_type = self._get_relationship_type(table_name)

    def _get_relationship_type(self, table_name: str) -> str:
        """Determine relationship type from table name"""
        if "trade" in table_name:
            return "TRADES_WITH"
        elif "production" in table_name:
            return "PRODUCES"
        elif "price" in table_name:
            return "HAS_PRICE"
        else:
            # Fallback: uppercase with underscores
            return table_name.upper()

    def _write_migrations(self):
        """Write all migration files to disk"""
        # Group migrations by pipeline (each module is its own pipeline)
        pipeline_groups = defaultdict(list)
        for migration in self.migrations:
            pipeline_groups[migration.pipeline_name].append(migration)

        # Process each pipeline
        for pipeline_name, migrations in pipeline_groups.items():
            pipeline_dir = self.output_dir / self.paths.db_pipelines / pipeline_name
            self.file_system.create_dir(pipeline_dir)

            # Write the .cypher.sql files
            for migration in migrations:
                file_path = pipeline_dir / migration.filename
                self.file_system.write_file_cache(file_path, migration.content)
                logger.info(f"  Created {pipeline_name}/{migration.filename}")
