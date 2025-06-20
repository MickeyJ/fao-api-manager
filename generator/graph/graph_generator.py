import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from generator.logger import logger
from generator import singularize, snake_to_pascal_case
from .relationship_identifier import relationship_identifier


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

        pattern_info = relationship_identifier.determine_relationship_pattern(table_name, foreign_keys)

        if not pattern_info:
            logger.warning(f"  ⚠️ Could not determine pattern for {table_name}")
            return

        # Handle dynamic patterns
        if pattern_info.get("relationships") in ["dynamic", "dynamic_indicators", "dynamic_references"]:
            # Determine which reference types this table uses
            reference_types = relationship_identifier.determine_reference_types_for_table(table_name, foreign_keys)

            if not reference_types:
                logger.warning(f"  ⚠️ No reference types found for {table_name}, skipping relationships")
                return

            # Collect relationship groups from all reference types
            all_relationship_groups = {}

            for ref_type in reference_types:
                # Get the reference data (elements, indicators, food_values, etc.)
                ref_items = relationship_identifier.get_reference_data_for_table(table_name, ref_type)

                if ref_items:
                    # Group by relationship type using the appropriate inference method
                    groups = relationship_identifier.group_by_relationship_type(
                        table_name, ref_items, pattern_info, ref_type
                    )

                    # Merge groups (handling potential conflicts)
                    for rel_type, rel_info in groups.items():
                        if rel_type in all_relationship_groups:
                            # Merge reference items if same relationship type from different sources
                            existing = all_relationship_groups[rel_type]

                            # Merge the codes and items lists
                            for key, value in rel_info.items():
                                if isinstance(value, list) and key in existing:
                                    existing[key].extend(value)
                                elif key == "properties":
                                    # Merge properties
                                    existing[key].update(value)
                        else:
                            all_relationship_groups[rel_type] = rel_info
                else:
                    logger.info(f"  No {ref_type} found for {table_name}")

            # Generate a migration for each relationship type found
            for rel_type, rel_info in all_relationship_groups.items():
                self._generate_single_relationship_migration(module, rel_info, pattern_info)

        # Handle fixed patterns (bilateral trade, etc.)
        else:
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
