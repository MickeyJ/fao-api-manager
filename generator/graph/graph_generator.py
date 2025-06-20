import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from dataclasses import dataclass

from jinja2 import Environment, FileSystemLoader

from generator.file_system import FileSystem
from ..logger import logger
from generator import singularize, snake_to_pascal_case

from .relationship_type_scorer import relationship_type_scorer, TypeScore


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
        self.cache_data = {}
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
                if module["name"] not in ["flags"]:
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

        self.cache_data = data

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

        logger.debug(f"Analyzing {table_name} for relationships... {len(foreign_keys)} foreign keys found")

        # Only create relationships if we have 2+ foreign keys
        if len(foreign_keys) >= 2:
            self._generate_relationship_migration(module)

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

        logger.debug(
            f"  Generated relationship pipeline: {pipeline_name}/ ({rel_info['type']} with {len(rel_info.get('element_codes', []))} elements)"
        )

    def _generate_relationship_migration(self, module: Dict):
        """Generate relationship creation for dataset modules using scoring system"""
        table_name = module["name"]
        foreign_keys = module["model"].get("foreign_keys", [])

        # Use the new scoring system to analyze relationships
        relationship_scores = relationship_type_scorer.analyze_dataset(table_name, foreign_keys)

        if not relationship_scores:
            logger.warning(f"  ⚠️ No relationships found for {table_name}")
            return

        # Consolidate relationships by type across different reference types
        consolidated_relationships = {}

        # Process each reference type's relationships
        for ref_type, scored_relationships in relationship_scores.items():
            logger.debug(f"  Processing {ref_type} relationships for {table_name}")

            for rel_score in scored_relationships:
                # Only generate migrations for high/medium confidence relationships
                if rel_score.confidence not in ["high", "medium"]:
                    logger.info(
                        f"    📉 Skipping {rel_score.type} for {table_name} table (confidence: {rel_score.confidence})"
                    )
                    continue

                # Consolidate by relationship type
                if rel_score.type not in consolidated_relationships:
                    consolidated_relationships[rel_score.type] = []

                consolidated_relationships[rel_score.type].append({"score": rel_score, "ref_type": ref_type})

        # Now generate one migration per relationship TYPE
        for rel_type, rel_data in consolidated_relationships.items():
            logger.debug(f"    Generating {rel_type} relationship (from {len(rel_data)} reference types)")

            # Merge the data from all reference types
            merged_rel_info = self._merge_relationship_data(rel_data, module, foreign_keys)
            if merged_rel_info is None:
                return None  # Can't create relationship

            # Generate the migration files
            self._generate_single_relationship_migration(module, merged_rel_info, merged_rel_info["pattern_info"])

    def _merge_relationship_data(self, rel_data: List[Dict], module: Dict, foreign_keys: List[Dict]) -> Dict | None:
        """Merge relationship data from multiple reference types into one"""

        # Start with the highest scoring one
        best_score_data = max(rel_data, key=lambda x: x["score"].score)
        base_rel_info = self._convert_score_to_migration_info(best_score_data["score"], module, foreign_keys)

        if base_rel_info is None:
            return None  # Can't create relationship

        # Merge in data from other reference types
        for data in rel_data:
            if data == best_score_data:
                continue

            ref_type = data["ref_type"]
            score = data["score"]

            # Add this reference type's data
            if ref_type in score.properties:
                base_rel_info[ref_type] = score.properties[ref_type]

            codes_key = f"{ref_type[:-1]}_codes"
            if codes_key in score.properties:
                base_rel_info[codes_key] = score.properties[codes_key]

        return base_rel_info

    def _convert_score_to_migration_info(
        self, rel_score: TypeScore, module: Dict, foreign_keys: List[Dict]
    ) -> Dict | None:
        """Convert RelationshipScore to migration info format"""

        # Determine source and target nodes based on foreign keys
        # This logic depends on your foreign key structure
        node_info = self._determine_nodes(rel_score, module, foreign_keys)

        if node_info is None:
            return None  # Skip this relationship
        source_fk, target_fk, source_node, target_node = node_info

        # Build the relationship info structure
        rel_info = {
            "type": rel_score.type,
            "source_fk": source_fk,
            "target_fk": target_fk,
            "source_node": source_node,
            "target_node": target_node,
            "properties": rel_score.properties,
            "pattern_info": {
                "source_fk": source_fk,
                "target_fk": target_fk,
                "source_node": source_node,
                "target_node": target_node,
            },
        }

        # Add reference-specific fields dynamically
        ref_type = rel_score.reference_type
        if ref_type in rel_score.properties:
            # e.g., if ref_type is "elements", add "elements" list
            rel_info[ref_type] = rel_score.properties[ref_type]

        # Add codes list (e.g., "element_codes", "indicator_codes")
        codes_key = f"{ref_type[:-1]}_codes"
        if codes_key in rel_score.properties:
            rel_info[codes_key] = rel_score.properties[codes_key]

        return rel_info

    def _determine_nodes(
        self, rel_score: TypeScore, module: Dict, foreign_keys: List[Dict]
    ) -> Tuple[str, str, str, str] | None:
        """Determine source and target nodes for the relationship"""

        # Filter out metadata tables that shouldn't be part of relationships
        metadata_tables = {"flags", "elements", "indicators"}
        meaningful_fks = [fk for fk in foreign_keys if fk["table_name"] not in metadata_tables]

        if len(meaningful_fks) < 2:
            logger.warning(f"  ⚠️ SKIPPING {module['name']}: Only {len(meaningful_fks)} FK(s)")
            return None

        # Special case: bilateral patterns (reporter/partner, donor/recipient)
        for i, fk1 in enumerate(meaningful_fks):
            for fk2 in meaningful_fks[i + 1 :]:
                # Check if these form a bilateral pair
                if self._is_bilateral_pair(fk1["table_name"], fk2["table_name"]):
                    return (
                        fk1["hash_fk_sql_column_name"],
                        fk2["hash_fk_sql_column_name"],
                        fk1["model_name"],
                        fk2["model_name"],
                    )

        # Default: Just use the first two meaningful FKs
        # The relationship type and properties already tell us what this means semantically
        return (
            meaningful_fks[0]["hash_fk_sql_column_name"],
            meaningful_fks[1]["hash_fk_sql_column_name"],
            meaningful_fks[0]["model_name"],
            meaningful_fks[1]["model_name"],
        )

    def _is_bilateral_pair(self, table1: str, table2: str) -> bool:
        """Check if two tables form a bilateral relationship"""
        bilateral_pairs = [("reporter", "partner"), ("donor", "recipient")]

        for word1, word2 in bilateral_pairs:
            if (word1 in table1 and word2 in table2) or (word2 in table1 and word1 in table2):
                return True
        return False
