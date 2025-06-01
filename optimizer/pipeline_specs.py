# optimizer/pipeline_specs.py
from typing import Dict, List, Set, Optional
from dataclasses import dataclass
import json
from pathlib import Path
from generator import logger


@dataclass
class CoreTableSpec:
    table_name: str
    primary_key_column: str
    description_column: str
    source_files: List[str]
    unifiable: bool
    conflicts: List | None = None


@dataclass
class DataTableSpec:
    table_name: str
    exclude_columns: List[str]  # Change from Set[str] to List[str]
    foreign_key_mappings: Dict[str, str]


class PipelineSpecs:
    def __init__(self, pattern_discovery_results: Dict):
        self.file_groups = pattern_discovery_results["file_groups"]
        self.pattern_analysis = pattern_discovery_results["pattern_analysis"]
        self.core_table_specs = {}
        self.data_table_specs = {}
        logger.info(
            f"📋 Initializing PipelineSpecs with {len(self.pattern_analysis)} analyzed patterns"
        )

    def generate_specifications(self) -> Dict:
        """Convert pattern discovery results into pipeline generation specs"""
        logger.info("🔧 Generating pipeline specifications...")

        # Step 1: Identify core/lookup tables
        self._identify_core_tables()

        # Step 2: Generate data table modifications
        self._generate_data_table_specs()

        # Step 3: Create specification output
        specs = {
            "core_tables": {
                name: spec.__dict__ for name, spec in self.core_table_specs.items()
            },
            "data_table_modifications": {
                name: spec.__dict__ for name, spec in self.data_table_specs.items()
            },
            "generation_rules": self._create_generation_rules(),
        }

        logger.info(
            f"✅ Generated specs: {len(self.core_table_specs)} core tables, {len(self.data_table_specs)} data table modifications"
        )
        self.save_specifications(specs=specs)
        return specs

    def _identify_core_tables(self):
        """Identify which patterns should become core/lookup tables"""
        logger.info("🔍 Identifying core/lookup tables...")

        # Use patterns that were actually analyzed and are unifiable
        for pattern_name, analysis in self.pattern_analysis.items():
            if analysis.get("unifiable") and len(analysis.get("files", [])) >= 3:
                logger.info(
                    f"   ✅ {pattern_name}: unifiable with {len(analysis.get('files', []))} source files"
                )

                self.core_table_specs[pattern_name.lower()] = CoreTableSpec(
                    table_name=pattern_name.lower(),
                    primary_key_column=analysis["primary_key_column"],
                    description_column=analysis["description_column"],
                    source_files=[f["csv_file"] for f in analysis["files"]],
                    unifiable=True,
                    conflicts=analysis.get("conflicts", []),
                )
            else:
                reason = (
                    "not unifiable"
                    if not analysis.get("unifiable")
                    else f"only {len(analysis.get('files', []))} files"
                )
                logger.info(f"   ❌ {pattern_name}: skipped ({reason})")

        logger.info(
            f"🎯 Identified {len(self.core_table_specs)} core tables for unification"
        )

    def _generate_data_table_specs(self):
        """Generate specs for how data tables should be modified"""
        logger.info("🔗 Generating data table modification specs...")

        for core_name, core_spec in self.core_table_specs.items():
            logger.info(f"   📊 Checking references to core table '{core_name}'...")

            # Find the original pattern name in pattern_analysis (case-sensitive)
            original_pattern_name = None
            for pattern_name in self.pattern_analysis.keys():
                if pattern_name.lower() == core_name:
                    original_pattern_name = pattern_name
                    break

            if not original_pattern_name:
                logger.warning(
                    f"   ⚠️ Could not find pattern analysis for '{core_name}'"
                )
                continue

            # Get all columns from the core table pattern
            core_table_columns = set(
                self.pattern_analysis[original_pattern_name]["columns"]
            )
            logger.info(f" core_table_columns: {core_table_columns}'")
            references_found = 0

            # For each data table pattern, check if its columns reference this core table
            for (columns_sig, pattern_name), files in self.file_groups.items():
                # Skip if this IS a core table
                if pattern_name.lower() == core_name:
                    continue

                exclude_cols = []
                fk_mappings = {}

                for col in columns_sig:
                    related_to_core = any(
                        col.lower().startswith(core_col.lower())
                        for core_col in core_table_columns
                    )

                    if related_to_core:
                        # Keep the primary key column as FK
                        if col == core_spec.primary_key_column:
                            fk_mappings[col] = (
                                f"{core_spec.table_name}.{core_spec.primary_key_column}"
                            )
                        # Exclude all other columns from the core table
                        else:
                            exclude_cols.append(col)

                if exclude_cols or fk_mappings:
                    references_found += 1
                    logger.info(
                        f"      🔗 {pattern_name}: exclude {exclude_cols}, FK {list(fk_mappings.keys())}"
                    )
                    dataset_name = files[0]["dataset"]

                    self.data_table_specs[dataset_name] = DataTableSpec(
                        table_name=dataset_name,
                        exclude_columns=exclude_cols,
                        foreign_key_mappings=fk_mappings,
                    )

            logger.info(
                f"   📈 Found {references_found} data tables referencing '{core_name}'"
            )

        logger.info(
            f"🔗 Generated {len(self.data_table_specs)} data table modification specs"
        )

    def _create_generation_rules(self) -> Dict:
        """Create rules for the existing generator to use"""
        logger.info("📝 Creating generation rules...")

        rules = {
            "core_pipeline_modules": list(self.core_table_specs.keys()),
            "excluded_from_regular_pipelines": list(self.core_table_specs.keys()),
            "primary_key_overrides": {
                spec.table_name: spec.primary_key_column
                for spec in self.core_table_specs.values()
                if spec.unifiable
            },
            "foreign_key_rules": {
                data_spec.table_name: data_spec.foreign_key_mappings
                for data_spec in self.data_table_specs.values()
            },
        }

        logger.info(
            f"📝 Rules: {len(rules['core_pipeline_modules'])} core modules, {len(rules['foreign_key_rules'])} FK rules"
        )
        return rules

    def save_specifications(
        self,
        output_path: str = "./analysis/pipeline_specs.json",
        specs: Optional[Dict] = None,
    ) -> None:
        """Save specifications to JSON file for use by generator"""
        logger.info(f"💾 Saving specifications to {output_path}...")

        # Ensure directory exists
        Path(output_path).parent.mkdir(exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(specs, f, indent=2)

        logger.info(f"✅ Pipeline specifications saved to {output_path}")
