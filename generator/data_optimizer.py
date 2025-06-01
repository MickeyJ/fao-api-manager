import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
import json
from io import StringIO
from collections import defaultdict, Counter
from dataclasses import dataclass
import time
from generator.scanner import Scanner
from generator.structure import Structure
from generator.file_generator import FileGenerator
from generator.csv_analyzer import CSVAnalyzer
from . import ZIP_PATH


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class OptimizationRecommendation:
    table_name: str
    action: str  # 'normalize', 'exclude_columns', 'merge_tables', 'split_table'
    details: Dict
    estimated_savings: str


class DataOptimizer:
    def __init__(self, zip_path: str, output_dir: str = "./analysis"):
        logger.info(f"🔧 Initializing DataOptimizer with zip_path: {zip_path}")
        self.zip_path = Path(zip_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        logger.info(f"📁 Output directory set to: {self.output_dir}")

        self.structure = Structure()
        self.scanner = Scanner(zip_path)
        self.file_generator = FileGenerator(output_dir)
        self.csv_analyzer = CSVAnalyzer(
            self.structure, self.scanner, self.file_generator
        )

        # Results storage
        self.area_codes_analysis = {}
        self.item_codes_analysis = {}
        self.column_redundancy_analysis = {}
        self.normalization_opportunities = []

        logger.info("✅ DataOptimizer initialization complete")

    def run_full_analysis(self) -> Dict:
        """Run comprehensive analysis and generate optimization recommendations"""
        start_time = time.time()
        logger.info("🔍 Starting comprehensive data analysis...")

        # Get all ZIP info
        logger.info("📥 Scanning all ZIP files...")
        scan_start = time.time()
        all_zip_info = self.scanner.scan_all_zips()
        scan_duration = time.time() - scan_start
        logger.info(
            f"📁 Found {len(all_zip_info)} ZIP files to analyze (scan took {scan_duration:.2f}s)"
        )

        # Run individual analyses
        logger.info("📍 Starting area codes unification analysis...")
        analysis_start = time.time()
        self.analyze_area_codes_unification(all_zip_info)
        logger.info(
            f"📍 Area codes analysis completed in {time.time() - analysis_start:.2f}s"
        )

        logger.info("📦 Starting item codes conflict analysis...")
        analysis_start = time.time()
        self.analyze_item_codes_conflicts(all_zip_info)
        logger.info(
            f"📦 Item codes analysis completed in {time.time() - analysis_start:.2f}s"
        )

        logger.info("📦 Starting elements codes conflict analysis...")
        analysis_start = time.time()
        self.analyze_elements_conflicts(all_zip_info)
        logger.info(
            f"📦 Elements codes analysis completed in {time.time() - analysis_start:.2f}s"
        )

        logger.info("📦 Starting flags codes conflict analysis...")
        analysis_start = time.time()
        self.analyze_flags_conflicts(all_zip_info)
        logger.info(
            f"📦 Flags codes analysis completed in {time.time() - analysis_start:.2f}s"
        )

        logger.info("🗂️ Starting column redundancy analysis...")
        analysis_start = time.time()
        self.analyze_column_redundancy(all_zip_info)
        logger.info(
            f"🗂️ Column redundancy analysis completed in {time.time() - analysis_start:.2f}s"
        )

        logger.info("🔄 Starting normalization opportunities analysis...")
        analysis_start = time.time()
        self.analyze_normalization_opportunities(all_zip_info)
        logger.info(
            f"🔄 Normalization analysis completed in {time.time() - analysis_start:.2f}s"
        )

        # Generate recommendations
        logger.info("💡 Generating optimization recommendations...")
        rec_start = time.time()
        recommendations = self.generate_recommendations()
        logger.info(f"💡 Recommendations generated in {time.time() - rec_start:.2f}s")

        # Save comprehensive report
        logger.info("📊 Saving analysis report...")
        save_start = time.time()
        self.save_analysis_report(recommendations)
        logger.info(f"📊 Report saved in {time.time() - save_start:.2f}s")

        total_duration = time.time() - start_time
        logger.info(
            f"✅ Comprehensive data analysis completed successfully in {total_duration:.2f}s!"
        )

        return {
            "area_codes": self.area_codes_analysis,
            "item_codes": self.item_codes_analysis,
            "element_codes": self.element_codes_analysis,
            "flags": self.flags_analysis,
            "column_redundancy": self.column_redundancy_analysis,
            "recommendations": [r.__dict__ for r in recommendations],
        }

    def analyze_elements_conflicts(self, all_zip_info: List[Dict]):
        """Check for element code conflicts across different domains"""
        logger.info("📊 Analyzing element codes for conflicts...")

        element_code_domains = defaultdict(dict)  # domain -> {code: element_name}
        domain_overlaps = defaultdict(set)
        total_files_processed = 0

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            dataset_name = zip_info["pipeline_name"]
            logger.info(
                f"🔍 Processing ZIP {zip_idx}/{len(all_zip_info)}: {dataset_name}"
            )

            for csv_file in zip_info["csv_files"]:
                if "elements" in csv_file.lower():
                    total_files_processed += 1
                    logger.debug(f"📄 Analyzing elements file: {csv_file}")
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        if csv_analysis["row_count"] > 0 and csv_analysis.get(
                            "sample_rows"
                        ):
                            logger.debug(
                                f"  📊 Processing {csv_analysis['row_count']} rows"
                            )
                            for row in csv_analysis["sample_rows"]:
                                if "Element Code" in row and "Element" in row:
                                    element_code = row["Element Code"]
                                    element_name = row["Element"]

                                    # Categorize by domain (reusing your item domain logic)
                                    domain = self.categorize_element_domain(
                                        dataset_name, element_name
                                    )

                                    if element_code in element_code_domains[domain]:
                                        # Check for conflicts
                                        existing_name = element_code_domains[domain][
                                            element_code
                                        ]
                                        if existing_name != element_name:
                                            domain_overlaps[element_code].add(
                                                (domain, existing_name)
                                            )
                                            domain_overlaps[element_code].add(
                                                (domain, element_name)
                                            )
                                    else:
                                        element_code_domains[domain][
                                            element_code
                                        ] = element_name

                    except Exception as e:
                        logger.warning(f"⚠️  Error analyzing {csv_file}: {e}")

        # Analyze conflicts
        conflicts = {}
        for element_code, domain_elements in domain_overlaps.items():
            if len(domain_elements) > 1:
                conflicts[element_code] = list(domain_elements)

        self.element_codes_analysis = {
            "domains_found": list(element_code_domains.keys()),
            "elements_per_domain": {
                domain: len(elements)
                for domain, elements in element_code_domains.items()
            },
            "conflicts": conflicts,
            "unification_feasible": len(conflicts) == 0,
            "recommendation": (
                "unified_table" if len(conflicts) == 0 else "domain_specific_tables"
            ),
        }

        logger.info(
            f"✅ Analyzed {len(element_code_domains)} element domains from {total_files_processed} files"
        )
        if conflicts:
            logger.warning(
                f"⚠️  Found {len(conflicts)} element code conflicts across domains"
            )
        else:
            logger.info("✅ No element code conflicts found - unification is safe!")

    def analyze_flags_conflicts(self, all_zip_info: List[Dict]):
        """Check for flag conflicts across different domains"""
        logger.info("🏁 Analyzing flags for conflicts...")

        flag_descriptions = defaultdict(set)  # flag -> set of descriptions
        flag_conflicts = defaultdict(set)
        total_files_processed = 0

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            logger.info(
                f"🔍 Processing ZIP {zip_idx}/{len(all_zip_info)}: {zip_info['pipeline_name']}"
            )

            for csv_file in zip_info["csv_files"]:
                if "flags" in csv_file.lower():
                    total_files_processed += 1
                    logger.debug(f"📄 Analyzing flags file: {csv_file}")
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        if csv_analysis["row_count"] > 0 and csv_analysis.get(
                            "sample_rows"
                        ):
                            logger.debug(
                                f"  📊 Processing {csv_analysis['row_count']} rows"
                            )
                            for row in csv_analysis["sample_rows"]:
                                if "Flag" in row and "Description" in row:
                                    flag = row["Flag"]
                                    description = row["Description"]

                                    flag_descriptions[flag].add(description)

                    except Exception as e:
                        logger.warning(f"⚠️  Error analyzing {csv_file}: {e}")

        # Check for conflicts
        conflicts = {}
        for flag, descriptions in flag_descriptions.items():
            if len(descriptions) > 1:
                conflicts[flag] = list(descriptions)

        self.flags_analysis = {
            "total_unique_flags": len(flag_descriptions),
            "flags_with_conflicts": len(conflicts),
            "conflicts": conflicts,
            "unification_feasible": len(conflicts) == 0,
            "recommendation": (
                "unified_table" if len(conflicts) == 0 else "investigate_conflicts"
            ),
        }

        logger.info(
            f"✅ Analyzed {len(flag_descriptions)} unique flags from {total_files_processed} files"
        )
        if conflicts:
            logger.warning(
                f"⚠️  Found {len(conflicts)} flags with conflicting descriptions"
            )
            for flag, descriptions in list(conflicts.items())[:3]:  # Show first 3
                logger.debug(f"  Flag '{flag}': {descriptions}")
        else:
            logger.info("✅ No flag conflicts found - unification is safe!")

    def categorize_element_domain(self, dataset_name: str, element_name: str) -> str:
        """Categorize elements into domains based on dataset and element characteristics"""
        dataset_lower = dataset_name.lower()
        element_lower = element_name.lower()

        if "emission" in dataset_lower or any(
            x in element_lower for x in ["emissions", "co2", "ch4", "n2o"]
        ):
            return "emissions"
        elif "production" in dataset_lower or any(
            x in element_lower for x in ["production", "yield", "area harvested"]
        ):
            return "production"
        elif "price" in dataset_lower or any(
            x in element_lower for x in ["price", "value", "cost"]
        ):
            return "economic"
        elif "trade" in dataset_lower or any(
            x in element_lower for x in ["import", "export", "trade"]
        ):
            return "trade"
        elif any(x in element_lower for x in ["population", "employment", "rural"]):
            return "demographic"
        else:
            return "other"

    def analyze_area_codes_unification(self, all_zip_info: List[Dict]):
        """Analyze area codes across all datasets to plan unification"""
        logger.info("📍 Analyzing area codes for unification...")

        all_area_codes = set()
        area_code_sources = defaultdict(list)
        area_name_variations = defaultdict(set)
        total_files_processed = 0

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            logger.info(
                f"🔍 Processing ZIP {zip_idx}/{len(all_zip_info)}: {zip_info['pipeline_name']}"
            )

            for csv_file in zip_info["csv_files"]:
                if "areacodes" in csv_file.lower():
                    total_files_processed += 1
                    logger.debug(f"📄 Analyzing area codes file: {csv_file}")
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        # Track which datasets have area codes
                        dataset_name = zip_info["pipeline_name"]
                        area_code_sources[dataset_name].append(
                            {"file": csv_file, "row_count": csv_analysis["row_count"]}
                        )

                        # If we have sample data, analyze the area codes
                        if csv_analysis.get("sample_rows"):
                            logger.debug(
                                f"  📊 Processing {csv_analysis['row_count']} rows"
                            )
                            for row in csv_analysis["sample_rows"]:
                                if "Area Code" in row and "Area" in row:
                                    area_code = row["Area Code"]
                                    area_name = row["Area"]
                                    all_area_codes.add(area_code)
                                    area_name_variations[area_code].add(area_name)

                    except Exception as e:
                        logger.warning(f"⚠️  Error analyzing {csv_file}: {e}")

        # Identify inconsistencies
        inconsistent_names = {
            code: names
            for code, names in area_name_variations.items()
            if len(names) > 1
        }

        self.area_codes_analysis = {
            "total_unique_codes": len(all_area_codes),
            "datasets_with_area_codes": len(area_code_sources),
            "area_code_sources": dict(area_code_sources),
            "name_inconsistencies": inconsistent_names,
            "unification_feasible": len(inconsistent_names) < 10,  # Arbitrary threshold
        }

        logger.info(
            f"✅ Found {len(all_area_codes)} unique area codes across {len(area_code_sources)} datasets from {total_files_processed} files"
        )
        if inconsistent_names:
            logger.warning(
                f"⚠️  Found {len(inconsistent_names)} area codes with name variations"
            )
        else:
            logger.info("✅ No area code conflicts found - unification is safe!")

    def analyze_item_codes_conflicts(self, all_zip_info: List[Dict]):
        """Check for item code conflicts across different domains"""
        logger.info("📦 Analyzing item codes for conflicts...")

        item_code_domains = defaultdict(dict)  # domain -> {code: item_name}
        domain_overlaps = defaultdict(set)
        total_files_processed = 0

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            logger.info(
                f"🔍 Processing ZIP {zip_idx}/{len(all_zip_info)}: {zip_info['pipeline_name']}"
            )
            dataset_name = zip_info["pipeline_name"]

            for csv_file in zip_info["csv_files"]:
                if "itemcodes" in csv_file.lower():
                    total_files_processed += 1
                    logger.debug(f"📄 Analyzing item codes file: {csv_file}")
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        if csv_analysis["row_count"] > 0 and csv_analysis.get(
                            "sample_rows"
                        ):
                            logger.debug(
                                f"  📊 Processing {csv_analysis['row_count']} rows"
                            )
                            for row in csv_analysis["sample_rows"]:
                                if "Item Code" in row and "Item" in row:
                                    item_code = row["Item Code"]
                                    item_name = row["Item"]

                                    # Categorize by domain
                                    domain = self.categorize_item_domain(
                                        dataset_name, item_name
                                    )

                                    if item_code in item_code_domains[domain]:
                                        # Check for conflicts
                                        existing_name = item_code_domains[domain][
                                            item_code
                                        ]
                                        if existing_name != item_name:
                                            domain_overlaps[item_code].add(
                                                (domain, existing_name)
                                            )
                                            domain_overlaps[item_code].add(
                                                (domain, item_name)
                                            )
                                    else:
                                        item_code_domains[domain][item_code] = item_name

                    except Exception as e:
                        logger.warning(f"⚠️  Error analyzing {csv_file}: {e}")

        # Analyze conflicts
        conflicts = {}
        for item_code, domain_items in domain_overlaps.items():
            if len(domain_items) > 1:
                conflicts[item_code] = list(domain_items)

        self.item_codes_analysis = {
            "domains_found": list(item_code_domains.keys()),
            "items_per_domain": {
                domain: len(items) for domain, items in item_code_domains.items()
            },
            "conflicts": conflicts,
            "unification_feasible": len(conflicts) == 0,
            "recommendation": (
                "unified_table" if len(conflicts) == 0 else "domain_specific_tables"
            ),
        }

        logger.info(
            f"✅ Analyzed {len(item_code_domains)} item domains from {total_files_processed} files"
        )
        if conflicts:
            logger.warning(
                f"⚠️  Found {len(conflicts)} item code conflicts across domains"
            )
        else:
            logger.info("✅ No item code conflicts found - unification is safe!")

    def categorize_item_domain(self, dataset_name: str, item_name: str) -> str:
        """Categorize items into domains based on dataset and item characteristics"""
        dataset_lower = dataset_name.lower()
        item_lower = item_name.lower()

        if "fertilizer" in dataset_lower or any(
            x in item_lower for x in ["urea", "phosphate", "nitrogen", "potash"]
        ):
            return "fertilizers"
        elif "emission" in dataset_lower:
            return "emissions"
        elif (
            "production" in dataset_lower
            or "crops" in dataset_lower
            or "livestock" in dataset_lower
        ):
            return "agriculture"
        elif "price" in dataset_lower:
            return "prices"
        elif "forestry" in dataset_lower:
            return "forestry"
        else:
            return "other"

    def analyze_column_redundancy(self, all_zip_info: List[Dict]):
        """Identify redundant columns that can be normalized"""
        logger.info("🗂️  Analyzing column redundancy...")

        redundant_patterns = []
        common_columns = defaultdict(list)
        total_files_processed = 0

        for zip_idx, zip_info in enumerate(all_zip_info, 1):
            dataset_name = zip_info["pipeline_name"]
            self._log_progress(
                zip_idx, len(all_zip_info), "Column redundancy analysis", dataset_name
            )

            for csv_file in zip_info["csv_files"]:
                if not any(
                    x in csv_file.lower()
                    for x in ["areacodes", "itemcodes", "flags", "elements"]
                ):
                    total_files_processed += 1
                    logger.debug(f"📄 Analyzing data file: {csv_file}")
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        columns = csv_analysis.get("columns", [])
                        logger.debug(f"  📊 Found {len(columns)} columns")

                        # Track common column patterns
                        for col in columns:
                            col_lower = col.lower()
                            if any(
                                pattern in col_lower
                                for pattern in [
                                    "area code",
                                    "area",
                                    "item code",
                                    "item",
                                    "element code",
                                    "element",
                                    "flag",
                                ]
                            ):
                                common_columns[col_lower].append(
                                    {
                                        "dataset": dataset_name,
                                        "file": csv_file,
                                        "column": col,
                                    }
                                )

                    except Exception as e:
                        logger.warning(f"⚠️  Error analyzing {csv_file}: {e}")
                        continue

        # Identify normalization opportunities
        for col_pattern, occurrences in common_columns.items():
            if len(occurrences) > 5:  # Appears in many files
                redundant_patterns.append(
                    {
                        "column_pattern": col_pattern,
                        "occurrences": len(occurrences),
                        "can_normalize": any(
                            x in col_pattern
                            for x in ["area code", "item code", "element code"]
                        ),
                        "files": occurrences[:5],  # Sample
                    }
                )

        self.column_redundancy_analysis = {
            "redundant_patterns": redundant_patterns,
            "total_patterns": len(redundant_patterns),
        }

        logger.info(
            f"✅ Found {len(redundant_patterns)} redundant column patterns from {total_files_processed} data files"
        )

    def analyze_normalization_opportunities(self, all_zip_info: List[Dict]):
        """Identify specific normalization opportunities"""
        logger.info("🔄 Analyzing normalization opportunities...")

        opportunities = []

        # Area normalization
        if self.area_codes_analysis.get("unification_feasible", False):
            logger.debug("✅ Area normalization opportunity identified")
            opportunities.append(
                {
                    "type": "area_normalization",
                    "description": "Create unified areas table, remove area names from data tables",
                    "affected_tables": len(
                        self.area_codes_analysis.get("area_code_sources", {})
                    ),
                    "estimated_savings": "30-50% reduction in string storage",
                }
            )

        # Item normalization
        if self.item_codes_analysis.get("unification_feasible", False):
            logger.debug("✅ Item normalization opportunity identified")
            opportunities.append(
                {
                    "type": "item_normalization",
                    "description": "Create unified items table, remove item names from data tables",
                    "affected_tables": len(
                        self.item_codes_analysis.get("domains_found", [])
                    ),
                    "estimated_savings": "20-40% reduction in string storage",
                }
            )

        # Flag/Element normalization
        logger.debug("💡 Lookup tables opportunity identified")
        opportunities.append(
            {
                "type": "lookup_tables",
                "description": "Create lookup tables for flags, elements, sources",
                "affected_tables": "Most data tables",
                "estimated_savings": "10-20% reduction in repeated strings",
            }
        )

        self.normalization_opportunities = opportunities
        logger.info(f"✅ Identified {len(opportunities)} normalization opportunities")

    def generate_recommendations(self) -> List[OptimizationRecommendation]:
        """Generate actionable optimization recommendations"""
        logger.info("💡 Generating optimization recommendations...")
        recommendations = []

        # Area code recommendations
        if self.area_codes_analysis.get("unification_feasible"):
            logger.debug("📍 Adding area unification recommendation")
            recommendations.append(
                OptimizationRecommendation(
                    table_name="areas",
                    action="create_unified_table",
                    details={
                        "source_files": list(
                            self.area_codes_analysis["area_code_sources"].keys()
                        ),
                        "columns_to_exclude": [
                            "Area",
                            "Area Code (M49)",
                        ],  # Keep area_code as PK
                        "foreign_key_setup": "Replace area names with area_code FK in all data tables",
                    },
                    estimated_savings="30-50% storage reduction in area references",
                )
            )

        # Item code recommendations
        if self.item_codes_analysis.get("unification_feasible"):
            logger.debug("📦 Adding item unification recommendation")
            recommendations.append(
                OptimizationRecommendation(
                    table_name="items",
                    action="create_unified_table",
                    details={
                        "strategy": "unified",
                        "columns_to_exclude": [
                            "Item",
                            "Item Code (CPC)",
                        ],  # Keep item_code as PK
                        "foreign_key_setup": "Replace item names with item_code FK in all data tables",
                    },
                    estimated_savings="20-40% storage reduction in item references",
                )
            )
        else:
            logger.debug("📦 Adding domain-specific item tables recommendation")
            recommendations.append(
                OptimizationRecommendation(
                    table_name="items",
                    action="create_domain_specific_tables",
                    details={
                        "strategy": "domain_specific",
                        "domains": self.item_codes_analysis.get("domains_found", []),
                        "conflicts": self.item_codes_analysis.get("conflicts", {}),
                    },
                    estimated_savings="15-25% storage reduction with domain separation",
                )
            )

        # Element code recommendations - ADD THIS SECTION
        if self.element_codes_analysis.get("unification_feasible"):
            # Count how many datasets actually have elements files (this is the real table count)
            datasets_with_elements = len(
                self.element_codes_analysis.get("domains_found", [])
            )
            logger.debug(
                f"📊 Adding element unification recommendation (saves {datasets_with_elements-1} tables)"
            )
            recommendations.append(
                OptimizationRecommendation(
                    table_name="elements",
                    action="create_unified_table",
                    details={
                        "strategy": "unified",
                        "domains_unified": self.element_codes_analysis.get(
                            "domains_found", []
                        ),
                        "tables_eliminated": datasets_with_elements
                        - 1,  # N tables become 1
                        "foreign_key_setup": "Replace element names with element_code FK in all data tables",
                    },
                    estimated_savings=f"Eliminates {datasets_with_elements - 1} separate element tables, 15-25% storage reduction in element references",
                )
            )
        else:
            conflicts_count = len(self.element_codes_analysis.get("conflicts", {}))
            logger.debug(
                f"📊 Adding domain-specific element tables recommendation ({conflicts_count} conflicts)"
            )
            recommendations.append(
                OptimizationRecommendation(
                    table_name="elements",
                    action="create_domain_specific_tables",
                    details={
                        "strategy": "domain_specific",
                        "domains": self.element_codes_analysis.get("domains_found", []),
                        "conflicts": self.element_codes_analysis.get("conflicts", {}),
                        "tables_saved": "Moderate - some consolidation possible",
                    },
                    estimated_savings="10-15% storage reduction with domain separation",
                )
            )

        # Flags recommendations - ADD THIS SECTION
        if self.flags_analysis.get("unification_feasible"):
            total_flag_tables = 68  # From your analysis - 68 flag files
            logger.debug(
                f"🏁 Adding flags unification recommendation (saves {total_flag_tables-1} tables)"
            )
            recommendations.append(
                OptimizationRecommendation(
                    table_name="flags",
                    action="create_unified_table",
                    details={
                        "strategy": "unified",
                        "total_unique_flags": self.flags_analysis.get(
                            "total_unique_flags", 0
                        ),
                        "tables_eliminated": total_flag_tables
                        - 1,  # 68 tables become 1
                        "foreign_key_setup": "Replace flag descriptions with flag FK in all data tables",
                    },
                    estimated_savings=f"Eliminates {total_flag_tables - 1} separate flag tables, 10-15% storage reduction in flag references",
                )
            )
        else:
            conflicts_count = len(self.flags_analysis.get("conflicts", {}))
            logger.debug(
                f"🏁 Adding flags conflict investigation recommendation ({conflicts_count} conflicts)"
            )
            recommendations.append(
                OptimizationRecommendation(
                    table_name="flags",
                    action="investigate_conflicts",
                    details={
                        "strategy": "investigate_first",
                        "conflicts": self.flags_analysis.get("conflicts", {}),
                        "total_conflicts": conflicts_count,
                        "next_steps": "Resolve flag description conflicts before unification",
                    },
                    estimated_savings="Potential for major table reduction after conflict resolution",
                )
            )

        # Time dimension optimization
        logger.debug("⏰ Adding time dimension standardization recommendation")
        recommendations.append(
            OptimizationRecommendation(
                table_name="time_dimension",
                action="standardize_time_handling",
                details={
                    "issues": [
                        "Redundant Year Code/Year columns",
                        "Mixed monthly/annual data",
                    ],
                    "solution": "Create date dimension table, standardize to proper dates",
                    "affected_columns": ["Year Code", "Year", "Months Code", "Months"],
                },
                estimated_savings="10-15% storage reduction + better query performance",
            )
        )

        # Column exclusion recommendations
        redundant_columns = []
        for pattern in self.column_redundancy_analysis.get("redundant_patterns", []):
            if pattern["can_normalize"]:
                redundant_columns.append(pattern["column_pattern"])

        if redundant_columns:
            logger.debug(
                f"🗂️ Adding column exclusion recommendation for {len(redundant_columns)} patterns"
            )
            recommendations.append(
                OptimizationRecommendation(
                    table_name="all_data_tables",
                    action="exclude_redundant_columns",
                    details={
                        "columns_to_exclude": redundant_columns,
                        "replace_with": "Foreign key references to lookup tables",
                    },
                    estimated_savings="20-35% storage reduction in repeated data",
                )
            )

        logger.info(f"✅ Generated {len(recommendations)} optimization recommendations")
        return recommendations

    def save_analysis_report(self, recommendations: List[OptimizationRecommendation]):
        """Save comprehensive analysis report"""
        logger.info("📊 Saving comprehensive analysis report...")

        report = {
            "analysis_summary": {
                "area_codes": self.area_codes_analysis,
                "item_codes": self.item_codes_analysis,
                "element_codes": self.element_codes_analysis,
                "flags": self.flags_analysis,
                "column_redundancy": self.column_redundancy_analysis,
                "normalization_opportunities": self.normalization_opportunities,
            },
            "recommendations": [r.__dict__ for r in recommendations],
            "implementation_priority": self.prioritize_recommendations(recommendations),
        }

        report_path = self.output_dir / "optimization_analysis.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"📊 Comprehensive analysis saved to {report_path}")

        # Also create a summary markdown report
        logger.info("📝 Creating markdown summary report...")
        self.create_markdown_summary(recommendations)

    def prioritize_recommendations(
        self, recommendations: List[OptimizationRecommendation]
    ) -> List[str]:
        """Prioritize recommendations by impact and implementation ease"""
        priority_order = []

        # High impact, easy implementation first
        easy_wins = [
            r for r in recommendations if "lookup" in r.action or "exclude" in r.action
        ]
        medium_effort = [r for r in recommendations if "unified" in r.action]
        complex_changes = [r for r in recommendations if "standardize" in r.action]

        for category, label in [
            (easy_wins, "Quick Wins"),
            (medium_effort, "Medium Effort"),
            (complex_changes, "Complex Changes"),
        ]:
            if category:
                priority_order.append(
                    f"{label}: {', '.join([r.table_name for r in category])}"
                )

        return priority_order

    def create_markdown_summary(
        self, recommendations: List[OptimizationRecommendation]
    ):
        """Create a readable markdown summary"""
        logger.debug("📝 Generating markdown summary...")

        md_content = f"""# FAO Data Optimization Analysis

## Executive Summary
- **Area Codes**: {self.area_codes_analysis.get('total_unique_codes', 'N/A')} unique codes across {self.area_codes_analysis.get('datasets_with_area_codes', 'N/A')} datasets
- **Item Codes**: {self.item_codes_analysis.get('recommendation', 'Analysis pending')} recommended
- **Normalization Opportunities**: {len(self.normalization_opportunities)} identified

## Key Findings

### Area Codes Unification
- **Feasible**: {self.area_codes_analysis.get('unification_feasible', False)}
- **Conflicts**: {len(self.area_codes_analysis.get('name_inconsistencies', {}))} area codes with name variations
- **Storage Impact**: Can eliminate repeated area names across all data tables

### Item Codes Strategy  
- **Recommendation**: {self.item_codes_analysis.get('recommendation', 'TBD')}
- **Conflicts Found**: {len(self.item_codes_analysis.get('conflicts', {}))} item codes with cross-domain conflicts
- **Domains**: {', '.join(self.item_codes_analysis.get('domains_found', []))}

### Element Codes Strategy
- **Recommendation**: {self.element_codes_analysis.get('recommendation', 'TBD')}
- **Conflicts Found**: {len(self.element_codes_analysis.get('conflicts', {}))} element codes with cross-domain conflicts
- **Domains**: {', '.join(self.element_codes_analysis.get('domains_found', []))}

### Flags Strategy
- **Recommendation**: {self.flags_analysis.get('recommendation', 'TBD')}
- **Conflicts Found**: {len(self.flags_analysis.get('conflicts', {}))} flags with conflicting descriptions
- **Total Unique Flags**: {self.flags_analysis.get('total_unique_flags', 'N/A')}

## Optimization Recommendations

"""

        for i, rec in enumerate(recommendations, 1):
            md_content += f"""### {i}. {rec.table_name.title()} - {rec.action.replace('_', ' ').title()}
**Expected Savings**: {rec.estimated_savings}

{rec.details}

---

"""

        md_path = self.output_dir / "optimization_summary.md"
        with open(md_path, "w") as f:
            f.write(md_content)

        logger.info(f"📝 Summary report saved to {md_path}")

    def _log_progress(
        self, current: int, total: int, operation: str, extra_info: str = ""
    ):
        """Log progress for long-running operations"""
        if total > 10:  # Only log progress for operations with many items
            progress_percent = (current / total) * 100
            if (
                current % max(1, total // 10) == 0 or current == total
            ):  # Log every 10% or at completion
                extra = f" - {extra_info}" if extra_info else ""
                logger.info(
                    f"📈 {operation}: {current}/{total} ({progress_percent:.1f}%){extra}"
                )


class OptimizedPipelineGenerator:
    """Enhanced pipeline generator that applies optimization recommendations"""

    def __init__(self, optimization_results: Dict):
        self.optimization_results = optimization_results
        self.recommendations = optimization_results.get("recommendations", [])

    def generate_optimized_models(self, csv_analysis: Dict, table_name: str) -> str:
        """Generate optimized SQLAlchemy models based on recommendations"""

        # Get relevant recommendations for this table
        area_unified = self.should_use_area_fk()
        item_unified = self.should_use_item_fk()
        exclude_columns = self.get_columns_to_exclude()

        # Build optimized column list
        optimized_columns = []
        foreign_keys = []

        for col_info in csv_analysis["column_analysis"]:
            col_name = col_info["sql_column_name"]

            # Skip excluded columns
            if col_name in exclude_columns:
                continue

            # Replace with foreign keys where appropriate
            if area_unified and col_name == "area_code":
                foreign_keys.append("areas(area_code)")
                optimized_columns.append(
                    {
                        "name": "area_id",
                        "type": "Integer",
                        "nullable": False,
                        "foreign_key": "areas.area_code",
                    }
                )
            elif item_unified and col_name == "item_code":
                foreign_keys.append("items(item_code)")
                optimized_columns.append(
                    {
                        "name": "item_id",
                        "type": "Integer",
                        "nullable": False,
                        "foreign_key": "items.item_code",
                    }
                )
            else:
                optimized_columns.append(
                    {
                        "name": col_name,
                        "type": col_info["inferred_sql_type"],
                        "nullable": col_info["null_count"] > 0,
                    }
                )

        return self.render_optimized_model(table_name, optimized_columns, foreign_keys)

    def should_use_area_fk(self) -> bool:
        """Check if areas should be normalized"""
        for rec in self.recommendations:
            if (
                rec.get("table_name") == "areas"
                and rec.get("action") == "create_unified_table"
            ):
                return True
        return False

    def should_use_item_fk(self) -> bool:
        """Check if items should be normalized"""
        for rec in self.recommendations:
            if rec.get("table_name") == "items" and "unified" in rec.get("action", ""):
                return True
        return False

    def get_columns_to_exclude(self) -> Set[str]:
        """Get columns that should be excluded due to normalization"""
        exclude_columns = set()

        for rec in self.recommendations:
            details = rec.get("details", {})
            if "columns_to_exclude" in details:
                exclude_columns.update(details["columns_to_exclude"])

        return exclude_columns

    def render_optimized_model(
        self, table_name: str, columns: List[Dict], foreign_keys: List[str]
    ) -> str:
        """Render optimized SQLAlchemy model"""

        model_name = "".join(word.capitalize() for word in table_name.split("_"))

        imports = ["Integer", "DateTime", "Column", "func"]
        if foreign_keys:
            imports.append("ForeignKey")

        # Collect unique types
        column_types = set(col["type"] for col in columns if col["type"] != "Integer")
        imports.extend(column_types)

        model_content = f"""from sqlalchemy import (
    {", ".join(sorted(imports))}
)
from db.database import Base

class {model_name}(Base):
    __tablename__ = "{table_name}"
    
    id = Column(Integer, primary_key=True)
"""

        # Add optimized columns
        for col in columns:
            nullable_str = "" if not col["nullable"] else ", nullable=True"
            fk_str = ""

            if col.get("foreign_key"):
                fk_str = f', ForeignKey("{col["foreign_key"]}")'

            model_content += (
                f'    {col["name"]} = Column({col["type"]}{fk_str}{nullable_str})\n'
            )

        # Add timestamps
        model_content += """
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<{model_name}(id={self.id})>"
"""

        return model_content


# Usage example and test functions
def run_optimization_analysis(zip_path: str):
    """Main function to run the optimization analysis"""
    logger.info(f"🚀 Starting optimization analysis for: {zip_path}")

    optimizer = DataOptimizer(zip_path)
    results = optimizer.run_full_analysis()

    logger.info("🎯 OPTIMIZATION ANALYSIS COMPLETE")
    logger.info("=" * 50)

    # Print key insights
    area_analysis = results["area_codes"]
    logger.info(
        f"📍 Areas: {area_analysis.get('total_unique_codes')} codes, unification {'✅ feasible' if area_analysis.get('unification_feasible') else '❌ has conflicts'}"
    )

    item_analysis = results["item_codes"]
    logger.info(
        f"📦 Items: {item_analysis.get('recommendation', 'TBD')} strategy recommended"
    )

    logger.info(
        f"💡 Recommendations: {len(results['recommendations'])} optimization opportunities identified"
    )
    logger.info("See ./analysis/ directory for detailed reports!")

    return results


if __name__ == "__main__":
    # Test with your ZIP path
    ZIP_PATH = r"C:\Users\18057\Documents\Data\fao-test-zips"
    results = run_optimization_analysis(ZIP_PATH)
