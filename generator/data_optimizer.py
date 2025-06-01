import zipfile
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
import json
from io import StringIO
from collections import defaultdict, Counter
from dataclasses import dataclass
from generator.scanner import Scanner
from generator.structure import Structure
from . import ZIP_PATH


@dataclass
class OptimizationRecommendation:
    table_name: str
    action: str  # 'normalize', 'exclude_columns', 'merge_tables', 'split_table'
    details: Dict
    estimated_savings: str


class DataOptimizer:
    def __init__(self, zip_path: str, output_dir: str = "./analysis"):
        self.zip_path = Path(zip_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.scanner = Scanner(zip_path)
        self.structure = Structure()
        self.csv_analyzer = CSVAnalyzer()

        # Results storage
        self.area_codes_analysis = {}
        self.item_codes_analysis = {}
        self.column_redundancy_analysis = {}
        self.normalization_opportunities = []

    def run_full_analysis(self) -> Dict:
        """Run comprehensive analysis and generate optimization recommendations"""
        print("🔍 Starting comprehensive data analysis...")

        # Get all ZIP info
        all_zip_info = self.scanner.scan_all_zips()

        # Run individual analyses
        self.analyze_area_codes_unification(all_zip_info)
        self.analyze_item_codes_conflicts(all_zip_info)
        self.analyze_column_redundancy(all_zip_info)
        self.analyze_normalization_opportunities(all_zip_info)

        # Generate recommendations
        recommendations = self.generate_recommendations()

        # Save comprehensive report
        self.save_analysis_report(recommendations)

        return {
            "area_codes": self.area_codes_analysis,
            "item_codes": self.item_codes_analysis,
            "column_redundancy": self.column_redundancy_analysis,
            "recommendations": [r.__dict__ for r in recommendations],
        }

    def analyze_area_codes_unification(self, all_zip_info: List[Dict]):
        """Analyze area codes across all datasets to plan unification"""
        print("📍 Analyzing area codes for unification...")

        all_area_codes = set()
        area_code_sources = defaultdict(list)
        area_name_variations = defaultdict(set)

        for zip_info in all_zip_info:
            for csv_file in zip_info["csv_files"]:
                if "areacodes" in csv_file.lower():
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
                            for row in csv_analysis["sample_rows"]:
                                if "Area Code" in row and "Area" in row:
                                    area_code = row["Area Code"]
                                    area_name = row["Area"]
                                    all_area_codes.add(area_code)
                                    area_name_variations[area_code].add(area_name)

                    except Exception as e:
                        print(f"⚠️  Error analyzing {csv_file}: {e}")

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

        print(
            f"✅ Found {len(all_area_codes)} unique area codes across {len(area_code_sources)} datasets"
        )
        if inconsistent_names:
            print(f"⚠️  Found {len(inconsistent_names)} area codes with name variations")

    def analyze_item_codes_conflicts(self, all_zip_info: List[Dict]):
        """Check for item code conflicts across different domains"""
        print("📦 Analyzing item codes for conflicts...")

        item_code_domains = defaultdict(dict)  # domain -> {code: item_name}
        domain_overlaps = defaultdict(set)

        for zip_info in all_zip_info:
            print(f"Analyzing ZIP: {zip_info}")
            dataset_name = zip_info["pipeline_name"]

            for csv_file in zip_info["csv_files"]:
                if "itemcodes" in csv_file.lower():
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        if csv_analysis["row_count"] > 0 and csv_analysis.get(
                            "sample_rows"
                        ):
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
                        print(f"⚠️  Error analyzing {csv_file}: {e}")

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

        print(f"✅ Analyzed {len(item_code_domains)} item domains")
        if conflicts:
            print(f"⚠️  Found {len(conflicts)} item code conflicts across domains")
        else:
            print("✅ No item code conflicts found - unification is safe!")

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
        print("🗂️  Analyzing column redundancy...")

        redundant_patterns = []
        common_columns = defaultdict(list)

        for zip_info in all_zip_info:
            dataset_name = zip_info["pipeline_name"]
            print(f"Analyzing ZIP for dataset: {dataset_name}")

            for csv_file in zip_info["csv_files"]:
                if not any(
                    x in csv_file.lower()
                    for x in ["areacodes", "itemcodes", "flags", "elements"]
                ):
                    try:
                        csv_analysis = self.csv_analyzer.analyze_csv_from_zip(
                            zip_info["zip_path"], csv_file
                        )

                        columns = csv_analysis.get("columns", [])
                        print(f"columns: {columns}")

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
                        print(f"⚠️  Error analyzing {csv_file}: {e}")
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

        print(f"✅ Found {len(redundant_patterns)} redundant column patterns")

    def analyze_normalization_opportunities(self, all_zip_info: List[Dict]):
        """Identify specific normalization opportunities"""
        print("🔄 Analyzing normalization opportunities...")

        opportunities = []

        # Area normalization
        if self.area_codes_analysis.get("unification_feasible", False):
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
        opportunities.append(
            {
                "type": "lookup_tables",
                "description": "Create lookup tables for flags, elements, sources",
                "affected_tables": "Most data tables",
                "estimated_savings": "10-20% reduction in repeated strings",
            }
        )

        self.normalization_opportunities = opportunities
        print(f"✅ Identified {len(opportunities)} normalization opportunities")

    def generate_recommendations(self) -> List[OptimizationRecommendation]:
        """Generate actionable optimization recommendations"""
        recommendations = []

        # Area code recommendations
        if self.area_codes_analysis.get("unification_feasible"):
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

        # Time dimension optimization
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

        return recommendations

    def save_analysis_report(self, recommendations: List[OptimizationRecommendation]):
        """Save comprehensive analysis report"""
        report = {
            "analysis_summary": {
                "area_codes": self.area_codes_analysis,
                "item_codes": self.item_codes_analysis,
                "column_redundancy": self.column_redundancy_analysis,
                "normalization_opportunities": self.normalization_opportunities,
            },
            "recommendations": [r.__dict__ for r in recommendations],
            "implementation_priority": self.prioritize_recommendations(recommendations),
        }

        report_path = self.output_dir / "optimization_analysis.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        print(f"📊 Comprehensive analysis saved to {report_path}")

        # Also create a summary markdown report
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

        print(f"📝 Summary report saved to {md_path}")


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

    optimizer = DataOptimizer(zip_path)
    results = optimizer.run_full_analysis()

    print("\n" + "=" * 50)
    print("🎯 OPTIMIZATION ANALYSIS COMPLETE")
    print("=" * 50)

    # Print key insights
    area_analysis = results["area_codes"]
    print(
        f"📍 Areas: {area_analysis.get('total_unique_codes')} codes, unification {'✅ feasible' if area_analysis.get('unification_feasible') else '❌ has conflicts'}"
    )

    item_analysis = results["item_codes"]
    print(
        f"📦 Items: {item_analysis.get('recommendation', 'TBD')} strategy recommended"
    )

    print(
        f"💡 Recommendations: {len(results['recommendations'])} optimization opportunities identified"
    )
    print("\nSee ./analysis/ directory for detailed reports!")

    return results


if __name__ == "__main__":
    # Test with your ZIP path
    ZIP_PATH = r"C:\Users\18057\Documents\Data\fao-test-zips"
    results = run_optimization_analysis(ZIP_PATH)


class CSVAnalyzer:

    def analyze_csv_from_zip(self, zip_path: Path, csv_filename: str) -> Dict:
        """Analyze a CSV file directly from inside a ZIP"""
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(csv_filename) as csv_file:
                # Try different encodings
                encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

                for encoding in encodings:
                    try:
                        csv_file.seek(0)
                        csv_data = csv_file.read().decode(encoding)
                        df = pd.read_csv(StringIO(csv_data), dtype=str)
                        return self._analyze_dataframe(df, csv_filename, encoding)
                    except UnicodeDecodeError:
                        continue

                # If all encodings fail, use errors='ignore'
                csv_file.seek(0)
                csv_data = csv_file.read().decode("utf-8", errors="ignore")
                df = pd.read_csv(StringIO(csv_data), dtype=str)
                return self._analyze_dataframe(
                    df, csv_filename, "utf-8 (with errors ignored)"
                )

    def _analyze_dataframe(
        self, df: pd.DataFrame, csv_filename: str, encoding: str
    ) -> Dict:
        """Analyze a DataFrame and return structured information"""
        # Clean column names
        df.columns = df.columns.str.strip()

        # Analyze each column for type inference
        column_analysis = []
        for col in df.columns:
            col_info = self._analyze_column(df[col].head(1000), col)
            column_analysis.append(col_info)

        # Sample rows
        sample_rows = df.head(2).to_dict("records")

        return {
            "file_name": csv_filename,
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": df.columns.tolist(),
            "sql_column_names": [self._format_column_name(col) for col in df.columns],
            "encoding_used": encoding,
            "sample_rows": sample_rows,
            "column_analysis": column_analysis,
        }

    def _analyze_column(self, series, column_name: str) -> Dict:
        """Analyze a single column to infer type and properties"""

        clean_series = self._clean_quoted_values(series)

        sample_values = clean_series.dropna().head(4).tolist()
        non_null_count = int(clean_series.count())  # Convert to Python int
        total_count = len(clean_series)
        null_count = total_count - non_null_count
        unique_count = int(clean_series.nunique())  # Convert to Python int

        # Infer SQL type
        inferred_type = self._infer_sql_type(clean_series, column_name)
        sql_column_name = self._format_column_name(column_name)

        return {
            "column_name": column_name,
            "sql_column_name": sql_column_name,
            "sample_values": sample_values,
            "null_count": null_count,
            "non_null_count": non_null_count,
            "unique_count": unique_count,
            "inferred_sql_type": inferred_type,
            "is_likely_foreign_key": self._is_likely_foreign_key(column_name),
        }

    def _format_column_name(self, column_name: str) -> str:
        """Convert CSV column name to database-friendly name"""
        return (
            column_name.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
        )

    def _infer_sql_type(self, series, column_name: str) -> str:
        """Infer SQLAlchemy column type from data patterns"""
        # Drop nulls for analysis
        clean_series = series.dropna()

        if len(clean_series) == 0:
            return "String"

        # Check for specific FAO patterns first
        if self._is_year_column(column_name, clean_series):
            return "Integer"

        if self._is_code_column(column_name, clean_series):
            return "Integer"  # Most FAO codes are integers

        if self._is_value_column(column_name, clean_series):
            return "Float"

        # General pattern matching
        if self._is_integer_pattern(clean_series):
            return "Integer"

        if self._is_float_pattern(clean_series):
            return "Float"

        # Default to String
        return "String"

    def _is_year_column(self, column_name: str, series) -> bool:
        """Check if this looks like a year column"""
        if "year" in column_name.lower():
            try:
                clean_values = self._clean_quoted_values(series)
                numeric_values = pd.to_numeric(clean_values, errors="coerce").dropna()
                if len(numeric_values) > 0:
                    return numeric_values.between(1900, 2030).all()
            except:
                pass
        return False

    def _is_code_column(self, column_name: str, series) -> bool:
        """Check if this looks like an area/item/element code"""
        code_columns = ["area code", "item code"]
        if column_name.lower() in code_columns:
            return True
        return False

    def _is_value_column(self, column_name: str, series) -> bool:
        """Check if this looks like a numeric value column"""
        value_patterns = ["value", "price", "amount", "quantity", "rate"]
        return any(pattern in column_name.lower() for pattern in value_patterns)

    def _clean_quoted_values(self, series):
        """Remove quotes from values like '004', '123'"""
        return (
            series.astype(str)
            .str.strip()
            .str.replace("^'", "", regex=True)
            .str.replace("'$", "", regex=True)
        )

    def _is_integer_pattern(self, series) -> bool:
        """Check if series contains integer-like values"""
        try:
            # Clean quoted values like "'004'" -> "004"
            clean_values = self._clean_quoted_values(series)
            numeric_values = pd.to_numeric(clean_values, errors="coerce")

            # If most convert to numbers and are whole numbers
            valid_numbers = numeric_values.dropna()
            if len(valid_numbers) / len(series) > 0.8:  # 80% are numeric
                return (valid_numbers % 1 == 0).all()  # All are whole numbers
        except:
            pass
        return False

    def _is_float_pattern(self, series) -> bool:
        """Check if series contains float-like values"""
        try:
            clean_values = self._clean_quoted_values(series)
            numeric_values = pd.to_numeric(clean_values, errors="coerce")

            # If most convert to numbers
            valid_numbers = numeric_values.dropna()
            return len(valid_numbers) / len(series) > 0.8
        except:
            pass
        return False

    def _is_likely_foreign_key(self, column_name: str) -> bool:
        """Check if this looks like a foreign key"""
        fk_patterns = ["area code", "item code", "element code", "area_id", "item_id"]
        return any(pattern in column_name.lower() for pattern in fk_patterns)
