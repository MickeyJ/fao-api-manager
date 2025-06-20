from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass
import re
from sqlalchemy import text

from generator import singularize
from generator.logger import logger
from _fao_.src.db.database import get_db
from .relationship_type_tables import RELATIONSHIP_REFERENCE_TABLES


@dataclass
class TypeScore:
    """Result of relationship scoring"""

    type: str
    score: float
    confidence: str  # 'high', 'medium', 'low'
    properties: Dict
    source_evidence: List[str]  # What contributed to this score
    reference_type: str  # Which reference table this came from


class RelationshipTypeScorer:
    """Score-based relationship type inference system"""

    def __init__(self):
        # Core relationship types with their scoring patterns
        # These patterns work across ALL reference types dynamically
        self.relationship_patterns = {
            "TRADES": {
                "table_patterns": ["trade", "import", "export", "bilateral", "detailed_trade_matrix"],
                "item_patterns": ["import", "export", "trade", "bilateral", "dependency", "self-sufficiency"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["flow_direction", "commodity_type", "measure"],
            },
            "PRODUCES": {
                "table_patterns": ["production", "crop", "livestock", "yield"],
                "item_patterns": ["production", "yield", "harvest", "laying", "milk", "output", "produce"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["production_type", "measure"],
            },
            "SUPPLIES": {
                "table_patterns": ["balance", "nutrition", "dietary", "food_and_diet"],
                "item_patterns": [
                    "protein",
                    "fat",
                    "vitamin",
                    "nutrient",
                    "kcal",
                    "calcium",
                    "iron",
                    "dietary",
                    "nutritional",
                ],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["nutrient_type", "unit", "measure"],
            },
            "CONSUMES": {
                "table_patterns": ["household", "consumption", "dietary", "food_supply"],
                "item_patterns": ["feed", "food supply", "consumption", "intake", "per capita", "household"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["consumption_type", "purpose"],
            },
            "UTILIZES": {
                "table_patterns": ["water", "aquastat", "inputs", "fertilizer", "pesticide", "irrigation"],
                "item_patterns": [
                    "withdrawal",
                    "use",
                    "application",
                    "equipped",
                    "irrigated",
                    "fertilizer",
                    "pesticide",
                ],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["resource_type", "purpose", "intensity"],
            },
            "EMITS": {
                "table_patterns": ["emissions", "climate", "greenhouse", "ghg"],
                "item_patterns": ["emissions", "ch4", "n2o", "co2", "carbon", "greenhouse", "methane", "nitrous"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["gas_type", "source", "category"],
            },
            "EMPLOYS": {
                "table_patterns": ["employment", "labor", "worker", "researcher", "asti"],
                "item_patterns": ["employment", "workers", "researchers", "labor", "farmers", "staff"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["role", "sector", "intensity"],
            },
            "INVESTS": {
                "table_patterns": ["investment", "capital", "credit", "finance", "expenditure"],
                "item_patterns": ["investment", "capital", "expenditure", "credit", "financing", "gross fixed"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["investment_type", "currency", "source"],
            },
            "MEASURES": {
                "table_patterns": ["indicators", "indices", "statistics", "metrics"],
                "item_patterns": ["index", "share", "percentage", "ratio", "per", "indicator", "average", "median"],
                "strong_weight": 3,
                "medium_weight": 2,
                "weak_weight": 1,
                "properties": ["measure_type", "category", "unit"],
            },
            "IMPACTS": {
                "table_patterns": ["change", "impact", "effect", "temperature_change"],
                "item_patterns": ["change", "impact", "loss", "damage", "effect", "degradation"],
                "strong_weight": 5,
                "medium_weight": 3,
                "weak_weight": 1,
                "properties": ["impact_type", "magnitude", "direction"],
            },
        }

        # Dynamic reference types to analyze (can be extended)
        self.reference_types_to_analyze = ["elements", "indicators", "food_values", "industries", "factors"]

        # Context-specific boosters (table + item pattern combinations)
        self.context_boosters = {
            "TRADES": [(r"trade|bilateral", r"import|export"), (r"detailed.*matrix", r"quantity|value")],
            "CONSUMES": [(r"food|diet", r"intake|consume")],
            "PRODUCES": [(r"production", r"production|yield"), (r"crops|livestock", r"harvest|yield|milk|laying")],
            "SUPPLIES": [(r"balance|dietary", r"protein|fat|vitamin|nutrient"), (r"nutrition", r"kcal|calcium|iron")],
            "UTILIZES": [(r"aquastat|water", r"withdrawal|irrigat"), (r"fertilizer", r"application|use")],
            "EMITS": [(r"emission", r"ch4|n2o|co2|emission"), (r"climate", r"greenhouse|carbon")],
        }

    def analyze_dataset(self, table_name: str, foreign_keys: List[Dict]) -> Dict[str, List[TypeScore]]:
        """
        Main entry point - analyzes a dataset and returns possible relationships
        Returns dict mapping reference_type -> list of scored relationships
        """
        logger.debug(f"Analyzing relationships for {table_name}")

        # Check if we have enough FKs upfront
        if len(foreign_keys) < 2:
            logger.error(f"  Not enough FKs for {table_name} ({len(foreign_keys)} FKs)")
            return {}  # Return empty, no relationships possible

        # Check for special patterns first
        special_pattern = self._check_special_patterns(table_name, foreign_keys)
        if special_pattern:
            return special_pattern

        # Get which reference types this table actually uses
        available_ref_types = self._get_available_reference_tables(foreign_keys)

        if not available_ref_types:
            logger.warning(f"No analyzable reference types found for {table_name}")
            return {}

        # Analyze each reference type
        all_relationships = {}

        for ref_type in available_ref_types:
            if ref_type not in self.reference_types_to_analyze:
                logger.debug(f"  Skipping {ref_type} for {table_name} table (not in types to analyze)")
                continue

            logger.debug(f" 🎈 Analyzing {ref_type} for {table_name}")

            # Get reference data from database
            ref_items = self._query_reference_data(table_name, ref_type)

            if ref_items:
                # Score relationships for these items
                scored_relationships = self._score_reference_items(table_name, ref_items, ref_type)

                if scored_relationships:
                    all_relationships[ref_type] = scored_relationships

        return all_relationships

    def _query_reference_data(self, table_name: str, reference_type: str) -> List[Dict]:
        """Query reference data from database dynamically"""
        ref_config = RELATIONSHIP_REFERENCE_TABLES.get(reference_type)
        if not ref_config:
            logger.warning(f"No configuration found for reference type: {reference_type}")
            return []

        db_gen = get_db()
        db = next(db_gen)

        try:
            # Build dynamic query based on configuration
            query = text(
                f"""
                SELECT DISTINCT 
                    r.id as id,
                    r.{ref_config['code_column']} as code,
                    r.{ref_config['name_column']} as name,
                    r.source_dataset
                FROM {ref_config['table']} r
                WHERE r.source_dataset = :table_name
                ORDER BY r.{ref_config['code_column']}
            """
            )

            result = db.execute(query, {"table_name": table_name})
            items = [dict(row) for row in result.mappings().all()]

            logger.debug(f"    Found {len(items)} {reference_type} for {table_name} table")
            return items

        except Exception as e:
            logger.error(f"Failed to query {reference_type}: {e}")
            return []
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _score_reference_items(self, table_name: str, ref_items: List[Dict], ref_type: str) -> List[TypeScore]:
        """Score all reference items and group by relationship type"""
        # Group items by their highest scoring relationship type
        relationship_groups = defaultdict(list)

        for item in ref_items:
            scores = self._calculate_scores(table_name, item["name"], item["code"])

            if scores:
                # Get highest scoring relationship
                best_type = max(scores.items(), key=lambda x: x[1])[0]
                confidence = self._calculate_confidence(scores)

                relationship_groups[best_type].append(
                    {"item": item, "score": scores[best_type], "confidence": confidence, "all_scores": scores}
                )

        # Convert groups to TypeScore objects
        results = []
        for rel_type, items in relationship_groups.items():
            # Get representative items (highest scoring)
            sorted_items = sorted(items, key=lambda x: x["score"], reverse=True)

            # Aggregate score (weighted average giving more weight to high scorers)
            total_weight = sum(item["score"] for item in sorted_items)
            if total_weight > 0:
                weighted_score = sum(item["score"] ** 2 for item in sorted_items) / total_weight
            else:
                weighted_score = 0

            # Aggregate confidence
            confidence = self._aggregate_confidence([item["confidence"] for item in sorted_items])

            # Extract item codes and names for the relationship
            item_codes = [item["item"]["code"] for item in sorted_items]
            item_names = [item["item"]["name"] for item in sorted_items]

            results.append(
                TypeScore(
                    type=rel_type,
                    score=weighted_score,
                    confidence=confidence,
                    properties=self._extract_properties(rel_type, sorted_items, ref_type, item_codes, item_names),
                    source_evidence=item_names[:5],  # Top 5 examples
                    reference_type=ref_type,
                )
            )

        return sorted(results, key=lambda x: x.score, reverse=True)

    def _calculate_scores(self, table_name: str, item_name: str, item_code: str) -> Dict[str, float]:
        """Calculate scores for each relationship type"""
        scores = defaultdict(float)
        table_lower = table_name.lower()
        item_lower = item_name.lower()

        for rel_type, config in self.relationship_patterns.items():
            # 1. Score based on table name patterns
            for pattern in config["table_patterns"]:
                if re.search(pattern, table_lower):
                    scores[rel_type] += config["medium_weight"]

            # 2. Score based on item name patterns
            for pattern in config["item_patterns"]:
                if re.search(pattern, item_lower):
                    scores[rel_type] += config["strong_weight"]

            # 3. Apply context-specific boosters
            if rel_type in self.context_boosters:
                for table_pattern, item_pattern in self.context_boosters[rel_type]:
                    if re.search(table_pattern, table_lower) and re.search(item_pattern, item_lower):
                        scores[rel_type] *= 1.5

            # 4. Weak signals from generic patterns
            if self._has_weak_signal(item_name, rel_type):
                scores[rel_type] += config["weak_weight"]

        # Apply penalties for overly generic items
        if self._is_too_generic(item_name):
            scores["MEASURES"] += 3
            for rel_type in scores:
                if rel_type != "MEASURES":
                    scores[rel_type] *= 0.7

        return dict(scores)

    def _has_weak_signal(self, item_name: str, rel_type: str) -> bool:
        """Check for weak signals that might indicate a relationship type"""
        weak_signals = {
            "TRADES": ["value", "quantity", "balance"],
            "PRODUCES": ["area", "animals", "stocks"],
            "UTILIZES": ["area", "efficiency", "intensity"],
            "MEASURES": ["total", "average", "mean"],
        }

        if rel_type in weak_signals:
            item_lower = item_name.lower()
            return any(signal in item_lower for signal in weak_signals[rel_type])
        return False

    def _is_too_generic(self, item_name: str) -> bool:
        """Check if this is too generic to be anything but MEASURES"""
        generic_patterns = [
            r"^total",
            r"^average",
            r"^mean",
            r"^median",
            r"confidence interval",
            r"standard deviation",
            r"^percentage",
            r"^share of",
            r"^ratio",
        ]
        item_lower = item_name.lower()
        return any(re.search(pattern, item_lower) for pattern in generic_patterns)

    def _calculate_confidence(self, scores: Dict[str, float]) -> str:
        """Calculate confidence based on score distribution"""
        if not scores:
            return "low"

        sorted_scores = sorted(scores.values(), reverse=True)
        best_score = sorted_scores[0]

        # Absolute threshold check
        if best_score < 3:
            return "low"

        # Relative difference check
        if len(sorted_scores) > 1:
            second_best = sorted_scores[1]
            # Prevent division by zero
            if second_best == 0:
                if best_score >= 5:
                    return "high"
                return "medium"

            ratio = best_score / second_best

            if ratio > 2.0 and best_score >= 5:
                return "high"
            elif ratio > 1.5 and best_score >= 3:
                return "medium"
        elif best_score >= 5:
            return "high"

        return "low"

    def _aggregate_confidence(self, confidences: List[str]) -> str:
        """Aggregate multiple confidence scores"""
        if not confidences:
            return "low"

        confidence_values = {"high": 3, "medium": 2, "low": 1}
        avg_value = sum(confidence_values.get(c, 1) for c in confidences) / len(confidences)

        if avg_value >= 2.5:
            return "high"
        elif avg_value >= 1.5:
            return "medium"
        return "low"

    def _extract_properties(
        self, rel_type: str, items: List[Dict], ref_type: str, item_codes: List[str], item_names: List[str]
    ) -> Dict:
        """Extract properties for the relationship dynamically"""
        properties = {}

        # Always include reference metadata
        properties[f"{singularize(ref_type)}_codes"] = item_codes  # e.g., "element_codes"
        properties[f"{singularize(ref_type)}"] = item_names  # e.g., "elements"

        # Get the last item as representative (highest scoring in this group)
        if items:
            rep_item = items[0]
            properties[f"{singularize(ref_type)}_code"] = rep_item["item"]["code"]
            properties[f"{singularize(ref_type)}"] = rep_item["item"]["name"]

        # Add relationship-specific properties
        if rel_type == "TRADES":
            # Infer flow direction from item names
            has_import = any("import" in name.lower() for name in item_names)
            has_export = any("export" in name.lower() for name in item_names)

            if has_import and has_export:
                properties["flow_direction"] = "bidirectional"
            elif has_import:
                properties["flow_direction"] = "import"
            elif has_export:
                properties["flow_direction"] = "export"
            else:
                properties["flow_direction"] = "unspecified"

        elif rel_type == "EMITS":
            # Extract gas type if present
            for gas in ["ch4", "n2o", "co2"]:
                if any(gas in name.lower() for name in item_names):
                    properties["gas_type"] = gas.upper()
                    break

        elif rel_type == "SUPPLIES":
            # Extract nutrient type if present
            nutrients = ["protein", "fat", "vitamin", "calcium", "iron"]
            for nutrient in nutrients:
                if any(nutrient in name.lower() for name in item_names):
                    properties["nutrient_type"] = nutrient
                    break

        return properties

    def _get_available_reference_tables(self, foreign_keys: List[Dict]) -> List[str]:
        """Get which reference types are available based on foreign keys"""
        available_types = []
        fk_tables = set(fk["table_name"] for fk in foreign_keys)

        # Check all configured reference types
        for ref_type, config in RELATIONSHIP_REFERENCE_TABLES.items():
            if config["table"] in fk_tables:
                available_types.append(ref_type)

        return available_types

    def _check_special_patterns(self, table_name: str, foreign_keys: List[Dict]) -> Optional[Dict]:
        """Check for special patterns that bypass normal scoring"""
        fk_tables = set(fk["table_name"] for fk in foreign_keys)

        # Bilateral trade pattern
        if "reporter_country_codes" in fk_tables and "partner_country_codes" in fk_tables:
            return {
                "bilateral_trade": [
                    TypeScore(
                        type="SHARES",
                        score=10.0,
                        confidence="high",
                        properties={
                            "pattern": "bilateral_trade",
                            "source_fk": "reporter_country_code_id",
                            "target_fk": "partner_country_code_id",
                        },
                        source_evidence=["Bilateral trade matrix pattern"],
                        reference_type="bilateral",
                    )
                ]
            }

        # Donor-recipient pattern
        if "recipient_country_codes" in fk_tables and "donors" in fk_tables:
            return {
                "donor_recipient": [
                    TypeScore(
                        type="SHARES",
                        score=10.0,
                        confidence="high",
                        properties={
                            "pattern": "donor_recipient",
                            "source_fk": "recipient_country_code_id",
                            "target_fk": "donor_id",
                        },
                        source_evidence=["Donor-recipient pattern"],
                        reference_type="bilateral",
                    )
                ]
            }

        return None


# Singleton instance
relationship_type_scorer = RelationshipTypeScorer()
