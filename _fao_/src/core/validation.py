from typing import Set, Optional, Dict, Any, Type, TYPE_CHECKING, List
from sqlalchemy.orm import Session
from sqlalchemy import select, distinct
from datetime import datetime, timedelta
from functools import lru_cache

# Type checking imports (doesn't run at runtime)
if TYPE_CHECKING:
    from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
    from fao.src.db.pipelines.reporter_country_codes.reporter_country_codes_model import ReporterCountryCodes
    from fao.src.db.pipelines.partner_country_codes.partner_country_codes_model import PartnerCountryCodes
    from fao.src.db.pipelines.recipient_country_codes.recipient_country_codes_model import RecipientCountryCodes
    from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
    from fao.src.db.pipelines.elements.elements_model import Elements
    from fao.src.db.pipelines.flags.flags_model import Flags
    from fao.src.db.pipelines.currencies.currencies_model import Currencies
    from fao.src.db.pipelines.sources.sources_model import Sources
    from fao.src.db.pipelines.releases.releases_model import Releases
    from fao.src.db.pipelines.sexs.sexs_model import Sexs
    from fao.src.db.pipelines.indicators.indicators_model import Indicators
    from fao.src.db.pipelines.population_age_groups.population_age_groups_model import PopulationAgeGroups
    from fao.src.db.pipelines.surveys.surveys_model import Surveys
    from fao.src.db.pipelines.purposes.purposes_model import Purposes
    from fao.src.db.pipelines.donors.donors_model import Donors
    from fao.src.db.pipelines.food_groups.food_groups_model import FoodGroups
    from fao.src.db.pipelines.geographic_levels.geographic_levels_model import GeographicLevels
    from fao.src.db.pipelines.food_values.food_values_model import FoodValues
    from fao.src.db.pipelines.industries.industries_model import Industries
    from fao.src.db.pipelines.factors.factors_model import Factors


class ValidationCache:
    """Simple in-memory cache for validation data"""

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl = timedelta(seconds=ttl_seconds)
        self._cache: Dict[str, tuple[Any, datetime]] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get value if not expired"""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if datetime.utcnow() - timestamp < self.ttl:
                return value
            else:
                del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """Set value with timestamp"""
        self._cache[key] = (value, datetime.utcnow())


# Global cache instance
_cache = ValidationCache(ttl_seconds=3600)


def _get_valid_codes_generic(db: Session, model_class: Type[Any], code_column_name: str, cache_key: str) -> Set[str]:
    """Generic function to get valid codes with caching"""
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # Use getattr to access the column dynamically
    column = getattr(model_class, code_column_name)
    codes = db.query(column).distinct().all()
    valid_codes = {code[0] for code in codes}

    _cache.set(cache_key, valid_codes)
    return valid_codes



def get_valid_area_code(db: Session) -> Set[str]:
    """Get valid area codes with caching"""
    from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes

    return _get_valid_codes_generic(db, AreaCodes, "area_code", "area_code")

def get_valid_reporter_country_code(db: Session) -> Set[str]:
    """Get valid reporter country codes with caching"""
    from fao.src.db.pipelines.reporter_country_codes.reporter_country_codes_model import ReporterCountryCodes

    return _get_valid_codes_generic(db, ReporterCountryCodes, "reporter_country_code", "reporter_country_code")

def get_valid_partner_country_code(db: Session) -> Set[str]:
    """Get valid partner country codes with caching"""
    from fao.src.db.pipelines.partner_country_codes.partner_country_codes_model import PartnerCountryCodes

    return _get_valid_codes_generic(db, PartnerCountryCodes, "partner_country_code", "partner_country_code")

def get_valid_recipient_country_code(db: Session) -> Set[str]:
    """Get valid recipient country codes with caching"""
    from fao.src.db.pipelines.recipient_country_codes.recipient_country_codes_model import RecipientCountryCodes

    return _get_valid_codes_generic(db, RecipientCountryCodes, "recipient_country_code", "recipient_country_code")

def get_valid_item_code(db: Session) -> Set[str]:
    """Get valid item codes with caching"""
    from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes

    return _get_valid_codes_generic(db, ItemCodes, "item_code", "item_code")

def get_valid_element_code(db: Session) -> Set[str]:
    """Get valid element codes with caching"""
    from fao.src.db.pipelines.elements.elements_model import Elements

    return _get_valid_codes_generic(db, Elements, "element_code", "element_code")

def get_valid_flag(db: Session) -> Set[str]:
    """Get valid flags with caching"""
    from fao.src.db.pipelines.flags.flags_model import Flags

    return _get_valid_codes_generic(db, Flags, "flag", "flag")

def get_valid_iso_currency_code(db: Session) -> Set[str]:
    """Get valid iso currency codes with caching"""
    from fao.src.db.pipelines.currencies.currencies_model import Currencies

    return _get_valid_codes_generic(db, Currencies, "iso_currency_code", "iso_currency_code")

def get_valid_source_code(db: Session) -> Set[str]:
    """Get valid source codes with caching"""
    from fao.src.db.pipelines.sources.sources_model import Sources

    return _get_valid_codes_generic(db, Sources, "source_code", "source_code")

def get_valid_release_code(db: Session) -> Set[str]:
    """Get valid release codes with caching"""
    from fao.src.db.pipelines.releases.releases_model import Releases

    return _get_valid_codes_generic(db, Releases, "release_code", "release_code")

def get_valid_sex_code(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from fao.src.db.pipelines.sexs.sexs_model import Sexs

    return _get_valid_codes_generic(db, Sexs, "sex_code", "sex_code")

def get_valid_indicator_code(db: Session) -> Set[str]:
    """Get valid indicator codes with caching"""
    from fao.src.db.pipelines.indicators.indicators_model import Indicators

    return _get_valid_codes_generic(db, Indicators, "indicator_code", "indicator_code")

def get_valid_population_age_group_code(db: Session) -> Set[str]:
    """Get valid population age group codes with caching"""
    from fao.src.db.pipelines.population_age_groups.population_age_groups_model import PopulationAgeGroups

    return _get_valid_codes_generic(db, PopulationAgeGroups, "population_age_group_code", "population_age_group_code")

def get_valid_survey_code(db: Session) -> Set[str]:
    """Get valid survey codes with caching"""
    from fao.src.db.pipelines.surveys.surveys_model import Surveys

    return _get_valid_codes_generic(db, Surveys, "survey_code", "survey_code")

def get_valid_purpose_code(db: Session) -> Set[str]:
    """Get valid purpose codes with caching"""
    from fao.src.db.pipelines.purposes.purposes_model import Purposes

    return _get_valid_codes_generic(db, Purposes, "purpose_code", "purpose_code")

def get_valid_donor_code(db: Session) -> Set[str]:
    """Get valid donor codes with caching"""
    from fao.src.db.pipelines.donors.donors_model import Donors

    return _get_valid_codes_generic(db, Donors, "donor_code", "donor_code")

def get_valid_food_group_code(db: Session) -> Set[str]:
    """Get valid food group codes with caching"""
    from fao.src.db.pipelines.food_groups.food_groups_model import FoodGroups

    return _get_valid_codes_generic(db, FoodGroups, "food_group_code", "food_group_code")

def get_valid_geographic_level_code(db: Session) -> Set[str]:
    """Get valid geographic level codes with caching"""
    from fao.src.db.pipelines.geographic_levels.geographic_levels_model import GeographicLevels

    return _get_valid_codes_generic(db, GeographicLevels, "geographic_level_code", "geographic_level_code")

def get_valid_food_value_code(db: Session) -> Set[str]:
    """Get valid food value codes with caching"""
    from fao.src.db.pipelines.food_values.food_values_model import FoodValues

    return _get_valid_codes_generic(db, FoodValues, "food_value_code", "food_value_code")

def get_valid_industry_code(db: Session) -> Set[str]:
    """Get valid industry codes with caching"""
    from fao.src.db.pipelines.industries.industries_model import Industries

    return _get_valid_codes_generic(db, Industries, "industry_code", "industry_code")

def get_valid_factor_code(db: Session) -> Set[str]:
    """Get valid factor codes with caching"""
    from fao.src.db.pipelines.factors.factors_model import Factors

    return _get_valid_codes_generic(db, Factors, "factor_code", "factor_code")



def is_valid_area_code(code: str, db: Session) -> bool:
    """Check if area code is valid"""
    valid_codes = get_valid_area_code(db)
    return code in valid_codes

def is_valid_reporter_country_code(code: str, db: Session) -> bool:
    """Check if reporter country code is valid"""
    valid_codes = get_valid_reporter_country_code(db)
    return code in valid_codes

def is_valid_partner_country_code(code: str, db: Session) -> bool:
    """Check if partner country code is valid"""
    valid_codes = get_valid_partner_country_code(db)
    return code in valid_codes

def is_valid_recipient_country_code(code: str, db: Session) -> bool:
    """Check if recipient country code is valid"""
    valid_codes = get_valid_recipient_country_code(db)
    return code in valid_codes

def is_valid_item_code(code: str, db: Session) -> bool:
    """Check if item code is valid"""
    valid_codes = get_valid_item_code(db)
    return code in valid_codes

def is_valid_element_code(code: str, db: Session) -> bool:
    """Check if element code is valid"""
    valid_codes = get_valid_element_code(db)
    return code in valid_codes

def is_valid_flag(code: str, db: Session) -> bool:
    """Check if flag is valid"""
    valid_codes = get_valid_flag(db)
    return code in valid_codes

def is_valid_iso_currency_code(code: str, db: Session) -> bool:
    """Check if iso currency code is valid"""
    valid_codes = get_valid_iso_currency_code(db)
    return code in valid_codes

def is_valid_source_code(code: str, db: Session) -> bool:
    """Check if source code is valid"""
    valid_codes = get_valid_source_code(db)
    return code in valid_codes

def is_valid_release_code(code: str, db: Session) -> bool:
    """Check if release code is valid"""
    valid_codes = get_valid_release_code(db)
    return code in valid_codes

def is_valid_sex_code(code: str, db: Session) -> bool:
    """Check if sex code is valid"""
    valid_codes = get_valid_sex_code(db)
    return code in valid_codes

def is_valid_indicator_code(code: str, db: Session) -> bool:
    """Check if indicator code is valid"""
    valid_codes = get_valid_indicator_code(db)
    return code in valid_codes

def is_valid_population_age_group_code(code: str, db: Session) -> bool:
    """Check if population age group code is valid"""
    valid_codes = get_valid_population_age_group_code(db)
    return code in valid_codes

def is_valid_survey_code(code: str, db: Session) -> bool:
    """Check if survey code is valid"""
    valid_codes = get_valid_survey_code(db)
    return code in valid_codes

def is_valid_purpose_code(code: str, db: Session) -> bool:
    """Check if purpose code is valid"""
    valid_codes = get_valid_purpose_code(db)
    return code in valid_codes

def is_valid_donor_code(code: str, db: Session) -> bool:
    """Check if donor code is valid"""
    valid_codes = get_valid_donor_code(db)
    return code in valid_codes

def is_valid_food_group_code(code: str, db: Session) -> bool:
    """Check if food group code is valid"""
    valid_codes = get_valid_food_group_code(db)
    return code in valid_codes

def is_valid_geographic_level_code(code: str, db: Session) -> bool:
    """Check if geographic level code is valid"""
    valid_codes = get_valid_geographic_level_code(db)
    return code in valid_codes

def is_valid_food_value_code(code: str, db: Session) -> bool:
    """Check if food value code is valid"""
    valid_codes = get_valid_food_value_code(db)
    return code in valid_codes

def is_valid_industry_code(code: str, db: Session) -> bool:
    """Check if industry code is valid"""
    valid_codes = get_valid_industry_code(db)
    return code in valid_codes

def is_valid_factor_code(code: str, db: Session) -> bool:
    """Check if factor code is valid"""
    valid_codes = get_valid_factor_code(db)
    return code in valid_codes


def is_valid_range(min_value: Any, max_value: Any) -> bool:
    """Validate that min is less than max for range queries."""
    if min_value is None or max_value is None:
        return True  # Partial ranges are valid
    return min_value <= max_value


def is_valid_sort_direction(direction: str) -> bool:
    """Validate sort direction is asc or desc."""
    return direction.lower() in ["asc", "desc"]


def is_valid_aggregation_function(function: str) -> bool:
    """Validate aggregation function is supported."""
    valid_functions = ["sum", "avg", "min", "max", "count", "count_distinct"]
    return function in valid_functions


def validate_fields_exist(requested_fields: List[str], allowed_fields: set) -> List[str]:
    """Validate all requested fields exist in allowed fields.
    
    Returns list of invalid fields, empty if all valid.
    """
    # 'id' is always allowed even if not in allowed_fields
    invalid_fields = []
    for field in requested_fields:
        if field != 'id' and field not in allowed_fields:
            invalid_fields.append(field)
    return invalid_fields


def validate_model_has_columns(model_class: Type, column_names: List[str]) -> List[str]:
    """Validate model has all specified columns.
    
    Returns list of missing columns, empty if all exist.
    """
    missing_columns = []
    for col_name in column_names:
        if not hasattr(model_class, col_name):
            missing_columns.append(col_name)
    return missing_columns