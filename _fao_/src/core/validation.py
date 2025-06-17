from typing import Set, Optional, Dict, Any, Type, TYPE_CHECKING
from sqlalchemy.orm import Session
from sqlalchemy import select, distinct
from datetime import datetime, timedelta
from functools import lru_cache

# Type checking imports (doesn't run at runtime)
if TYPE_CHECKING:
    from _fao_.src.db.pipelines.area_codes.area_codes_model import AreaCodes
    from _fao_.src.db.pipelines.item_codes.item_codes_model import ItemCodes
    from _fao_.src.db.pipelines.elements.elements_model import Elements


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


# Now use the generic function for all validators
def get_valid_area_codes(db: Session) -> Set[str]:
    """Get valid area codes with caching"""
    from _fao_.src.db.pipelines.area_codes.area_codes_model import AreaCodes

    return _get_valid_codes_generic(db, AreaCodes, "area_code", "area_codes")


def get_valid_item_codes(db: Session) -> Set[str]:
    """Get valid item codes with caching"""
    from _fao_.src.db.pipelines.item_codes.item_codes_model import ItemCodes

    return _get_valid_codes_generic(db, ItemCodes, "item_code", "item_codes")


def get_valid_element_codes(db: Session) -> Set[str]:
    """Get valid element codes with caching"""
    from _fao_.src.db.pipelines.elements.elements_model import Elements

    return _get_valid_codes_generic(db, Elements, "element_code", "element_codes")


def get_valid_flags(db: Session) -> Set[str]:
    """Get valid flags with caching"""
    from _fao_.src.db.pipelines.flags.flags_model import Flags

    return _get_valid_codes_generic(db, Flags, "flag", "flags")


def get_valid_donor_codes(db: Session) -> Set[str]:
    """Get valid donor codes with caching"""
    from _fao_.src.db.pipelines.donors.donors_model import Donors

    return _get_valid_codes_generic(db, Donors, "donor_code", "donor_codes")


def get_valid_source_codes(db: Session) -> Set[str]:
    """Get valid source codes with caching"""
    from _fao_.src.db.pipelines.sources.sources_model import Sources

    return _get_valid_codes_generic(db, Sources, "source_code", "source_codes")


def get_valid_purpose_codes(db: Session) -> Set[str]:
    """Get valid purpose codes with caching"""
    from _fao_.src.db.pipelines.purposes.purposes_model import Purposes

    return _get_valid_codes_generic(db, Purposes, "purpose_code", "purpose_codes")


def get_valid_sex_codes(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from _fao_.src.db.pipelines.sexs.sexs_model import Sexs

    return _get_valid_codes_generic(db, Sexs, "sex_code", "sex_codes")


def get_valid_release_codes(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from _fao_.src.db.pipelines.releases.releases_model import Releases

    return _get_valid_codes_generic(db, Releases, "release_code", "release_codes")


def get_valid_reporter_country_codes(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from _fao_.src.db.pipelines.reporter_country_codes.reporter_country_codes_model import ReporterCountryCodes

    return _get_valid_codes_generic(db, ReporterCountryCodes, "reporter_country_code", "reporter_country_codes")


def get_valid_partner_country_codes(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from _fao_.src.db.pipelines.partner_country_codes.partner_country_codes_model import PartnerCountryCodes

    return _get_valid_codes_generic(db, PartnerCountryCodes, "partner_country_code", "partner_country_codes")


def get_valid_recipient_country_codes(db: Session) -> Set[str]:
    """Get valid sex codes with caching"""
    from _fao_.src.db.pipelines.recipient_country_codes.recipient_country_codes_model import RecipientCountryCodes

    return _get_valid_codes_generic(db, RecipientCountryCodes, "recipient_country_code", "recipient_country_codes")


def get_valid_currency_codes(db: Session) -> Set[str]:
    """Get valid currency codes with caching"""
    from _fao_.src.db.pipelines.currencies.currencies_model import Currencies

    return _get_valid_codes_generic(db, Currencies, "iso_currency_code", "currency_codes")


def get_valid_survey_codes(db: Session) -> Set[str]:
    """Get valid survey codes with caching"""
    from _fao_.src.db.pipelines.surveys.surveys_model import Surveys

    return _get_valid_codes_generic(db, Surveys, "survey_code", "survey_codes")


def get_valid_population_age_group_codes(db: Session) -> Set[str]:
    """Get valid population age group codes with caching"""
    from _fao_.src.db.pipelines.population_age_groups.population_age_groups_model import PopulationAgeGroups

    return _get_valid_codes_generic(db, PopulationAgeGroups, "population_age_group_code", "population_age_group_codes")


def get_valid_indicator_codes(db: Session) -> Set[str]:
    """Get valid indicator codes with caching"""
    from _fao_.src.db.pipelines.indicators.indicators_model import Indicators

    return _get_valid_codes_generic(db, Indicators, "indicator_code", "indicator_codes")


def get_valid_food_group_codes(db: Session) -> Set[str]:
    """Get valid food group codes with caching"""
    from _fao_.src.db.pipelines.food_groups.food_groups_model import FoodGroups

    return _get_valid_codes_generic(db, FoodGroups, "food_group_code", "food_group_codes")


def get_valid_geographic_level_codes(db: Session) -> Set[str]:
    """Get valid geographic level codes with caching"""
    from _fao_.src.db.pipelines.geographic_levels.geographic_levels_model import GeographicLevels

    return _get_valid_codes_generic(db, GeographicLevels, "geographic_level_code", "geographic_level_codes")


# Validation functions
def is_valid_area_code(code: str, db: Session) -> bool:
    """Check if area code is valid"""
    valid_codes = get_valid_area_codes(db)
    return code in valid_codes


def is_valid_item_code(code: str, db: Session) -> bool:
    """Check if item code is valid"""
    valid_codes = get_valid_item_codes(db)
    return code in valid_codes


def is_valid_element_code(code: str, db: Session) -> bool:
    """Check if element code is valid"""
    valid_codes = get_valid_element_codes(db)
    return code in valid_codes


def is_valid_flag(code: str, db: Session) -> bool:
    """Check if flag is valid"""
    valid_codes = get_valid_flags(db)
    return code in valid_codes


def is_valid_donor_code(code: str, db: Session) -> bool:
    """Check if donor code is valid"""
    valid_codes = get_valid_donor_codes(db)
    return code in valid_codes


def is_valid_source_code(code: str, db: Session) -> bool:
    """Check if source code is valid"""
    valid_codes = get_valid_source_codes(db)
    return code in valid_codes


def is_valid_purpose_code(code: str, db: Session) -> bool:
    """Check if purpose code is valid"""
    valid_codes = get_valid_purpose_codes(db)
    return code in valid_codes


def is_valid_sex_code(code: str, db: Session) -> bool:
    """Check if sex code is valid"""
    valid_codes = get_valid_sex_codes(db)
    return code in valid_codes


def is_valid_release_code(code: str, db: Session) -> bool:
    """Check if release code is valid"""
    valid_codes = get_valid_release_codes(db)
    return code in valid_codes


def is_valid_reporter_country_code(code: str, db: Session) -> bool:
    """Check if reporter_country code is valid"""
    valid_codes = get_valid_reporter_country_codes(db)
    return code in valid_codes


def is_valid_partner_country_code(code: str, db: Session) -> bool:
    """Check if partner_country code is valid"""
    valid_codes = get_valid_partner_country_codes(db)
    return code in valid_codes


def is_valid_recipient_country_code(code: str, db: Session) -> bool:
    """Check if recipient_country code is valid"""
    valid_codes = get_valid_recipient_country_codes(db)
    return code in valid_codes


def is_valid_currency_code(code: str, db: Session) -> bool:
    """Check if currency code is valid"""
    valid_codes = get_valid_currency_codes(db)
    return code in valid_codes


def is_valid_survey_code(code: str, db: Session) -> bool:
    """Check if survey code is valid"""
    valid_codes = get_valid_survey_codes(db)
    return code in valid_codes


def is_valid_population_age_group_code(code: str, db: Session) -> bool:
    """Check if population age group code is valid"""
    valid_codes = get_valid_population_age_group_codes(db)
    return code in valid_codes


def is_valid_indicator_code(code: str, db: Session) -> bool:
    """Check if indicator code is valid"""
    valid_codes = get_valid_indicator_codes(db)
    return code in valid_codes


def is_valid_food_group_code(code: str, db: Session) -> bool:
    """Check if food group code is valid"""
    valid_codes = get_valid_food_group_codes(db)
    return code in valid_codes


def is_valid_geographic_level_code(code: str, db: Session) -> bool:
    """Check if geographic level code is valid"""
    valid_codes = get_valid_geographic_level_codes(db)
    return code in valid_codes


def is_valid_year_range(year_start: int, year_end: int):
    """Shared validation function"""
    return year_start < year_end
