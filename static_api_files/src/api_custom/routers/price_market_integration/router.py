from pathlib import Path
import numpy as np
from typing import List, Optional
from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text, select, and_, or_, func
from pydantic import BaseModel

# Correct imports following project patterns
from fao.src.db.database import get_db
from fao.src.core import settings
from fao.src.core.validation import is_valid_item_code, is_valid_element_code, is_valid_area_code
from fao.src.core.exceptions import (
    invalid_parameter,
    invalid_item_code,
    invalid_element_code,
    invalid_area_code,
    missing_parameter,
    no_data_found,
)
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags
from fao.src.db.pipelines.prices.prices_model import Prices


def load_sql(filename: str) -> str:
    """Load SQL query from file"""
    sql_path = Path(__file__).parent / "sql" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text()


router = APIRouter(
    prefix=f"/{settings.api_version_prefix}/prices/market-integration", tags=["prices", "analytics", "custom"]
)


class MarketIntegrationPair(BaseModel):
    area_1_code: str
    area_1_name: str
    area_2_code: str
    area_2_name: str
    correlation: float
    data_points: int
    integration_level: str


class MarketIntegrationResponse(BaseModel):
    item: dict
    parameters: dict
    integration_pairs: List[MarketIntegrationPair]
    summary: dict


def calculate_price_correlation(time_series, current_metrics):
    # Need at least 2 data points
    if len(time_series) < 2:
        return {
            "correlation": None,
            "correlation_based_integration": "insufficient_data",
            "ratio_based_integration": current_metrics["integration_level"],
        }

    # Calculate year-over-year returns
    returns1 = []
    returns2 = []

    for i in range(1, len(time_series)):
        return1 = (time_series[i]["price1"] - time_series[i - 1]["price1"]) / time_series[i - 1]["price1"]
        return2 = (time_series[i]["price2"] - time_series[i - 1]["price2"]) / time_series[i - 1]["price2"]
        returns1.append(return1)
        returns2.append(return2)

    # Calculate Pearson correlation
    correlation = np.corrcoef(returns1, returns2)[0, 1]

    # Handle NaN case (when all values are identical)
    if np.isnan(correlation):
        correlation = 0.0

    # Determine integration level based on correlation
    if correlation > 0.7:
        correlation_integration = "high"
    elif correlation > 0.3:
        correlation_integration = "moderate"
    else:
        correlation_integration = "none"

    return {
        "correlation": round(float(correlation), 3),  # Convert numpy float to Python float
        "correlation_based_integration": correlation_integration,
        "ratio_based_integration": current_metrics["integration_level"],
    }


# "flag": "A",
# "description": "Official figure",
# "record_count": 837045,
# "percentage": 66.34

# "element_code": "5530",
# "element": "Producer Price (LCU/tonne)",
# "record_count": 512874

# "element_code": "5532",
# "element": "Producer Price (USD/tonne)",
# "record_count": 162702

PRICE_ELEMENT_CODE = "5532"


@router.get("/data")
def get_market_integration(
    item_code: str = Query(..., description="FAO item code"),
    year_start: int = Query(2010, description="Start year"),  # Changed from 2010
    area_codes: Optional[List[str]] = Query(None, description="Specific countries to analyze"),
    db: Session = Depends(get_db),
):
    """
    Calculate market integration (price correlations) between countries for a commodity.

    High correlation (>0.8) indicates integrated markets where prices move together.
    Low correlation (<0.5) suggests isolated markets with independent price movements.
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Item Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not item_code:
        raise missing_parameter("item_code")

    if not is_valid_item_code(item_code, db):
        raise invalid_item_code(item_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Area Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not area_codes:
        raise missing_parameter("area_codes")

    if len(area_codes) > 10:
        raise invalid_parameter(
            param="area_codes", value=f"{len(area_codes)} items", reason="maximum 10 area codes allowed"
        )

    for area_code in area_codes:
        if area_code and not is_valid_area_code(area_code, db):
            raise invalid_area_code(area_code)

    # Load SQL from file
    market_integration_query = load_sql("market_integration.sql")

    # Execute with parameters
    results = (
        db.execute(
            text(market_integration_query),
            {"item_code": item_code, "start_year": year_start, "selected_area_codes": area_codes},  # PostgreSQL array
        )
        .mappings()
        .all()
    )

    if not results:
        raise no_data_found(
            dataset="prices", filters={"item_code": item_code, "area_codes": area_codes, "year_start": year_start}
        )

    comparisons = []
    for row in results:

        metrics = {
            "years_compared": row["years_compared"],
            "avg_ratio": float(row["avg_ratio"]),
            "volatility": float(row["ratio_volatility"]),
            "min_ratio": float(row["min_ratio"]),
            "max_ratio": float(row["max_ratio"]),
            "integration_level": row["integration_level"],
        }

        comparisons.append(
            {
                "country_pair": {
                    "country1": {
                        "area_id": row["country1_id"],
                        "area_code": row["country1_code"],
                        "area_name": row["country1"],
                    },
                    "country2": {
                        "area_id": row["country2_id"],
                        "area_code": row["country2_code"],
                        "area_name": row["country2"],
                    },
                },
                "metrics": metrics,
                "calculated_metrics": calculate_price_correlation(row["time_series"], metrics),
                "time_series": row["time_series"],  # Already JSON from query
            }
        )

    # Get item details
    item_info = db.query(ItemCodes).filter(ItemCodes.item_code == item_code).first()

    return {
        "item": {"code": item_code, "name": item_info.item if item_info else "Unknown"},
        "analysis_period": {
            "start_year": year_start,
            "end_year": max(max(point["year"] for point in comp["time_series"]) for comp in comparisons),
        },
        "countries_analyzed": len(area_codes),
        "comparisons_count": len(comparisons),
        "comparisons": comparisons,
    }


@router.get("/items")
def get_all_items(
    element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
    db: Session = Depends(get_db),
):
    """Get all food items that have price data"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Element Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not element_code:
        raise missing_parameter("element_code")

    if element_code and not is_valid_element_code(element_code, db):
        raise invalid_element_code(element_code)

    query = (
        select(
            ItemCodes.id,
            ItemCodes.item.label("name"),
            ItemCodes.item_code.label("item_code"),
            ItemCodes.item_code_cpc.label("cpc_code"),
            func.count(func.distinct(Prices.id)).label("price_points"),
            func.count(func.distinct(Prices.area_code_id)).label("countries_with_data"),
            func.count(func.distinct(Prices.year)).label("years_with_data"),
            func.min(Prices.year).label("earliest_year"),
            func.max(Prices.year).label("latest_year"),
            # Average data points per country (indicates data density)
            (func.count(Prices.id) / func.count(func.distinct(Prices.area_code_id))).label("avg_points_per_country"),
        )
        .join(Prices, ItemCodes.id == Prices.item_code_id)
        .join(Elements, Elements.id == Prices.element_code_id)
        .join(Flags, Flags.id == Prices.flag_id)
        .where(
            and_(
                Elements.element_code == element_code,  # '5532' for producer prices
                ItemCodes.source_dataset == "prices",
                Flags.flag == "A",
                Prices.year >= 2010,  # Recent data only
            )
        )
        .group_by(ItemCodes.id, ItemCodes.item, ItemCodes.item_code, ItemCodes.item_code_cpc)
        .having(
            and_(
                func.count(func.distinct(Prices.area_code_id)) >= 10,  # At least 10 countries
                func.count(func.distinct(Prices.year)) >= 5,  # At least 5 years of data
            )
        )
        .order_by(
            func.count(func.distinct(Prices.area_code_id)).desc(),  # Most countries first
            func.count(func.distinct(Prices.year)).desc(),  # Then most years
            func.count(Prices.id).desc(),  # Then most data points
        )
    )

    results = db.execute(query).mappings().all()

    if not results:
        raise no_data_found(
            dataset="item_codes",
            filters={
                "element_code": element_code,
            },
        )

    return {"items": [dict(item) for item in results]}


@router.get("/available-countries")
def get_countries_with_price_data(
    item_code: str = Query(..., description="FAO item code"),
    year_start: int = Query(2010, description="Start year"),
    year_end: int = Query(2024, description="End year"),
    db: Session = Depends(get_db),
):
    """
    Get list of countries that have price data for a specific item in the given time range.
    Useful for the frontend to show available options.
    """

    # Validate item code
    if not item_code:
        raise missing_parameter("item_code")

    if not is_valid_item_code(item_code, db):
        raise invalid_item_code(item_code)

    # Load SQL query
    query_sql = load_sql("available_countries_for_item.sql")

    params = {"item_code": item_code, "year_start": year_start, "year_end": year_end}

    results = db.execute(text(query_sql), params).mappings().all()

    return {
        "item_code": item_code,
        "year_range": f"{year_start}-{year_end}",
        "countries": [dict(row) for row in results],
        "total_countries": len(results),
    }
