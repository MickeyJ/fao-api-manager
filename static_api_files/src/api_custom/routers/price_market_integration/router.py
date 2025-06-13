from pathlib import Path
from typing import List, Optional
from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import table, column, text, select, and_, or_, func, literal

# Correct imports following project patterns
from fao.src.db.database import get_db
from fao.src.core import settings
from fao.src.core.utils import load_sql, calculate_price_correlation
from fao.src.core.validation import is_valid_item_code, is_valid_element_code, is_valid_area_code, is_valid_year_range
from fao.src.core.exceptions import (
    invalid_parameter,
    invalid_item_code,
    invalid_element_code,
    invalid_area_code,
    missing_parameter,
    no_data_found,
)
from fao.src.db.pipelines.exchange_rate.exchange_rate_model import ExchangeRate
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags
from fao.src.db.pipelines.prices.prices_model import Prices


router = APIRouter(prefix=f"/{settings.api_version_prefix}/market-integration", tags=["prices", "analytics", "custom"])

PRICE_ELEMENT_CODE = "5530"
START_YEAR = 1990


# @router.get("/correlations")
# def get_market_integration(
#     item_code: str = Query(..., description="FAO item code"),
#     element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
#     year_start: int = Query(START_YEAR, description="Start year"),
#     area_codes: Optional[List[str]] = Query(None, description="Specific countries to analyze"),
#     db: Session = Depends(get_db),
# ):
#     """
#     Calculate market integration (price correlations) between countries for a commodity.

#     High correlation (>0.8) indicates integrated markets where prices move together.
#     Low correlation (<0.5) suggests isolated markets with independent price movements.
#     """

#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     # Item Code validation
#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     if not item_code:
#         raise missing_parameter("item_code")

#     if not is_valid_item_code(item_code, db):
#         raise invalid_item_code(item_code)

#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     # Element Code validation
#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     if not element_code:
#         raise missing_parameter("element_code")

#     if element_code and not is_valid_element_code(element_code, db):
#         raise invalid_element_code(element_code)

#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     # Area Code validation
#     # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#     if not area_codes:
#         raise missing_parameter("area_codes")

#     if len(area_codes) > 4:
#         raise invalid_parameter(
#             param="area_codes", value=f"{len(area_codes)} items", reason="maximum 4 area codes allowed"
#         )

#     for area_code in area_codes:
#         if area_code and not is_valid_area_code(area_code, db):
#             raise invalid_area_code(area_code)

#     # Load SQL from file
#     market_integration_query = load_sql("sql/market_integration_USD.sql", Path(__file__).parent)

#     if element_code == "5530":  # LCU prices
#         market_integration_query = load_sql("sql/market_integration_LCU.sql", Path(__file__).parent)

#     # Execute with parameters
#     results = (
#         db.execute(
#             text(market_integration_query),
#             {
#                 "item_code": item_code,
#                 "element_code": element_code,
#                 "start_year": year_start,
#                 "selected_area_codes": area_codes,
#             },
#         )
#         .mappings()
#         .all()
#     )

#     if not results:
#         return {
#             "item": {"code": item_code, "name": "Unknown"},
#             "analysis_period": {
#                 "start_year": year_start,
#                 "end_year": None,
#             },
#             "countries_analyzed": len(area_codes),
#             "comparisons_count": 0,
#             "comparisons": [],
#         }

#     comparisons = []
#     for row in results:

#         metrics = {
#             "years_compared": row["years_compared"],
#             "avg_ratio": float(row["avg_ratio"]),
#             "volatility": float(row["ratio_volatility"]),
#             "min_ratio": float(row["min_ratio"]),
#             "max_ratio": float(row["max_ratio"]),
#             "integration_level": row["integration_level"],
#         }

#         comparisons.append(
#             {
#                 "country_pair": {
#                     "country1": {
#                         "area_id": row["country1_id"],
#                         "area_code": row["country1_code"],
#                         "area_name": row["country1"],
#                     },
#                     "country2": {
#                         "area_id": row["country2_id"],
#                         "area_code": row["country2_code"],
#                         "area_name": row["country2"],
#                     },
#                 },
#                 "metrics": metrics,
#                 "calculated_metrics": calculate_price_correlation(row["time_series"], metrics),
#                 "time_series": row["time_series"],  # Already JSON from query
#             }
#         )

#     # Get item details
#     item_info = db.query(ItemCodes).filter(ItemCodes.item_code == item_code).first()

#     return {
#         "element_code": element_code,
#         "item": {"code": item_code, "name": item_info.item if item_info else "Unknown"},
#         "analysis_period": {
#             "start_year": year_start,
#             "end_year": max(max(point["year"] for point in comp["time_series"]) for comp in comparisons),
#         },
#         "countries_analyzed": len(area_codes),
#         "comparisons_count": len(comparisons),
#         "comparisons": comparisons,
#     }


@router.get("/correlations")
def get_market_integration(
    item_code: str = Query(..., description="FAO item code"),
    element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
    year_start: int = Query(START_YEAR, description="Start year"),
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
    # Element Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not element_code:
        raise missing_parameter("element_code")

    if element_code and not is_valid_element_code(element_code, db):
        raise invalid_element_code(element_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Area Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not area_codes:
        raise missing_parameter("area_codes")

    if len(area_codes) > 4:
        raise invalid_parameter(
            param="area_codes", value=f"{len(area_codes)} items", reason="maximum 4 area codes allowed"
        )

    for area_code in area_codes:
        if area_code and not is_valid_area_code(area_code, db):
            raise invalid_area_code(area_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Use materialized views instead of SQL files
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    view_name = "price_ratios_usd" if element_code == "5532" else "price_ratios_lcu"
    view = table(
        view_name,
        column("country1"),
        column("country2"),
        column("country1_id"),
        column("country2_id"),
        column("country1_code"),
        column("country2_code"),
        column("item_code"),
        column("time_series"),
        column("years_compared"),
        column("avg_ratio"),
        column("ratio_volatility"),
        column("min_ratio"),
        column("max_ratio"),
        column("integration_level"),
    )

    # Build the query - SQLAlchemy handles parameterization
    query = select(view).where(
        and_(view.c.item_code == item_code, view.c.country1_code.in_(area_codes), view.c.country2_code.in_(area_codes))
    )

    # Execute
    results = db.execute(query).mappings().all()

    if not results:
        return {
            "item": {"code": item_code, "name": "Unknown"},
            "analysis_period": {
                "start_year": year_start,
                "end_year": None,
            },
            "countries_analyzed": len(area_codes),
            "comparisons_count": 0,
            "comparisons": [],
        }

    comparisons = []
    for row in results:
        # Filter time series to only include years >= year_start
        filtered_time_series = [point for point in row["time_series"] if point["year"] >= year_start]

        # Skip if no data after filtering
        if not filtered_time_series:
            continue

        metrics = {
            "years_compared": len(filtered_time_series),  # Recalculate after filtering
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
                "calculated_metrics": calculate_price_correlation(filtered_time_series, metrics),
                "time_series": filtered_time_series,
            }
        )

    # Get item details
    item_info = db.query(ItemCodes).filter(ItemCodes.item_code == item_code).first()

    return {
        "element_code": element_code,
        "item": {"code": item_code, "name": item_info.item if item_info else "Unknown"},
        "analysis_period": {
            "start_year": year_start,
            "end_year": (
                max(max(point["year"] for point in comp["time_series"]) for comp in comparisons)
                if comparisons
                else None
            ),
        },
        "countries_analyzed": len(area_codes),
        "comparisons_count": len(comparisons),
        "comparisons": comparisons,
    }


@router.get("/comparison")
def get_multi_line_price_trends(
    item_code: str = Query(None, description="Item FAO code (2-4 digits)"),
    area_codes: List[str] = Query(None, description="List of up to 5 area FAO codes"),
    element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
    year_start: int = Query(1990, description="Start year"),
    year_end: int = Query(2023, description="End year"),
    db: Session = Depends(get_db),
):
    """
    Get price trend data for multi-line chart visualization
    Returns data optimized for D3.js multi-line charts
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

    if len(area_codes) > 5:
        raise invalid_parameter(
            param="area_codes", value=f"{len(area_codes)} items", reason="maximum 5 area codes allowed"
        )

    for area_code in area_codes:
        if area_code and not is_valid_area_code(area_code, db):
            raise invalid_area_code(area_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Element Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not element_code:
        raise missing_parameter("element_code")

    if element_code and not is_valid_element_code(element_code, db):
        raise invalid_element_code(element_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Year validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not is_valid_year_range(year_start, year_end):
        raise invalid_parameter(
            param="year_range", value=f"{year_start}-{year_end}", reason="start year must be before end year"
        )

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Use materialized views
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    from sqlalchemy import table, column, select, and_

    # Choose the appropriate view
    view_name = "price_details_usd" if element_code == "5532" else "price_details_lcu"

    # Create table reference
    view = table(
        view_name,
        column("area_id"),
        column("area_name"),
        column("area_code"),
        column("year"),
        column("price"),
        column("unit"),
        column("item_name"),
        column("item_code"),
        column("item_id"),
    )

    # Build query
    query = (
        select(view)
        .where(
            and_(
                view.c.item_code == str(item_code),
                view.c.area_code.in_(area_codes),
                view.c.year >= year_start,
                view.c.year <= year_end,
            )
        )
        .order_by(view.c.area_name, view.c.year)
    )

    # Execute query
    results = db.execute(query).mappings().all()

    if not results:
        raise no_data_found(
            dataset="prices",
            filters={
                "item_code": item_code,
                "area_codes": area_codes,
                "element_code": element_code,
                "year_range": f"{year_start}-{year_end}",
            },
        )

    # Group data by area for D3.js consumption
    data_by_area = {}
    item_info = None
    units = set()

    for row in results:
        area_name = row["area_name"]

        # Store item info (should be same for all rows)
        if item_info is None:
            item_info = {"name": row["item_name"], "code": item_code}

        # Track units
        units.add(row["unit"])

        # Group by area
        if area_name not in data_by_area:
            data_by_area[area_name] = {
                "area_id": row["area_id"],
                "area_name": area_name,
                "area_code": row["area_code"],
                "currency": row["unit"],
                "data_points": [],
            }

        data_by_area[area_name]["data_points"].append(
            {
                "year": row["year"],
                "price_per_t": row["price"],
                "price_per_kg": round(row["price"] / 1000, 4) if row["price"] else None,
                "price_per_lb": round(row["price"] / 2204.6, 4) if row["price"] else None,
            }
        )

    # Convert to list format that D3 likes
    lines_data = list(data_by_area.values())

    # Calculate some summary stats for the frontend
    all_prices = [
        point["price_per_t"] for area_data in lines_data for point in area_data["data_points"] if point["price_per_t"]
    ]
    all_years = [point["year"] for area_data in lines_data for point in area_data["data_points"]]

    summary = {
        "min_price": min(all_prices) if all_prices else 0,
        "max_price": max(all_prices) if all_prices else 0,
        "min_year": min(all_years) if all_years else year_start,
        "max_year": max(all_years) if all_years else year_end,
        "areas_found": len(lines_data),
        "total_data_points": len(all_prices),
    }

    return {
        "item": item_info,
        "parameters": {
            "element_code": element_code,
            "requested_areas": area_codes,
            "year_start": year_start,
            "year_end": year_end,
        },
        "lines": lines_data,
        "summary": summary,
        "currencies": list(units),
        "quantities": (
            "Prices are in USD per metric ton"
            if element_code == "5532"
            else "Prices are converted from LCU to USD per metric ton"
        ),
        "note": "",
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

    # Determine which view to use based on element code
    if element_code == "5532":
        view_name = "item_stats_usd"
    elif element_code == "5530":
        view_name = "item_stats_lcu"
    else:
        raise invalid_parameter(
            param="element_code", value=element_code, reason="Unsupported element code. Use 5530 or 5532."
        )

    # Simple query from the view
    query = text(f"SELECT * FROM {view_name}")

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
    start_year: int = Query(START_YEAR, description="Start year"),
    element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
    db: Session = Depends(get_db),
):
    """
    Get list of countries that have price data for a specific item in the given time range.
    Useful for the frontend to show available options.
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Item Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not item_code:
        raise missing_parameter("item_code")

    if not is_valid_item_code(item_code, db):
        raise invalid_item_code(item_code)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Element Code validation
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    if not element_code:
        raise missing_parameter("element_code")

    if element_code and not is_valid_element_code(element_code, db):
        raise invalid_element_code(element_code)

    # Load SQL query
    query_sql = load_sql("sql/available_countries_for_item.sql", Path(__file__).parent)

    params = {"item_code": item_code, "element_code": element_code, "start_year": 1990}

    results = db.execute(text(query_sql), params).mappings().all()

    return {
        "item_code": item_code,
        "start_year": start_year,
        "countries": [dict(row) for row in results],
        "total_countries": len(results),
    }
