from typing import List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, func
from ....core.validation import (
    is_valid_area_code,
    is_valid_item_code,
    is_valid_element_code,
    is_valid_year_range,
)
from ....core.exceptions import (
    missing_parameter,
    invalid_parameter,
    invalid_area_code,
    invalid_item_code,
    invalid_element_code,
    no_data_found,
)

# Correct imports for FAO API project
from fao.src.db.database import get_db
from fao.src.core import settings
from fao.src.db.pipelines.prices.prices_model import Prices
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix=f"/{settings.api_version_prefix}/prices/multi-line",
    tags=["prices", "analytics", "visualizations", "multi-line"],
)


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


@router.get("/price-data")
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

    # Build the query
    query = (
        select(
            AreaCodes.area.label("area_name"),
            AreaCodes.area_code.label("area_code"),
            Prices.year,
            Prices.value.label("price"),
            Prices.unit,  # Using unit instead of currency
            ItemCodes.item.label("item_name"),
        )
        .join(ItemCodes, ItemCodes.id == Prices.item_code_id)
        .join(AreaCodes, AreaCodes.id == Prices.area_code_id)
        .join(Elements, Elements.id == Prices.element_code_id)
        .join(Flags, Flags.id == Prices.flag_id)
        .where(
            and_(
                Prices.year >= year_start,
                Prices.year <= year_end,
                ItemCodes.item_code == str(item_code),
                Elements.element_code == element_code,
                Flags.flag == "A",
            )
        )
    )

    # Filter by area_codes (FAO codes)
    area_conditions = []
    for area_code in area_codes:
        area_conditions.append(AreaCodes.area_code == str(area_code))

    query = query.where(or_(*area_conditions))

    # Order by area and year for clean data structure
    query = query.order_by(AreaCodes.area, Prices.year)

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

        # Track units (instead of currencies for now)
        units.add(row["unit"])

        # Group by area
        if area_name not in data_by_area:
            data_by_area[area_name] = {
                "area_name": area_name,
                "area_code": row["area_code"],
                "currency": row["unit"],  # Using unit for now, will map to currency later
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
            "requested_areas": area_codes,
            "year_start": year_start,
            "year_end": year_end,
        },
        "lines": lines_data,
        "summary": summary,
        "currencies": list(units),  # Will be units for now
        "quantities": "Prices are likely in USD per metric ton",
        "note": "Prices may show currency transitions/redenominations. Use caution when comparing across years.",
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
            func.count(Prices.id).label("price_points"),
        )
        .join(Prices, ItemCodes.id == Prices.item_code_id)
        .join(Elements, Elements.id == Prices.element_code_id)
        .join(Flags, Flags.id == Prices.flag_id)
        .where(and_(Elements.element_code == element_code, ItemCodes.source_dataset == "prices", Flags.flag == "A"))
        .group_by(ItemCodes.id, ItemCodes.item, ItemCodes.item_code, ItemCodes.item_code_cpc)
        .order_by(func.count(Prices.id).desc(), ItemCodes.item)
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


@router.get("/available-areas")
def get_available_data_for_item(
    item_code: str = Query(..., description="Item FAO code"),
    element_code: str = Query(PRICE_ELEMENT_CODE, description="Element code for price data"),
    db: Session = Depends(get_db),
):
    """
    Get what countries and years have data for this item
    Useful for frontend to show available options
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

    query = (
        select(
            AreaCodes.area.label("name"),
            AreaCodes.area_code.label("area_code"),
            func.min(Prices.year).label("earliest_year"),
            func.max(Prices.year).label("latest_year"),
            func.count(Prices.year).label("data_points"),
            Prices.unit,  # Using unit instead of currency for now
        )
        .join(ItemCodes, ItemCodes.id == Prices.item_code_id)
        .join(AreaCodes, AreaCodes.id == Prices.area_code_id)
        .join(Elements, Elements.id == Prices.element_code_id)
        .join(Flags, Flags.id == Prices.flag_id)
        .where(
            and_(
                ItemCodes.item_code == str(item_code),
                ItemCodes.source_dataset == "prices",
                Elements.element_code == element_code,
                Flags.flag == "A",
            )
        )
        .group_by(AreaCodes.area, AreaCodes.area_code, Prices.unit)
        .order_by(func.count(Prices.year).desc())
    )

    results = db.execute(query).mappings().all()

    if not results:
        raise no_data_found(
            dataset="area_codes",
            filters={
                "item_code": item_code,
                "element_code": element_code,
            },
        )

    return {
        "item_code": item_code,
        "available_areas": [dict(row) for row in results],
        "total_areas": len(results),
    }
