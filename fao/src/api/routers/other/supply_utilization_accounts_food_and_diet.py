from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.supply_utilization_accounts_food_and_diet.supply_utilization_accounts_food_and_diet_model import SupplyUtilizationAccountsFoodAndDiet
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.food_groups.food_groups_model import FoodGroups
from fao.src.db.pipelines.indicators.indicators_model import Indicators
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/supply_utilization_accounts_food_and_diet",
    tags=["supply_utilization_accounts_food_and_diet"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_supply_utilization_accounts_food_and_diet(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    area_code: Optional[str] = Query(None, description="Filter by area_codes code"),
    area: Optional[str] = Query(None, description="Filter by area_codes description"),
    food_group_code: Optional[str] = Query(None, description="Filter by food_groups code"),
    food_group: Optional[str] = Query(None, description="Filter by food_groups description"),
    indicator_code: Optional[str] = Query(None, description="Filter by indicators code"),
    indicator: Optional[str] = Query(None, description="Filter by indicators description"),
    element_code: Optional[str] = Query(None, description="Filter by elements code"),
    element: Optional[str] = Query(None, description="Filter by elements description"),
    flag: Optional[str] = Query(None, description="Filter by flags code"),
    description: Optional[str] = Query(None, description="Filter by flags description"),
    db: Session = Depends(get_db)
):
    """
    supply utilization accounts food and diet data with filters.
    Filter options:
    - area_code: Filter by area_codes code
    - area: Filter by area_codes description (partial match)
    - food_group_code: Filter by food_groups code
    - food_group: Filter by food_groups description (partial match)
    - indicator_code: Filter by indicators code
    - indicator: Filter by indicators description (partial match)
    - element_code: Filter by elements code
    - element: Filter by elements description (partial match)
    - flag: Filter by flags code
    - description: Filter by flags description (partial match)
    """
    
    query = (
        select(
            SupplyUtilizationAccountsFoodAndDiet,
            AreaCodes.area_code.label("area_codes_code"),
            AreaCodes.area.label("area_codes_desc"),
            FoodGroups.food_group_code.label("food_groups_code"),
            FoodGroups.food_group.label("food_groups_desc"),
            Indicators.indicator_code.label("indicators_code"),
            Indicators.indicator.label("indicators_desc"),
            Elements.element_code.label("elements_code"),
            Elements.element.label("elements_desc"),
            Flags.flag.label("flags_code"),
            Flags.description.label("flags_desc"),
        )
        .select_from(SupplyUtilizationAccountsFoodAndDiet)
        .outerjoin(AreaCodes, SupplyUtilizationAccountsFoodAndDiet.area_code_id == AreaCodes.id)
        .outerjoin(FoodGroups, SupplyUtilizationAccountsFoodAndDiet.food_group_code_id == FoodGroups.id)
        .outerjoin(Indicators, SupplyUtilizationAccountsFoodAndDiet.indicator_code_id == Indicators.id)
        .outerjoin(Elements, SupplyUtilizationAccountsFoodAndDiet.element_code_id == Elements.id)
        .outerjoin(Flags, SupplyUtilizationAccountsFoodAndDiet.flag_id == Flags.id)
    )
    
    # Apply filters
    if area_code:
        query = query.where(AreaCodes.area_code == area_code)
    if area:
        query = query.where(AreaCodes.area.ilike("%" + area + "%"))
    if food_group_code:
        query = query.where(FoodGroups.food_group_code == food_group_code)
    if food_group:
        query = query.where(FoodGroups.food_group.ilike("%" + food_group + "%"))
    if indicator_code:
        query = query.where(Indicators.indicator_code == indicator_code)
    if indicator:
        query = query.where(Indicators.indicator.ilike("%" + indicator + "%"))
    if element_code:
        query = query.where(Elements.element_code == element_code)
    if element:
        query = query.where(Elements.element.ilike("%" + element + "%"))
    if flag:
        query = query.where(Flags.flag == flag)
    if description:
        query = query.where(Flags.description.ilike("%" + description + "%"))
   
    
    # Get total count (with filters)
    total_count = db.execute(select(func.count()).select_from(query.subquery())).scalar()
    
    # Paginate and execute
    query = query.offset(offset).limit(limit)
    results = db.execute(query).mappings().all()
    
    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data": [dict(row) for row in results]
    }

# Metadata endpoints for understanding the dataset

@router.get("/areas")
def get_available_areas(db: Session = Depends(get_db)):
    """Get all areas with data in this dataset"""
    query = (
        select(
            AreaCodes.area_code,
            AreaCodes.area,
            func.count(SupplyUtilizationAccountsFoodAndDiet.id).label('record_count')
        )
        .join(SupplyUtilizationAccountsFoodAndDiet, AreaCodes.id == SupplyUtilizationAccountsFoodAndDiet.area_code_id)
        .group_by(AreaCodes.area_code, AreaCodes.area)
        .order_by(func.count(SupplyUtilizationAccountsFoodAndDiet.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "supply_utilization_accounts_food_and_diet",
        "total_areas": len(results),
        "areas": [
            {
                "area_code": r.area_code,
                "area": r.area,
                "record_count": r.record_count
            }
            for r in results
        ]
    }










@router.get("/elements")
def get_available_elements(db: Session = Depends(get_db)):
    """Get all elements (measures/indicators) in this dataset"""
    query = (
        select(
            Elements.element_code,
            Elements.element,
            func.count(SupplyUtilizationAccountsFoodAndDiet.id).label('record_count')
        )
        .join(SupplyUtilizationAccountsFoodAndDiet, Elements.id == SupplyUtilizationAccountsFoodAndDiet.element_code_id)
        .group_by(Elements.element_code, Elements.element)
        .order_by(func.count(SupplyUtilizationAccountsFoodAndDiet.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "supply_utilization_accounts_food_and_diet",
        "total_elements": len(results),
        "elements": [
            {
                "element_code": r.element_code,
                "element": r.element,
                "record_count": r.record_count
            }
            for r in results
        ]
    }




@router.get("/flags")
def get_data_quality_summary(db: Session = Depends(get_db)):
    """Get data quality flag distribution for this dataset"""
    query = (
        select(
            Flags.flag,
            Flags.description,
            func.count(SupplyUtilizationAccountsFoodAndDiet.id).label('record_count')
        )
        .join(SupplyUtilizationAccountsFoodAndDiet, Flags.id == SupplyUtilizationAccountsFoodAndDiet.flag_id)
        .group_by(Flags.flag, Flags.description)
        .order_by(func.count(SupplyUtilizationAccountsFoodAndDiet.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "supply_utilization_accounts_food_and_diet",
        "total_records": sum(r.record_count for r in results),
        "flag_distribution": [
            {
                "flag": r.flag,
                "description": r.description,
                "record_count": r.record_count,
                "percentage": round(r.record_count / sum(r2.record_count for r2 in results) * 100, 2)
            }
            for r in results
        ]
    }

@router.get("/years")
def get_temporal_coverage(db: Session = Depends(get_db)):
    """Get temporal coverage information for this dataset"""
    # Get year range and counts
    query = (
        select(
            SupplyUtilizationAccountsFoodAndDiet.year,
            func.count(SupplyUtilizationAccountsFoodAndDiet.id).label('record_count')
        )
        .group_by(SupplyUtilizationAccountsFoodAndDiet.year)
        .order_by(SupplyUtilizationAccountsFoodAndDiet.year)
    )
    
    results = db.execute(query).all()
    years_data = [{"year": r.year, "record_count": r.record_count} for r in results]
    
    if not years_data:
        return {"dataset": "supply_utilization_accounts_food_and_diet", "message": "No temporal data available"}
    
    return {
        "dataset": "supply_utilization_accounts_food_and_diet",
        "earliest_year": min(r["year"] for r in years_data),
        "latest_year": max(r["year"] for r in years_data),
        "total_years": len(years_data),
        "total_records": sum(r["record_count"] for r in years_data),
        "years": years_data
    }

@router.get("/summary")
def get_dataset_summary(db: Session = Depends(get_db)):
    """Get comprehensive summary of this dataset"""
    total_records = db.query(func.count(SupplyUtilizationAccountsFoodAndDiet.id)).scalar()
    
    summary = {
        "dataset": "supply_utilization_accounts_food_and_diet",
        "total_records": total_records,
        "foreign_keys": [
            "area_codes",
            "food_groups",
            "indicators",
            "elements",
            "flags",
        ]
    }
    
    # Add counts for each FK relationship
    summary["unique_areas"] = db.query(func.count(func.distinct(SupplyUtilizationAccountsFoodAndDiet.area_code_id))).scalar()
    summary["unique_elements"] = db.query(func.count(func.distinct(SupplyUtilizationAccountsFoodAndDiet.element_code_id))).scalar()
    
    return summary
