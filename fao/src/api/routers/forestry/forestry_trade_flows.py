from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.forestry_trade_flows.forestry_trade_flows_model import ForestryTradeFlows
# Import core/lookup tables for joins
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/forestry_trade_flows",
    tags=["forestry_trade_flows"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_forestry_trade_flows(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    item_code: Optional[str] = Query(None, description="Filter by item_codes code"),
    item: Optional[str] = Query(None, description="Filter by item_codes description"),
    element_code: Optional[str] = Query(None, description="Filter by elements code"),
    element: Optional[str] = Query(None, description="Filter by elements description"),
    flag: Optional[str] = Query(None, description="Filter by flags code"),
    description: Optional[str] = Query(None, description="Filter by flags description"),
    db: Session = Depends(get_db)
):
    """
    forestry trade flows data with filters.
    Filter options:
    - item_code: Filter by item_codes code
    - item: Filter by item_codes description (partial match)
    - element_code: Filter by elements code
    - element: Filter by elements description (partial match)
    - flag: Filter by flags code
    - description: Filter by flags description (partial match)
    """
    
    query = (
        select(
            ForestryTradeFlows,
            ItemCodes.item_code.label("item_codes_code"),
            ItemCodes.item.label("item_codes_desc"),
            Elements.element_code.label("elements_code"),
            Elements.element.label("elements_desc"),
            Flags.flag.label("flags_code"),
            Flags.description.label("flags_desc"),
        )
        .select_from(ForestryTradeFlows)
        .outerjoin(ItemCodes, ForestryTradeFlows.item_code_id == ItemCodes.id)
        .outerjoin(Elements, ForestryTradeFlows.element_code_id == Elements.id)
        .outerjoin(Flags, ForestryTradeFlows.flag_id == Flags.id)
    )
    
    # Apply filters
    if item_code:
        query = query.where(ItemCodes.item_code == item_code)
    if item:
        query = query.where(ItemCodes.item.ilike("%" + item + "%"))
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
@router.get("/items")
def get_available_items(db: Session = Depends(get_db)):
    """Get all items available in this dataset with record counts"""
    query = (
        select(
            ItemCodes.item_code,
            ItemCodes.item,
            func.count(ForestryTradeFlows.id).label('record_count')
        )
        .join(ForestryTradeFlows, ItemCodes.id == ForestryTradeFlows.item_code_id)
        .group_by(ItemCodes.item_code, ItemCodes.item)
        .order_by(func.count(ForestryTradeFlows.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "forestry_trade_flows",
        "total_items": len(results),
        "items": [
            {
                "item_code": r.item_code,
                "item": r.item,
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
            func.count(ForestryTradeFlows.id).label('record_count')
        )
        .join(ForestryTradeFlows, Elements.id == ForestryTradeFlows.element_code_id)
        .group_by(Elements.element_code, Elements.element)
        .order_by(func.count(ForestryTradeFlows.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "forestry_trade_flows",
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
            func.count(ForestryTradeFlows.id).label('record_count')
        )
        .join(ForestryTradeFlows, Flags.id == ForestryTradeFlows.flag_id)
        .group_by(Flags.flag, Flags.description)
        .order_by(func.count(ForestryTradeFlows.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "forestry_trade_flows",
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
            ForestryTradeFlows.year,
            func.count(ForestryTradeFlows.id).label('record_count')
        )
        .group_by(ForestryTradeFlows.year)
        .order_by(ForestryTradeFlows.year)
    )
    
    results = db.execute(query).all()
    years_data = [{"year": r.year, "record_count": r.record_count} for r in results]
    
    if not years_data:
        return {"dataset": "forestry_trade_flows", "message": "No temporal data available"}
    
    return {
        "dataset": "forestry_trade_flows",
        "earliest_year": min(r["year"] for r in years_data),
        "latest_year": max(r["year"] for r in years_data),
        "total_years": len(years_data),
        "total_records": sum(r["record_count"] for r in years_data),
        "years": years_data
    }

@router.get("/summary")
def get_dataset_summary(db: Session = Depends(get_db)):
    """Get comprehensive summary of this dataset"""
    total_records = db.query(func.count(ForestryTradeFlows.id)).scalar()
    
    summary = {
        "dataset": "forestry_trade_flows",
        "total_records": total_records,
        "foreign_keys": [
            "item_codes",
            "elements",
            "flags",
        ]
    }
    
    # Add counts for each FK relationship
    summary["unique_items"] = db.query(func.count(func.distinct(ForestryTradeFlows.item_code_id))).scalar()
    summary["unique_elements"] = db.query(func.count(func.distinct(ForestryTradeFlows.element_code_id))).scalar()
    
    return summary
