from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.exchange_rate.exchange_rate_model import ExchangeRate
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.currencies.currencies_model import Currencies
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/exchange_rate",
    tags=["exchange_rate"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_exchange_rate(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    area_code: Optional[str] = Query(None, description="Filter by area_codes code"),
    area: Optional[str] = Query(None, description="Filter by area_codes description"),
    element_code: Optional[str] = Query(None, description="Filter by elements code"),
    element: Optional[str] = Query(None, description="Filter by elements description"),
    iso_currency_code: Optional[str] = Query(None, description="Filter by currencies code"),
    currency: Optional[str] = Query(None, description="Filter by currencies description"),
    flag: Optional[str] = Query(None, description="Filter by flags code"),
    description: Optional[str] = Query(None, description="Filter by flags description"),
    db: Session = Depends(get_db)
):
    """
    exchange rate data with filters.
    Filter options:
    - area_code: Filter by area_codes code
    - area: Filter by area_codes description (partial match)
    - element_code: Filter by elements code
    - element: Filter by elements description (partial match)
    - iso_currency_code: Filter by currencies code
    - currency: Filter by currencies description (partial match)
    - flag: Filter by flags code
    - description: Filter by flags description (partial match)
    """
    
    query = (
        select(
            ExchangeRate,
            AreaCodes.area_code.label("area_codes_code"),
            AreaCodes.area.label("area_codes_desc"),
            Elements.element_code.label("elements_code"),
            Elements.element.label("elements_desc"),
            Currencies.iso_currency_code.label("currencies_code"),
            Currencies.currency.label("currencies_desc"),
            Flags.flag.label("flags_code"),
            Flags.description.label("flags_desc"),
        )
        .select_from(ExchangeRate)
        .outerjoin(AreaCodes, ExchangeRate.area_code_id == AreaCodes.id)
        .outerjoin(Elements, ExchangeRate.element_code_id == Elements.id)
        .outerjoin(Currencies, ExchangeRate.iso_currency_code_id == Currencies.id)
        .outerjoin(Flags, ExchangeRate.flag_id == Flags.id)
    )
    
    # Apply filters
    if area_code:
        query = query.where(AreaCodes.area_code == area_code)
    if area:
        query = query.where(AreaCodes.area.ilike("%" + area + "%"))
    if element_code:
        query = query.where(Elements.element_code == element_code)
    if element:
        query = query.where(Elements.element.ilike("%" + element + "%"))
    if iso_currency_code:
        query = query.where(Currencies.iso_currency_code == iso_currency_code)
    if currency:
        query = query.where(Currencies.currency.ilike("%" + currency + "%"))
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
            func.count(ExchangeRate.id).label('record_count')
        )
        .join(ExchangeRate, AreaCodes.id == ExchangeRate.area_code_id)
        .group_by(AreaCodes.area_code, AreaCodes.area)
        .order_by(func.count(ExchangeRate.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "exchange_rate",
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
            func.count(ExchangeRate.id).label('record_count')
        )
        .join(ExchangeRate, Elements.id == ExchangeRate.element_code_id)
        .group_by(Elements.element_code, Elements.element)
        .order_by(func.count(ExchangeRate.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "exchange_rate",
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
            func.count(ExchangeRate.id).label('record_count')
        )
        .join(ExchangeRate, Flags.id == ExchangeRate.flag_id)
        .group_by(Flags.flag, Flags.description)
        .order_by(func.count(ExchangeRate.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "exchange_rate",
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
            ExchangeRate.year,
            func.count(ExchangeRate.id).label('record_count')
        )
        .group_by(ExchangeRate.year)
        .order_by(ExchangeRate.year)
    )
    
    results = db.execute(query).all()
    years_data = [{"year": r.year, "record_count": r.record_count} for r in results]
    
    if not years_data:
        return {"dataset": "exchange_rate", "message": "No temporal data available"}
    
    return {
        "dataset": "exchange_rate",
        "earliest_year": min(r["year"] for r in years_data),
        "latest_year": max(r["year"] for r in years_data),
        "total_years": len(years_data),
        "total_records": sum(r["record_count"] for r in years_data),
        "years": years_data
    }

@router.get("/summary")
def get_dataset_summary(db: Session = Depends(get_db)):
    """Get comprehensive summary of this dataset"""
    total_records = db.query(func.count(ExchangeRate.id)).scalar()
    
    summary = {
        "dataset": "exchange_rate",
        "total_records": total_records,
        "foreign_keys": [
            "area_codes",
            "elements",
            "currencies",
            "flags",
        ]
    }
    
    # Add counts for each FK relationship
    summary["unique_areas"] = db.query(func.count(func.distinct(ExchangeRate.area_code_id))).scalar()
    summary["unique_elements"] = db.query(func.count(func.distinct(ExchangeRate.element_code_id))).scalar()
    
    return summary
