from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.employment_indicators_agriculture.employment_indicators_agriculture_model import EmploymentIndicatorsAgriculture
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.sources.sources_model import Sources
from fao.src.db.pipelines.indicators.indicators_model import Indicators
from fao.src.db.pipelines.sexs.sexs_model import Sexs
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/employment_indicators_agriculture",
    tags=["employment_indicators_agriculture"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_employment_indicators_agriculture(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    area_code: Optional[str] = Query(None, description="Filter by area_codes code"),
    area: Optional[str] = Query(None, description="Filter by area_codes description"),
    source_code: Optional[str] = Query(None, description="Filter by sources code"),
    source: Optional[str] = Query(None, description="Filter by sources description"),
    indicator_code: Optional[str] = Query(None, description="Filter by indicators code"),
    indicator: Optional[str] = Query(None, description="Filter by indicators description"),
    sex_code: Optional[str] = Query(None, description="Filter by sexs code"),
    sex: Optional[str] = Query(None, description="Filter by sexs description"),
    element_code: Optional[str] = Query(None, description="Filter by elements code"),
    element: Optional[str] = Query(None, description="Filter by elements description"),
    flag: Optional[str] = Query(None, description="Filter by flags code"),
    description: Optional[str] = Query(None, description="Filter by flags description"),
    db: Session = Depends(get_db)
):
    """
    employment indicators agriculture data with filters.
    Filter options:
    - area_code: Filter by area_codes code
    - area: Filter by area_codes description (partial match)
    - source_code: Filter by sources code
    - source: Filter by sources description (partial match)
    - indicator_code: Filter by indicators code
    - indicator: Filter by indicators description (partial match)
    - sex_code: Filter by sexs code
    - sex: Filter by sexs description (partial match)
    - element_code: Filter by elements code
    - element: Filter by elements description (partial match)
    - flag: Filter by flags code
    - description: Filter by flags description (partial match)
    """
    
    query = (
        select(
            EmploymentIndicatorsAgriculture,
            AreaCodes.area_code.label("area_codes_code"),
            AreaCodes.area.label("area_codes_desc"),
            Sources.source_code.label("sources_code"),
            Sources.source.label("sources_desc"),
            Indicators.indicator_code.label("indicators_code"),
            Indicators.indicator.label("indicators_desc"),
            Sexs.sex_code.label("sexs_code"),
            Sexs.sex.label("sexs_desc"),
            Elements.element_code.label("elements_code"),
            Elements.element.label("elements_desc"),
            Flags.flag.label("flags_code"),
            Flags.description.label("flags_desc"),
        )
        .select_from(EmploymentIndicatorsAgriculture)
        .outerjoin(AreaCodes, EmploymentIndicatorsAgriculture.area_code_id == AreaCodes.id)
        .outerjoin(Sources, EmploymentIndicatorsAgriculture.source_code_id == Sources.id)
        .outerjoin(Indicators, EmploymentIndicatorsAgriculture.indicator_code_id == Indicators.id)
        .outerjoin(Sexs, EmploymentIndicatorsAgriculture.sex_code_id == Sexs.id)
        .outerjoin(Elements, EmploymentIndicatorsAgriculture.element_code_id == Elements.id)
        .outerjoin(Flags, EmploymentIndicatorsAgriculture.flag_id == Flags.id)
    )
    
    # Apply filters
    if area_code:
        query = query.where(AreaCodes.area_code == area_code)
    if area:
        query = query.where(AreaCodes.area.ilike("%" + area + "%"))
    if source_code:
        query = query.where(Sources.source_code == source_code)
    if source:
        query = query.where(Sources.source.ilike("%" + source + "%"))
    if indicator_code:
        query = query.where(Indicators.indicator_code == indicator_code)
    if indicator:
        query = query.where(Indicators.indicator.ilike("%" + indicator + "%"))
    if sex_code:
        query = query.where(Sexs.sex_code == sex_code)
    if sex:
        query = query.where(Sexs.sex.ilike("%" + sex + "%"))
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
            func.count(EmploymentIndicatorsAgriculture.id).label('record_count')
        )
        .join(EmploymentIndicatorsAgriculture, AreaCodes.id == EmploymentIndicatorsAgriculture.area_code_id)
        .group_by(AreaCodes.area_code, AreaCodes.area)
        .order_by(func.count(EmploymentIndicatorsAgriculture.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "employment_indicators_agriculture",
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
            func.count(EmploymentIndicatorsAgriculture.id).label('record_count')
        )
        .join(EmploymentIndicatorsAgriculture, Elements.id == EmploymentIndicatorsAgriculture.element_code_id)
        .group_by(Elements.element_code, Elements.element)
        .order_by(func.count(EmploymentIndicatorsAgriculture.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "employment_indicators_agriculture",
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
            func.count(EmploymentIndicatorsAgriculture.id).label('record_count')
        )
        .join(EmploymentIndicatorsAgriculture, Flags.id == EmploymentIndicatorsAgriculture.flag_id)
        .group_by(Flags.flag, Flags.description)
        .order_by(func.count(EmploymentIndicatorsAgriculture.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "employment_indicators_agriculture",
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
            EmploymentIndicatorsAgriculture.year,
            func.count(EmploymentIndicatorsAgriculture.id).label('record_count')
        )
        .group_by(EmploymentIndicatorsAgriculture.year)
        .order_by(EmploymentIndicatorsAgriculture.year)
    )
    
    results = db.execute(query).all()
    years_data = [{"year": r.year, "record_count": r.record_count} for r in results]
    
    if not years_data:
        return {"dataset": "employment_indicators_agriculture", "message": "No temporal data available"}
    
    return {
        "dataset": "employment_indicators_agriculture",
        "earliest_year": min(r["year"] for r in years_data),
        "latest_year": max(r["year"] for r in years_data),
        "total_years": len(years_data),
        "total_records": sum(r["record_count"] for r in years_data),
        "years": years_data
    }

@router.get("/summary")
def get_dataset_summary(db: Session = Depends(get_db)):
    """Get comprehensive summary of this dataset"""
    total_records = db.query(func.count(EmploymentIndicatorsAgriculture.id)).scalar()
    
    summary = {
        "dataset": "employment_indicators_agriculture",
        "total_records": total_records,
        "foreign_keys": [
            "area_codes",
            "sources",
            "indicators",
            "sexs",
            "elements",
            "flags",
        ]
    }
    
    # Add counts for each FK relationship
    summary["unique_areas"] = db.query(func.count(func.distinct(EmploymentIndicatorsAgriculture.area_code_id))).scalar()
    summary["unique_elements"] = db.query(func.count(func.distinct(EmploymentIndicatorsAgriculture.element_code_id))).scalar()
    
    return summary
