from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.indicators_from_household_surveys.indicators_from_household_surveys_model import IndicatorsFromHouseholdSurveys
# Import core/lookup tables for joins
from fao.src.db.pipelines.surveys.surveys_model import Surveys
from fao.src.db.pipelines.indicators.indicators_model import Indicators
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/indicators_from_household_surveys",
    tags=["indicators_from_household_surveys"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_indicators_from_household_surveys(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    survey_code: Optional[str] = Query(None, description="Filter by surveys code"),
    survey: Optional[str] = Query(None, description="Filter by surveys description"),
    indicator_code: Optional[str] = Query(None, description="Filter by indicators code"),
    indicator: Optional[str] = Query(None, description="Filter by indicators description"),
    element_code: Optional[str] = Query(None, description="Filter by elements code"),
    element: Optional[str] = Query(None, description="Filter by elements description"),
    flag: Optional[str] = Query(None, description="Filter by flags code"),
    description: Optional[str] = Query(None, description="Filter by flags description"),
    db: Session = Depends(get_db)
):
    """
    indicators from household surveys data with filters.
    Filter options:
    - survey_code: Filter by surveys code
    - survey: Filter by surveys description (partial match)
    - indicator_code: Filter by indicators code
    - indicator: Filter by indicators description (partial match)
    - element_code: Filter by elements code
    - element: Filter by elements description (partial match)
    - flag: Filter by flags code
    - description: Filter by flags description (partial match)
    """
    
    query = (
        select(
            IndicatorsFromHouseholdSurveys,
            Surveys.survey_code.label("surveys_code"),
            Surveys.survey.label("surveys_desc"),
            Indicators.indicator_code.label("indicators_code"),
            Indicators.indicator.label("indicators_desc"),
            Elements.element_code.label("elements_code"),
            Elements.element.label("elements_desc"),
            Flags.flag.label("flags_code"),
            Flags.description.label("flags_desc"),
        )
        .select_from(IndicatorsFromHouseholdSurveys)
        .outerjoin(Surveys, IndicatorsFromHouseholdSurveys.survey_code_id == Surveys.id)
        .outerjoin(Indicators, IndicatorsFromHouseholdSurveys.indicator_code_id == Indicators.id)
        .outerjoin(Elements, IndicatorsFromHouseholdSurveys.element_code_id == Elements.id)
        .outerjoin(Flags, IndicatorsFromHouseholdSurveys.flag_id == Flags.id)
    )
    
    # Apply filters
    if survey_code:
        query = query.where(Surveys.survey_code == survey_code)
    if survey:
        query = query.where(Surveys.survey.ilike("%" + survey + "%"))
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








@router.get("/elements")
def get_available_elements(db: Session = Depends(get_db)):
    """Get all elements (measures/indicators) in this dataset"""
    query = (
        select(
            Elements.element_code,
            Elements.element,
            func.count(IndicatorsFromHouseholdSurveys.id).label('record_count')
        )
        .join(IndicatorsFromHouseholdSurveys, Elements.id == IndicatorsFromHouseholdSurveys.element_code_id)
        .group_by(Elements.element_code, Elements.element)
        .order_by(func.count(IndicatorsFromHouseholdSurveys.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "indicators_from_household_surveys",
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
            func.count(IndicatorsFromHouseholdSurveys.id).label('record_count')
        )
        .join(IndicatorsFromHouseholdSurveys, Flags.id == IndicatorsFromHouseholdSurveys.flag_id)
        .group_by(Flags.flag, Flags.description)
        .order_by(func.count(IndicatorsFromHouseholdSurveys.id).desc())
    )
    
    results = db.execute(query).all()
    
    return {
        "dataset": "indicators_from_household_surveys",
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
            IndicatorsFromHouseholdSurveys.year,
            func.count(IndicatorsFromHouseholdSurveys.id).label('record_count')
        )
        .group_by(IndicatorsFromHouseholdSurveys.year)
        .order_by(IndicatorsFromHouseholdSurveys.year)
    )
    
    results = db.execute(query).all()
    years_data = [{"year": r.year, "record_count": r.record_count} for r in results]
    
    if not years_data:
        return {"dataset": "indicators_from_household_surveys", "message": "No temporal data available"}
    
    return {
        "dataset": "indicators_from_household_surveys",
        "earliest_year": min(r["year"] for r in years_data),
        "latest_year": max(r["year"] for r in years_data),
        "total_years": len(years_data),
        "total_records": sum(r["record_count"] for r in years_data),
        "years": years_data
    }

@router.get("/summary")
def get_dataset_summary(db: Session = Depends(get_db)):
    """Get comprehensive summary of this dataset"""
    total_records = db.query(func.count(IndicatorsFromHouseholdSurveys.id)).scalar()
    
    summary = {
        "dataset": "indicators_from_household_surveys",
        "total_records": total_records,
        "foreign_keys": [
            "surveys",
            "indicators",
            "elements",
            "flags",
        ]
    }
    
    # Add counts for each FK relationship
    summary["unique_elements"] = db.query(func.count(func.distinct(IndicatorsFromHouseholdSurveys.element_code_id))).scalar()
    
    return summary
