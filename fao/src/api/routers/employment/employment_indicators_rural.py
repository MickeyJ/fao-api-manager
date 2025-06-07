from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.employment_indicators_rural.employment_indicators_rural_model import EmploymentIndicatorsRural
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.sources.sources_model import Sources
from fao.src.db.pipelines.indicators.indicators_model import Indicators
from fao.src.db.pipelines.sexs.sexs_model import Sexs
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/employment_indicators_rural",
    tags=["employment_indicators_rural"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_employment_indicators_rural(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    Get employment indicators rural data with all related information from lookup tables.
    """
    
    # Build query - select all columns from main table
    query = select(EmploymentIndicatorsRural)
    
    # Add joins for all foreign key relationships
    query = query.outerjoin(AreaCodes, EmploymentIndicatorsRural.area_code_id == AreaCodes.id)
    query = query.add_columns(AreaCodes)
    query = query.outerjoin(Sources, EmploymentIndicatorsRural.source_code_id == Sources.id)
    query = query.add_columns(Sources)
    query = query.outerjoin(Indicators, EmploymentIndicatorsRural.indicator_code_id == Indicators.id)
    query = query.add_columns(Indicators)
    query = query.outerjoin(Sexs, EmploymentIndicatorsRural.sex_code_id == Sexs.id)
    query = query.add_columns(Sexs)
    query = query.outerjoin(Elements, EmploymentIndicatorsRural.element_code_id == Elements.id)
    query = query.add_columns(Elements)
    query = query.outerjoin(Flags, EmploymentIndicatorsRural.flag_id == Flags.id)
    query = query.add_columns(Flags)
    
    # Get total count before pagination
    count_query = select(func.count()).select_from(EmploymentIndicatorsRural)
    total_count = db.execute(count_query).scalar()
    
    # Apply pagination
    query = query.offset(offset).limit(limit)
    
    # Execute query
    results = db.execute(query).all()
    
    # Format results
    data = []
    for row in results:
        item = {}
        # Add all columns from all tables in the row
        for table_data in row:
            if hasattr(table_data, '__table__'):
                table_name = table_data.__table__.name
                for column in table_data.__table__.columns:
                    col_name = f"{table_name}_{column.name}" if table_name != "employment_indicators_rural" else column.name
                    item[col_name] = getattr(table_data, column.name)
        data.append(item)
    
    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data": data
    }