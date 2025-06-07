from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.investment_machinery.investment_machinery_model import InvestmentMachinery
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/investment_machinery",
    tags=["investment_machinery"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_investment_machinery(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    Get investment machinery data with all related information from lookup tables.
    """
    
    # Build query - select all columns from main table
    query = select(InvestmentMachinery)
    
    # Add joins for all foreign key relationships
    query = query.outerjoin(AreaCodes, InvestmentMachinery.area_code_id == AreaCodes.id)
    query = query.add_columns(AreaCodes)
    query = query.outerjoin(ItemCodes, InvestmentMachinery.item_code_id == ItemCodes.id)
    query = query.add_columns(ItemCodes)
    query = query.outerjoin(Elements, InvestmentMachinery.element_code_id == Elements.id)
    query = query.add_columns(Elements)
    query = query.outerjoin(Flags, InvestmentMachinery.flag_id == Flags.id)
    query = query.add_columns(Flags)
    
    # Get total count before pagination
    count_query = select(func.count()).select_from(InvestmentMachinery)
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
                    col_name = f"{table_name}_{column.name}" if table_name != "investment_machinery" else column.name
                    item[col_name] = getattr(table_data, column.name)
        data.append(item)
    
    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data": data
    }