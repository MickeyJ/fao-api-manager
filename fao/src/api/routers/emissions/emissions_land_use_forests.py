from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.emissions_land_use_forests.emissions_land_use_forests_model import EmissionsLandUseForests
# Import core/lookup tables for joins
from fao.src.db.pipelines.area_codes.area_codes_model import AreaCodes
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.sources.sources_model import Sources
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/emissions_land_use_forests",
    tags=["emissions_land_use_forests"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_emissions_land_use_forests(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    Get emissions land use forests data with all related information from lookup tables.
    """
    
    # Build query - select all columns from main table
    query = select(EmissionsLandUseForests)
    
    # Add joins for all foreign key relationships
    query = query.outerjoin(AreaCodes, EmissionsLandUseForests.area_code_id == AreaCodes.id)
    query = query.add_columns(AreaCodes)
    query = query.outerjoin(ItemCodes, EmissionsLandUseForests.item_code_id == ItemCodes.id)
    query = query.add_columns(ItemCodes)
    query = query.outerjoin(Elements, EmissionsLandUseForests.element_code_id == Elements.id)
    query = query.add_columns(Elements)
    query = query.outerjoin(Sources, EmissionsLandUseForests.source_code_id == Sources.id)
    query = query.add_columns(Sources)
    query = query.outerjoin(Flags, EmissionsLandUseForests.flag_id == Flags.id)
    query = query.add_columns(Flags)
    
    # Get total count before pagination
    count_query = select(func.count()).select_from(EmissionsLandUseForests)
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
                    col_name = f"{table_name}_{column.name}" if table_name != "emissions_land_use_forests" else column.name
                    item[col_name] = getattr(table_data, column.name)
        data.append(item)
    
    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data": data
    }