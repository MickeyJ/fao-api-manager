from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.item_codes.item_codes_model import ItemCodes

router = APIRouter(
    prefix="/item_codes",
    tags=["item_codes"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_item_codes(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    item codes data with filters.
    Filter options:
    """
    
    query = (
        select(
            ItemCodes,
        )
        .select_from(ItemCodes)
    )
    
    # Apply filters
   
    
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

