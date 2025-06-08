from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.population_age_groups.population_age_groups_model import PopulationAgeGroups

router = APIRouter(
    prefix="/population_age_groups",
    tags=["population_age_groups"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_population_age_groups(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    population age groups data with filters.
    Filter options:
    """
    
    query = (
        select(
            PopulationAgeGroups,
        )
        .select_from(PopulationAgeGroups)
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

