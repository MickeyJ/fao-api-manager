from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from fao.src.db.database import get_db
from fao.src.db.pipelines.individual_quantitative_dietary_data_food_and_diet.individual_quantitative_dietary_data_food_and_diet_model import IndividualQuantitativeDietaryDataFoodAndDiet
# Import core/lookup tables for joins
from fao.src.db.pipelines.surveys.surveys_model import Surveys
from fao.src.db.pipelines.geographic_levels.geographic_levels_model import GeographicLevels
from fao.src.db.pipelines.population_age_groups.population_age_groups_model import PopulationAgeGroups
from fao.src.db.pipelines.food_groups.food_groups_model import FoodGroups
from fao.src.db.pipelines.indicators.indicators_model import Indicators
from fao.src.db.pipelines.elements.elements_model import Elements
from fao.src.db.pipelines.sexs.sexs_model import Sexs
from fao.src.db.pipelines.flags.flags_model import Flags

router = APIRouter(
    prefix="/individual_quantitative_dietary_data_food_and_diet",
    tags=["individual_quantitative_dietary_data_food_and_diet"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
def get_individual_quantitative_dietary_data_food_and_diet(
    limit: int = Query(100, le=1000, ge=1, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    db: Session = Depends(get_db)
):
    """
    Get individual quantitative dietary data food and diet data with all related information from lookup tables.
    """
    
    # Build query - select all columns from main table
    query = select(IndividualQuantitativeDietaryDataFoodAndDiet)
    
    # Add joins for all foreign key relationships
    query = query.outerjoin(Surveys, IndividualQuantitativeDietaryDataFoodAndDiet.survey_code_id == Surveys.id)
    query = query.add_columns(Surveys)
    query = query.outerjoin(GeographicLevels, IndividualQuantitativeDietaryDataFoodAndDiet.geographic_level_code_id == GeographicLevels.id)
    query = query.add_columns(GeographicLevels)
    query = query.outerjoin(PopulationAgeGroups, IndividualQuantitativeDietaryDataFoodAndDiet.population_age_group_code_id == PopulationAgeGroups.id)
    query = query.add_columns(PopulationAgeGroups)
    query = query.outerjoin(FoodGroups, IndividualQuantitativeDietaryDataFoodAndDiet.food_group_code_id == FoodGroups.id)
    query = query.add_columns(FoodGroups)
    query = query.outerjoin(Indicators, IndividualQuantitativeDietaryDataFoodAndDiet.indicator_code_id == Indicators.id)
    query = query.add_columns(Indicators)
    query = query.outerjoin(Elements, IndividualQuantitativeDietaryDataFoodAndDiet.element_code_id == Elements.id)
    query = query.add_columns(Elements)
    query = query.outerjoin(Sexs, IndividualQuantitativeDietaryDataFoodAndDiet.sex_code_id == Sexs.id)
    query = query.add_columns(Sexs)
    query = query.outerjoin(Flags, IndividualQuantitativeDietaryDataFoodAndDiet.flag_id == Flags.id)
    query = query.add_columns(Flags)
    
    # Get total count before pagination
    count_query = select(func.count()).select_from(IndividualQuantitativeDietaryDataFoodAndDiet)
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
                    col_name = f"{table_name}_{column.name}" if table_name != "individual_quantitative_dietary_data_food_and_diet" else column.name
                    item[col_name] = getattr(table_data, column.name)
        data.append(item)
    
    return {
        "total_count": total_count,
        "limit": limit,
        "offset": offset,
        "data": data
    }