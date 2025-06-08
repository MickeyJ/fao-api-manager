# templates/model.py.jinja2
from sqlalchemy import (
    String,
    Float,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    Column,
    func,
)
from fao.src.db.database import Base


class MinimumDietaryDiversityForWomenMddWFoodAndDiet(Base):
    __tablename__ = "minimum_dietary_diversity_for_women_mdd_w_food_and_diet"
     # Dataset table - use auto-increment id
    id = Column(Integer, primary_key=True)
    # Foreign key to surveys
    survey_code_id = Column(Integer, ForeignKey("surveys.id"), index=True)
    # Foreign key to food_groups
    food_group_code_id = Column(Integer, ForeignKey("food_groups.id"), index=True)
    # Foreign key to indicators
    indicator_code_id = Column(Integer, ForeignKey("indicators.id"), index=True)
    # Foreign key to geographic_levels
    geographic_level_code_id = Column(Integer, ForeignKey("geographic_levels.id"), index=True)
    # Foreign key to elements
    element_code_id = Column(Integer, ForeignKey("elements.id"), index=True)
    # Foreign key to flags
    flag_id = Column(Integer, ForeignKey("flags.id"), index=True)
    unit = Column(String(50), nullable=False, index=False)
    value = Column(Float, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    #     #         #         #             #         #             #         #             #         #             #         #             #         #             #         #         #         #         #         #             #             
    #         # __table_args__ = (
    #     Index("ix_77c4a4b1_uniq_uniq", 
    #         'survey_code_id', 'food_group_code_id', 'indicator_code_id', 'geographic_level_code_id', 'element_code_id', 'flag_id', 'unit',
    #         unique=True),
    # )
    #         
    def __repr__(self):
        # Show first few columns for datasets
        return f"<MinimumDietaryDiversityForWomenMddWFoodAndDiet(id={self.id})>"
