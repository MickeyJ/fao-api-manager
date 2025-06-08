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


class IndicatorsFromHouseholdSurveys(Base):
    __tablename__ = "indicators_from_household_surveys"
     # Dataset table - use auto-increment id
    id = Column(Integer, primary_key=True)
    # Foreign key to surveys
    survey_code_id = Column(Integer, ForeignKey("surveys.id"), index=True)
    # Foreign key to indicators
    indicator_code_id = Column(Integer, ForeignKey("indicators.id"), index=True)
    # Foreign key to elements
    element_code_id = Column(Integer, ForeignKey("elements.id"), index=True)
    # Foreign key to flags
    flag_id = Column(Integer, ForeignKey("flags.id"), index=True)
    breakdown_variable_code = Column(String, nullable=False, index=False)
    breakdown_variable = Column(String, nullable=False, index=False)
    breadown_by_sex_of_the_household_head_code = Column(String, nullable=False, index=False)
    breadown_by_sex_of_the_household_head = Column(String, nullable=False, index=False)
    unit = Column(String(50), nullable=False, index=False)
    value = Column(Float, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    #     #         #         #             #         #             #         #             #         #             #         #         #         #         #         #             #             
    #         # __table_args__ = (
    #     Index("ix_6ca61764_uniq_uniq", 
    #         'survey_code_id', 'indicator_code_id', 'element_code_id', 'flag_id', 'unit',
    #         unique=True),
    # )
    #         
    def __repr__(self):
        # Show first few columns for datasets
        return f"<IndicatorsFromHouseholdSurveys(id={self.id}, breakdown_variable_code={self.breakdown_variable_code})>"
