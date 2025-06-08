# templates/model.py.jinja2
from sqlalchemy import (
    String,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    Column,
    func,
)
from fao.src.db.database import Base


class FoodGroups(Base):
    __tablename__ = "food_groups"
    # Lookup table - use domain primary key
    id = Column(Integer, primary_key=True)
    food_group_code = Column(String, nullable=False, index=False)
    food_group = Column(String, nullable=False, index=False)
    source_dataset = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Composite indexes for lookup tables
    __table_args__ = (
        Index("ix_food_gro_food_gro_src", 'food_group_code', 'source_dataset', unique=True),
    )
    # TODO: Indices for dataset tables
    #     
    def __repr__(self):
        return f"<FoodGroups(food_group_code={self.food_group_code})>"
