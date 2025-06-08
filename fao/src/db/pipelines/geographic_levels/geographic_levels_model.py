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


class GeographicLevels(Base):
    __tablename__ = "geographic_levels"
    # Lookup table - use domain primary key
    id = Column(Integer, primary_key=True)
    geographic_level_code = Column(String, nullable=False, index=False)
    geographic_level = Column(String, nullable=False, index=False)
    source_dataset = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Composite indexes for lookup tables
    __table_args__ = (
        Index("ix_geograph_geograph_src", 'geographic_level_code', 'source_dataset', unique=True),
    )
    # TODO: Indices for dataset tables
    #     
    def __repr__(self):
        return f"<GeographicLevels(geographic_level_code={self.geographic_level_code})>"
