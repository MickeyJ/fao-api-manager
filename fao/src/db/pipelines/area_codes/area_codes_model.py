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


class AreaCodes(Base):
    __tablename__ = "area_codes"
    # Lookup table - use domain primary key
    id = Column(Integer, primary_key=True)
    area_code = Column(String, nullable=False, index=False)
    area = Column(String, nullable=False, index=False)
    area_code_m49 = Column(String, index=False)
    source_dataset = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Composite indexes for lookup tables
    __table_args__ = (
        Index("ix_area_cod_area_cod_src", 'area_code', 'source_dataset', unique=True),
    )
    # TODO: Indices for dataset tables
    #     
    def __repr__(self):
        return f"<AreaCodes(area_code={self.area_code})>"
