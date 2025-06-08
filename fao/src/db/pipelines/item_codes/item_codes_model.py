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


class ItemCodes(Base):
    __tablename__ = "item_codes"
    # Lookup table - use domain primary key
    id = Column(Integer, primary_key=True)
    item_code = Column(String, nullable=False, index=False)
    item = Column(String, nullable=False, index=False)
    item_code_cpc = Column(String, index=False)
    item_code_fbs = Column(String, index=False)
    item_code_sdg = Column(String, index=False)
    source_dataset = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Composite indexes for lookup tables
    __table_args__ = (
        Index("ix_item_cod_item_cod_src", 'item_code', 'source_dataset', unique=True),
    )
    # TODO: Indices for dataset tables
    #     
    def __repr__(self):
        return f"<ItemCodes(item_code={self.item_code})>"
