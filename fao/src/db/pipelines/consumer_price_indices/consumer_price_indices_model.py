# templates/model.py.jinja2
from sqlalchemy import (
    String,
    SmallInteger,
    Float,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    Column,
    func,
)
from fao.src.db.database import Base


class ConsumerPriceIndices(Base):
    __tablename__ = "consumer_price_indices"
     # Dataset table - use auto-increment id
    id = Column(Integer, primary_key=True)
    # Foreign key to area_codes
    area_code_id = Column(Integer, ForeignKey("area_codes.id"), index=True)
    # Foreign key to item_codes
    item_code_id = Column(Integer, ForeignKey("item_codes.id"), index=True)
    # Foreign key to elements
    element_code_id = Column(Integer, ForeignKey("elements.id"), index=True)
    # Foreign key to flags
    flag_id = Column(Integer, ForeignKey("flags.id"), index=True)
    months_code = Column(String(4), nullable=False, index=False)
    months = Column(String(20), nullable=False, index=False)
    year_code = Column(String(4), nullable=False, index=False)
    year = Column(SmallInteger, nullable=False, index=True)
    unit = Column(String(50), index=False)
    value = Column(Float, nullable=False, index=False)
    note = Column(String, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    #     #         #         #             #         #             #         #             #         #             #         #         #             #         #         #             #         #         #             #         #         #             #             
    #         # __table_args__ = (
    #     Index("ix_1cd7ff31_uniq_uniq", 
    #         'area_code_id', 'item_code_id', 'element_code_id', 'flag_id', 'year', 'months_code', 'months', 'unit',
    #         unique=True),
    # )
    #         
    def __repr__(self):
        # Show first few columns for datasets
        return f"<ConsumerPriceIndices(id={self.id})>"
