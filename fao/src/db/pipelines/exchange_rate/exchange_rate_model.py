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


class ExchangeRate(Base):
    __tablename__ = "exchange_rate"
     # Dataset table - use auto-increment id
    id = Column(Integer, primary_key=True)
    # Foreign key to area_codes
    area_code_id = Column(Integer, ForeignKey("area_codes.id"), index=True)
    # Foreign key to elements
    element_code_id = Column(Integer, ForeignKey("elements.id"), index=True)
    # Foreign key to currencies
    iso_currency_code_id = Column(Integer, ForeignKey("currencies.id"), index=True)
    # Foreign key to flags
    flag_id = Column(Integer, ForeignKey("flags.id"), index=True)
    year_code = Column(String(4), nullable=False, index=False)
    year = Column(SmallInteger, nullable=False, index=True)
    months_code = Column(String(4), nullable=False, index=False)
    months = Column(String(20), nullable=False, index=False)
    unit = Column(String(50), index=False)
    value = Column(Float, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    #     #         #         #             #         #             #         #             #         #             #         #         #             #         #         #             #         #         #             #         #         #             #             
    #         # __table_args__ = (
    #     Index("ix_be812b69_uniq_uniq", 
    #         'area_code_id', 'element_code_id', 'iso_currency_code_id', 'flag_id', 'year', 'months_code', 'months', 'unit',
    #         unique=True),
    # )
    #         
    def __repr__(self):
        # Show first few columns for datasets
        return f"<ExchangeRate(id={self.id})>"
