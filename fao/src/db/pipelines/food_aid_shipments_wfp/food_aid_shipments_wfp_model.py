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


class FoodAidShipmentsWfp(Base):
    __tablename__ = "food_aid_shipments_wfp"
     # Dataset table - use auto-increment id
    id = Column(Integer, primary_key=True)
    # Foreign key to item_codes
    item_code_id = Column(Integer, ForeignKey("item_codes.id"), index=True)
    # Foreign key to elements
    element_code_id = Column(Integer, ForeignKey("elements.id"), index=True)
    # Foreign key to flags
    flag_id = Column(Integer, ForeignKey("flags.id"), index=True)
    recipient_country_code = Column(String, nullable=False, index=False)
    recipient_country_code_m49 = Column(String, nullable=False, index=False)
    recipient_country = Column(String, nullable=False, index=False)
    year_code = Column(String(4), nullable=False, index=False)
    year = Column(SmallInteger, nullable=False, index=True)
    unit = Column(String(50), nullable=False, index=False)
    value = Column(Float, nullable=False, index=False)
    note = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    #     #         #         #             #         #             #         #             #         #         #             #         #         #         #         #             #             
    #         # __table_args__ = (
    #     Index("ix_7ed4ab55_uniq_uniq", 
    #         'item_code_id', 'element_code_id', 'flag_id', 'year', 'unit',
    #         unique=True),
    # )
    #         
    def __repr__(self):
        # Show first few columns for datasets
        return f"<FoodAidShipmentsWfp(id={self.id}, recipient_country_code={self.recipient_country_code}, recipient_country_code_m49={self.recipient_country_code_m49}, recipient_country={self.recipient_country})>"
