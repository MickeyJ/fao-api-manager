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


class Donors(Base):
    __tablename__ = "donors"
    # Lookup table - use domain primary key
    id = Column(Integer, primary_key=True)
    donor_code = Column(String, nullable=False, index=False)
    donor = Column(String, nullable=False, index=False)
    donor_code_m49 = Column(String, nullable=False, index=False)
    source_dataset = Column(String, nullable=False, index=False)
   
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Composite indexes for lookup tables
    __table_args__ = (
        Index("ix_donors_donor_co_src", 'donor_code', 'source_dataset', unique=True),
    )
    # TODO: Indices for dataset tables
    #     
    def __repr__(self):
        return f"<Donors(donor_code={self.donor_code})>"
