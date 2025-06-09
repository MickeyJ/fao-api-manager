from sqlalchemy import Column, Integer, String, DateTime, func
from fao.src.db.database import Base


class PipelineProgress(Base):
    __tablename__ = "pipeline_progress"

    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), unique=True, nullable=False, index=True)
    last_row_processed = Column(Integer, nullable=False)
    total_rows = Column(Integer)
    last_chunk_time = Column(DateTime, default=func.now())
    status = Column(String(20), default="in_progress")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<PipelineProgress(table={self.table_name}, progress={self.last_row_processed}/{self.total_rows})>"
