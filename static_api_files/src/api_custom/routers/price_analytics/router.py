from pathlib import Path
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func, or_, text
from typing import Optional
from .....src.db.database import get_db


def load_sql(filename: str) -> str:
    """Load SQL query from file"""
    sql_path = Path(__file__).parent / "sql" / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text()


router = APIRouter(
    prefix="/v1/prices/analytics",
    tags=["prices", "analytics", "custom"],
)


# Add this endpoint to your existing router
@router.get("/volatility-comparison")
def compare_price_volatility(
    period1_start: int = Query(..., description="Start year for first period"),
    period1_end: int = Query(..., description="End year for first period"),
    period2_start: int = Query(..., description="Start year for second period"),
    period2_end: int = Query(..., description="End year for second period"),
    element: str = Query("Producer Price (USD/tonne)", description="Price element to analyze"),
    area_code: Optional[str] = Query(None, description="Filter by specific country code"),
    item_code: Optional[str] = Query(None, description="Filter by specific item code"),
    min_observations: int = Query(2, description="Minimum observations per period"),
    limit: int = Query(50, description="Maximum results to return"),
    db: Session = Depends(get_db),
):
    """
    Compare price volatility between two time periods.

    Example: /volatility-comparison?period1_start=2017&period1_end=2019&period2_start=2020&period2_end=2023
    """

    # Load SQL from file
    sql_query = load_sql("volatility_comparison.sql")

    result = db.execute(
        text(sql_query),
        {
            "p1_start": period1_start,
            "p1_end": period1_end,
            "p2_start": period2_start,
            "p2_end": period2_end,
            "min_year": min(period1_start, period2_start),
            "max_year": max(period1_end, period2_end),
            "element": element,
            "area_filter": area_code,
            "item_filter": item_code,
            "min_obs": min_observations,
            "limit": limit,
        },
    )

    rows = result.mappings().all()

    return {
        "parameters": {
            "period1": f"{period1_start}-{period1_end}",
            "period2": f"{period2_start}-{period2_end}",
            "element": element,
            "filters": {"area_code": area_code, "item_code": item_code},
        },
        "total_results": len(rows),
        "data": [dict(row) for row in rows],
    }
