from pathlib import Path
import numpy as np


def load_sql(filename: str, base_dir: Path) -> str:
    """Load SQL query from file relative to base_dir"""

    sql_path = base_dir / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text()


def calculate_price_correlation(time_series, current_metrics):
    # Need at least 2 data points
    if len(time_series) < 2:
        return {
            "correlation": None,
            "correlation_based_integration": "insufficient_data",
            "ratio_based_integration": current_metrics["integration_level"],
        }

    # Calculate year-over-year returns
    returns1 = []
    returns2 = []

    for i in range(1, len(time_series)):
        return1 = (time_series[i]["price1"] - time_series[i - 1]["price1"]) / time_series[i - 1]["price1"]
        return2 = (time_series[i]["price2"] - time_series[i - 1]["price2"]) / time_series[i - 1]["price2"]
        returns1.append(return1)
        returns2.append(return2)

    # Calculate Pearson correlation
    correlation = np.corrcoef(returns1, returns2)[0, 1]

    # Handle NaN case (when all values are identical)
    if np.isnan(correlation):
        correlation = 0.0

    # Determine integration level based on correlation
    if correlation > 0.67:
        correlation_integration = "high"
    elif correlation > 0.33:
        correlation_integration = "moderate"
    else:
        correlation_integration = "none"

    return {
        "correlation": round(float(correlation), 3),  # Convert numpy float to Python float
        "correlation_based_integration": correlation_integration,
        "ratio_based_integration": current_metrics["integration_level"],
    }
