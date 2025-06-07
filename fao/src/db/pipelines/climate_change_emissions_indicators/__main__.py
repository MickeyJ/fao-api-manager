from . import climate_change_emissions_indicators
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running climate_change_emissions_indicators pipeline")
    climate_change_emissions_indicators.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("climate_change_emissions_indicators pipeline complete")