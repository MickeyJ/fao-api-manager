from . import environment_emissions_by_sector
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_emissions_by_sector pipeline")
    environment_emissions_by_sector.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_emissions_by_sector pipeline complete")