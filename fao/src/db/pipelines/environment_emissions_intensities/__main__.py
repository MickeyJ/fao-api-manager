from . import environment_emissions_intensities
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_emissions_intensities pipeline")
    environment_emissions_intensities.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_emissions_intensities pipeline complete")