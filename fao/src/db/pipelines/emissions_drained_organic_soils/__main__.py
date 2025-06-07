from . import emissions_drained_organic_soils
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_drained_organic_soils pipeline")
    emissions_drained_organic_soils.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_drained_organic_soils pipeline complete")