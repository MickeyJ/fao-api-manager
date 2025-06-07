from . import emissions_land_use_forests
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_land_use_forests pipeline")
    emissions_land_use_forests.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_land_use_forests pipeline complete")