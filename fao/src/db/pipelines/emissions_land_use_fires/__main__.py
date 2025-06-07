from . import emissions_land_use_fires
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_land_use_fires pipeline")
    emissions_land_use_fires.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_land_use_fires pipeline complete")