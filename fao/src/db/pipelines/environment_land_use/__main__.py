from . import environment_land_use
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_land_use pipeline")
    environment_land_use.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_land_use pipeline complete")