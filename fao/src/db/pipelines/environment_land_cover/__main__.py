from . import environment_land_cover
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_land_cover pipeline")
    environment_land_cover.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_land_cover pipeline complete")