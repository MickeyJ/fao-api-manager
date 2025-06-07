from . import emissions_crops
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_crops pipeline")
    emissions_crops.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_crops pipeline complete")