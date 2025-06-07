from . import emissions_livestock
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_livestock pipeline")
    emissions_livestock.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_livestock pipeline complete")