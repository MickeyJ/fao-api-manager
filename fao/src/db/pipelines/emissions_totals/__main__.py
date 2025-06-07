from . import emissions_totals
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_totals pipeline")
    emissions_totals.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_totals pipeline complete")