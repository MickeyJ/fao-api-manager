from . import geographic_levels
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running geographic_levels pipeline")
    geographic_levels.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("geographic_levels pipeline complete")