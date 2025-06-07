from . import forestry
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running forestry pipeline")
    forestry.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("forestry pipeline complete")