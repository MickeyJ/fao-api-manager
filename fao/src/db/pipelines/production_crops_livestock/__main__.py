from . import production_crops_livestock
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running production_crops_livestock pipeline")
    production_crops_livestock.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("production_crops_livestock pipeline complete")