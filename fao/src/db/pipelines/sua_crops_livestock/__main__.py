from . import sua_crops_livestock
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running sua_crops_livestock pipeline")
    sua_crops_livestock.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("sua_crops_livestock pipeline complete")