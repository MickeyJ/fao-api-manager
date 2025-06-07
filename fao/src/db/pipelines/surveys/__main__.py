from . import surveys
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running surveys pipeline")
    surveys.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("surveys pipeline complete")