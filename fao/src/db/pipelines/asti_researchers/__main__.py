from . import asti_researchers
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running asti_researchers pipeline")
    asti_researchers.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("asti_researchers pipeline complete")