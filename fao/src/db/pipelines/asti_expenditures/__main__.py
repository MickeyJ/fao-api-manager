from . import asti_expenditures
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running asti_expenditures pipeline")
    asti_expenditures.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("asti_expenditures pipeline complete")