from . import investment_machinery
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_machinery pipeline")
    investment_machinery.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_machinery pipeline complete")