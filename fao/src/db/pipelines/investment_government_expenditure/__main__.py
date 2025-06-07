from . import investment_government_expenditure
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_government_expenditure pipeline")
    investment_government_expenditure.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_government_expenditure pipeline complete")