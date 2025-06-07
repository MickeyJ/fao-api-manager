from . import investment_credit_agriculture
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_credit_agriculture pipeline")
    investment_credit_agriculture.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_credit_agriculture pipeline complete")