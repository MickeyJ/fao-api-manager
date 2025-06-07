from . import investment_foreign_direct_investment
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_foreign_direct_investment pipeline")
    investment_foreign_direct_investment.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_foreign_direct_investment pipeline complete")