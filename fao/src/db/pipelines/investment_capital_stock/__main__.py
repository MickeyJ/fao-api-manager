from . import investment_capital_stock
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_capital_stock pipeline")
    investment_capital_stock.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_capital_stock pipeline complete")