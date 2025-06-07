from . import food_balance_sheets_historic
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_balance_sheets_historic pipeline")
    food_balance_sheets_historic.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_balance_sheets_historic pipeline complete")