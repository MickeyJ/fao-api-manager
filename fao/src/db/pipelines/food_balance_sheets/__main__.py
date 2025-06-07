from . import food_balance_sheets
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_balance_sheets pipeline")
    food_balance_sheets.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_balance_sheets pipeline complete")