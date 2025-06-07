from . import commodity_balances_non_food_2013_old_methodology
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running commodity_balances_non_food_2013_old_methodology pipeline")
    commodity_balances_non_food_2013_old_methodology.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("commodity_balances_non_food_2013_old_methodology pipeline complete")