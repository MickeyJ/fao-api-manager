from . import supply_utilization_accounts_food_and_diet
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running supply_utilization_accounts_food_and_diet pipeline")
    supply_utilization_accounts_food_and_diet.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("supply_utilization_accounts_food_and_diet pipeline complete")