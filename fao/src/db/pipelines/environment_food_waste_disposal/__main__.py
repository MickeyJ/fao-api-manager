from . import environment_food_waste_disposal
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_food_waste_disposal pipeline")
    environment_food_waste_disposal.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_food_waste_disposal pipeline complete")