from . import environment_soil_nutrient_budget
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_soil_nutrient_budget pipeline")
    environment_soil_nutrient_budget.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_soil_nutrient_budget pipeline complete")