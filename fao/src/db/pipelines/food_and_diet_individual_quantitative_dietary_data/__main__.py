from . import food_and_diet_individual_quantitative_dietary_data
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_and_diet_individual_quantitative_dietary_data pipeline")
    food_and_diet_individual_quantitative_dietary_data.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_and_diet_individual_quantitative_dietary_data pipeline complete")