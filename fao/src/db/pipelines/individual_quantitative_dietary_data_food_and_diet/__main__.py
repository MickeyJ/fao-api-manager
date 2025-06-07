from . import individual_quantitative_dietary_data_food_and_diet
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running individual_quantitative_dietary_data_food_and_diet pipeline")
    individual_quantitative_dietary_data_food_and_diet.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("individual_quantitative_dietary_data_food_and_diet pipeline complete")