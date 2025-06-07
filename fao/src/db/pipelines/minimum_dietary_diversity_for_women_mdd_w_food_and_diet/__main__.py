from . import minimum_dietary_diversity_for_women_mdd_w_food_and_diet
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running minimum_dietary_diversity_for_women_mdd_w_food_and_diet pipeline")
    minimum_dietary_diversity_for_women_mdd_w_food_and_diet.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("minimum_dietary_diversity_for_women_mdd_w_food_and_diet pipeline complete")