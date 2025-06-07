from . import household_consumption_and_expenditure_surveys_food_and_diet
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running household_consumption_and_expenditure_surveys_food_and_diet pipeline")
    household_consumption_and_expenditure_surveys_food_and_diet.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("household_consumption_and_expenditure_surveys_food_and_diet pipeline complete")