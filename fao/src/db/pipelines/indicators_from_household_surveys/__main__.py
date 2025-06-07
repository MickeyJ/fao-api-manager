from . import indicators_from_household_surveys
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running indicators_from_household_surveys pipeline")
    indicators_from_household_surveys.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("indicators_from_household_surveys pipeline complete")