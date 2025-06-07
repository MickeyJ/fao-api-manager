from . import food_security_data
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_security_data pipeline")
    food_security_data.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_security_data pipeline complete")