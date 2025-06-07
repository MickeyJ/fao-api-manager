from . import food_groups
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_groups pipeline")
    food_groups.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_groups pipeline complete")