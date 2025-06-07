from . import population_age_groups
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running population_age_groups pipeline")
    population_age_groups.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("population_age_groups pipeline complete")