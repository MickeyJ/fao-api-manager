from . import environment_livestock_manure
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_livestock_manure pipeline")
    environment_livestock_manure.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_livestock_manure pipeline complete")