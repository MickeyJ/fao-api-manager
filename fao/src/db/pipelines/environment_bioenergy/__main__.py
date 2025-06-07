from . import environment_bioenergy
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_bioenergy pipeline")
    environment_bioenergy.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_bioenergy pipeline complete")