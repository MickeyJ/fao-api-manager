from . import environment_temperature_change
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_temperature_change pipeline")
    environment_temperature_change.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_temperature_change pipeline complete")