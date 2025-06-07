from . import environment_pesticides
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_pesticides pipeline")
    environment_pesticides.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_pesticides pipeline complete")