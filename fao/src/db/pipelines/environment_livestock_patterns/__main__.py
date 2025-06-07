from . import environment_livestock_patterns
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running environment_livestock_patterns pipeline")
    environment_livestock_patterns.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("environment_livestock_patterns pipeline complete")