from . import development_assistance_to_agriculture
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running development_assistance_to_agriculture pipeline")
    development_assistance_to_agriculture.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("development_assistance_to_agriculture pipeline complete")