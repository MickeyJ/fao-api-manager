from . import sources
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running sources pipeline")
    sources.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("sources pipeline complete")