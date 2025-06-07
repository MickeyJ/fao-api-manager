from . import indicators
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running indicators pipeline")
    indicators.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("indicators pipeline complete")