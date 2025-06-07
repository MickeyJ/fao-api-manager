from . import employment_indicators_rural
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running employment_indicators_rural pipeline")
    employment_indicators_rural.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("employment_indicators_rural pipeline complete")