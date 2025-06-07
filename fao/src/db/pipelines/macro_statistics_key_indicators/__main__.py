from . import macro_statistics_key_indicators
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running macro_statistics_key_indicators pipeline")
    macro_statistics_key_indicators.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("macro_statistics_key_indicators pipeline complete")