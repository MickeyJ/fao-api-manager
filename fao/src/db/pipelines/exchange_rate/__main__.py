from . import exchange_rate
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running exchange_rate pipeline")
    exchange_rate.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("exchange_rate pipeline complete")