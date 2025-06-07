from . import consumer_price_indices
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running consumer_price_indices pipeline")
    consumer_price_indices.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("consumer_price_indices pipeline complete")