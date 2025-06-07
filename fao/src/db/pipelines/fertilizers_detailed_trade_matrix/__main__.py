from . import fertilizers_detailed_trade_matrix
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running fertilizers_detailed_trade_matrix pipeline")
    fertilizers_detailed_trade_matrix.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("fertilizers_detailed_trade_matrix pipeline complete")