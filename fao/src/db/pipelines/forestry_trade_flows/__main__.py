from . import forestry_trade_flows
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running forestry_trade_flows pipeline")
    forestry_trade_flows.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("forestry_trade_flows pipeline complete")