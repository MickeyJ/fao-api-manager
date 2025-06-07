from . import cost_affordability_healthy_diet_co_ahd
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running cost_affordability_healthy_diet_co_ahd pipeline")
    cost_affordability_healthy_diet_co_ahd.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("cost_affordability_healthy_diet_co_ahd pipeline complete")