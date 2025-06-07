from . import food_aid_shipments_wfp
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running food_aid_shipments_wfp pipeline")
    food_aid_shipments_wfp.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("food_aid_shipments_wfp pipeline complete")