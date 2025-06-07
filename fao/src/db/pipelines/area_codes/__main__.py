from . import area_codes
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running area_codes pipeline")
    area_codes.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("area_codes pipeline complete")