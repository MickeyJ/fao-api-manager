from . import item_codes
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running item_codes pipeline")
    item_codes.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("item_codes pipeline complete")