from . import sexs
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running sexs pipeline")
    sexs.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("sexs pipeline complete")