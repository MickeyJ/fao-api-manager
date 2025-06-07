from . import prices_archive
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running prices_archive pipeline")
    prices_archive.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("prices_archive pipeline complete")