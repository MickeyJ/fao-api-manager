from . import sdg_bulk_downloads
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running sdg_bulk_downloads pipeline")
    sdg_bulk_downloads.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("sdg_bulk_downloads pipeline complete")