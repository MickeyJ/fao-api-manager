from . import investment_machinery_archive
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_machinery_archive pipeline")
    investment_machinery_archive.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_machinery_archive pipeline complete")