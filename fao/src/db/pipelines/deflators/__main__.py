from . import deflators
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running deflators pipeline")
    deflators.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("deflators pipeline complete")