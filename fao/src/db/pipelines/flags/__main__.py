from . import flags
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running flags pipeline")
    flags.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("flags pipeline complete")