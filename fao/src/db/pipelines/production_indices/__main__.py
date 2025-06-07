from . import production_indices
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running production_indices pipeline")
    production_indices.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("production_indices pipeline complete")