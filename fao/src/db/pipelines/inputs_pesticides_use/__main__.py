from . import inputs_pesticides_use
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running inputs_pesticides_use pipeline")
    inputs_pesticides_use.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("inputs_pesticides_use pipeline complete")