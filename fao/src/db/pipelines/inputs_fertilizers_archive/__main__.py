from . import inputs_fertilizers_archive
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running inputs_fertilizers_archive pipeline")
    inputs_fertilizers_archive.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("inputs_fertilizers_archive pipeline complete")