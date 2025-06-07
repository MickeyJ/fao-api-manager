from . import emissions_pre_post_production
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_pre_post_production pipeline")
    emissions_pre_post_production.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_pre_post_production pipeline complete")