from . import forestry_pulp_paper_survey
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running forestry_pulp_paper_survey pipeline")
    forestry_pulp_paper_survey.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("forestry_pulp_paper_survey pipeline complete")