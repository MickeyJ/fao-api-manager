from . import investment_country_investment_statistics_profile
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running investment_country_investment_statistics_profile pipeline")
    investment_country_investment_statistics_profile.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("investment_country_investment_statistics_profile pipeline complete")