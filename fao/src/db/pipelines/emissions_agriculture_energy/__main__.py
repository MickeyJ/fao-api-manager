from . import emissions_agriculture_energy
from fao.src.db.database import run_with_session


def run_all(db):
    print("Running emissions_agriculture_energy pipeline")
    emissions_agriculture_energy.run(db)


if __name__ == "__main__":
    run_with_session(run_all)
    print("emissions_agriculture_energy pipeline complete")