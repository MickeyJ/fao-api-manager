from .pipeline_specs import PipelineSpecs
from generator import ZIP_PATH


def main():
    PipelineSpecs(ZIP_PATH).create()


if __name__ == "__main__":
    main()
