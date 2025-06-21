from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ProjectPaths:
    root: Path
    core: Path = field(default=Path("core"))
    db: Path = field(default=Path("db"))
    db_pipelines: Path = field(default=Path("db/pipelines"))
    api: Path = field(default=Path("api"))
    api_routers: Path = field(default=Path("api/routers"))

    def __post_init__(self):
        # Convert root to Path if string
        self.root = Path(self.root)

        # Prepend root path to all other paths
        for field_name in self.__dataclass_fields__:
            if field_name != "root":
                current_value = getattr(self, field_name)
                setattr(self, field_name, self.root / current_value)
