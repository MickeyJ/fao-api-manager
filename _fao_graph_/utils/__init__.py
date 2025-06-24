import yaml
from pathlib import Path
from .load_sql import load_sql


def singularize(name: str) -> str:
    """Convert plural to singular for common cases"""
    if name.endswith("ies"):
        return name[:-3] + "y"  # currencies -> currency
    elif name.endswith("ses"):
        return name[:-2]  # purposes -> purpose
    elif name.endswith("s"):
        return name[:-1]  # area_codes -> area_code
    return name


def load_yaml_config(filename: Path | str, base_dir: Path) -> dict:
    """Load the YAML configuration"""
    yaml_config_path = base_dir / filename
    if not yaml_config_path.exists():
        raise FileNotFoundError(f"SQL file not found: {yaml_config_path}")
    with open(yaml_config_path, "r") as f:
        return yaml.safe_load(f)


__all__ = ["load_yaml_config", "load_sql", "singularize"]
