from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).parent


def load(name: str) -> dict:
    """Load config/<name>.yaml as a dict. Raises FileNotFoundError if absent."""
    path = _CONFIG_DIR / f"{name}.yaml"
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)
