import os
import pathlib
from pathlib import Path

from data_adapter_oemof.utils import load_yaml

ROOT_DIR = pathlib.Path(__file__).parent

COLLECTIONS_DIR = (
    pathlib.Path(os.environ["COLLECTIONS_DIR"])
    if "COLLECTIONS_DIR" in os.environ
    else pathlib.Path.cwd() / "collections"
)
if not COLLECTIONS_DIR.exists():
    raise FileNotFoundError(
        f"Could not find collections directory '{COLLECTIONS_DIR}'. "
        "You should either create the collections folder or "
        "change path to collection folder by changing environment variable 'COLLECTIONS_DIR'.",
    )
STRUCTURES_DIR = (
    pathlib.Path(os.environ["STRUCTURES_DIR"])
    if "STRUCTURES_DIR" in os.environ
    else pathlib.Path.cwd() / "structures"
)
if not STRUCTURES_DIR.exists():
    raise FileNotFoundError(
        f"Could not find structure directory '{STRUCTURES_DIR}'. "
        "You should either create the structure folder or "
        "change path to structure folder by changing environment variable 'STRUCTURES_DIR'.",
    )

# Maps from oemof.tabular parameter names
# to ontological terms or to sedos nomenclature as fallback option
GLOBAL_PARAMETER_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "GLOBAL_PARAMETER_MAP.yaml"
)
PROCESS_ADAPTER_MAP = load_yaml(
    Path(__file__).parent / "mappings" / "PROCESS_ADAPTER_MAP.yaml"
)
BUS_NAME_MAP = load_yaml(Path(__file__).parent / "mappings" / "BUS_NAME_MAP.yaml")
