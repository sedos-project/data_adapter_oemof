import os
import pathlib

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
