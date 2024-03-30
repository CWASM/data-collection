import os
from pathlib import Path
import warnings

ROOT_DIR = Path(__file__).parents[1]

DEFAULT_DATA_DIR = ROOT_DIR / "data"
DATA_DIR = os.environ.get("SAME_DATA_DIR", None)
if not DATA_DIR:
    warnings.warn(f"Environment variable 'SAME_DATA_DIR' not set. "
                  f"Saving data to {DEFAULT_DATA_DIR.absolute()} by default.")
    DATA_DIR = DEFAULT_DATA_DIR

if not DATA_DIR.exists():
    warnings.warn(f"{DATA_DIR.absolute()} not found. Creating.")
    DATA_DIR.mkdir(parents=True)