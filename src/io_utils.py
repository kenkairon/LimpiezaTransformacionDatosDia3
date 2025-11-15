from pathlib import Path
import pandas as pd
import logging
from typing import Union

logger = logging.getLogger(__name__)

def ensure_dir(path: Union[str, Path]) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def read_csv(path: Union[str, Path]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        logger.error('File not found: %s', p)
        raise FileNotFoundError(p)
    return pd.read_csv(p)

def save_csv(df: pd.DataFrame, path: Union[str, Path]) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    df.to_csv(p, index=False)
    logger.info('Saved CSV to %s', p)