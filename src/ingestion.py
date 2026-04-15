from __future__ import annotations
from pathlib import Path
import pandas as pd


def load_csv(path: str | Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if parse_dates:
        for c in parse_dates:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c])
    return df
