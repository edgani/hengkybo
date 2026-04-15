from pathlib import Path
import polars as pl
from src.utils.io import read_table


REQUIRED_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume_shares"]


def load_prices(path: str | Path) -> pl.DataFrame:
    df = read_table(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"prices_daily missing columns: {missing}")
    if "volume_lots" not in df.columns:
        df = df.with_columns((pl.col("volume_shares") / 100.0).alias("volume_lots"))
    return df.with_columns(pl.col("date").cast(pl.Date))
