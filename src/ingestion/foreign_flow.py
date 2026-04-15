from pathlib import Path
import polars as pl
from src.utils.io import read_table


REQUIRED_COLUMNS = ["date", "ticker", "foreign_buy_lot", "foreign_sell_lot"]


def load_foreign_flow(path: str | Path) -> pl.DataFrame:
    df = read_table(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"foreign_daily missing columns: {missing}")
    return df.with_columns([
        pl.col("date").cast(pl.Date),
        (pl.col("foreign_buy_lot") - pl.col("foreign_sell_lot")).alias("foreign_net_lot"),
    ])
