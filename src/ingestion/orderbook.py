from __future__ import annotations

from pathlib import Path
import polars as pl


def load_orderbook(path: str | Path) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True)
    required = {"timestamp", "trade_date", "ticker"}
    price_cols = [f"bid_{i}_price" for i in range(1, 6)] + [f"offer_{i}_price" for i in range(1, 6)]
    lot_cols = [f"bid_{i}_lot" for i in range(1, 6)] + [f"offer_{i}_lot" for i in range(1, 6)]
    missing = (required | set(price_cols) | set(lot_cols)) - set(df.columns)
    if missing:
        raise ValueError(f"Missing orderbook columns: {sorted(missing)}")
    casts = [pl.col(c).cast(pl.Float64) for c in price_cols + lot_cols]
    return df.with_columns([pl.col("trade_date").cast(pl.Date)] + casts)
