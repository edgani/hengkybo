from __future__ import annotations

from pathlib import Path
import polars as pl


def load_done_detail(path: str | Path) -> pl.DataFrame:
    df = pl.read_csv(path, try_parse_dates=True)
    required = {
        "timestamp", "trade_date", "ticker", "price", "lot",
        "buyer_broker", "seller_broker", "side_aggressor", "trade_seq"
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing done-detail columns: {sorted(missing)}")
    return df.with_columns([
        pl.col("trade_date").cast(pl.Date),
        pl.col("lot").cast(pl.Float64),
        pl.col("price").cast(pl.Float64),
        (pl.col("price") * pl.col("lot") * 100).alias("value"),
        pl.col("side_aggressor").cast(pl.Utf8).str.to_uppercase(),
        pl.col("buyer_broker").cast(pl.Utf8).str.to_uppercase(),
        pl.col("seller_broker").cast(pl.Utf8).str.to_uppercase(),
    ])
