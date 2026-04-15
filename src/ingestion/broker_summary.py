from pathlib import Path
import polars as pl
from src.utils.io import read_table


REQUIRED_COLUMNS = ["date", "ticker", "broker_code", "buy_lot", "buy_value", "sell_lot", "sell_value"]


def load_broker_summary(path: str | Path) -> pl.DataFrame:
    df = read_table(path)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"broker_summary_daily missing columns: {missing}")
    return (
        df.with_columns([
            pl.col("date").cast(pl.Date),
            (pl.col("buy_value") / pl.when(pl.col("buy_lot") > 0).then(pl.col("buy_lot")).otherwise(None)).alias("buy_avg"),
            (pl.col("sell_value") / pl.when(pl.col("sell_lot") > 0).then(pl.col("sell_lot")).otherwise(None)).alias("sell_avg"),
            (pl.col("buy_lot") - pl.col("sell_lot")).alias("net_lot"),
            (pl.col("buy_value") - pl.col("sell_value")).alias("net_value"),
            (pl.col("buy_lot") + pl.col("sell_lot")).alias("gross_activity_lot"),
            (pl.col("buy_value") + pl.col("sell_value")).alias("gross_activity_value"),
        ])
    )
