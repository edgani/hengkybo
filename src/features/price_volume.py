import numpy as np
import polars as pl


def build_price_volume_features(prices: pl.DataFrame) -> pl.DataFrame:
    prices = prices.sort(["ticker", "date"])
    out = prices.with_columns([
        pl.col("close").pct_change().over("ticker").alias("ret_1d"),
        ((pl.col("close") - pl.col("open")) / pl.when(pl.col("open") != 0).then(pl.col("open")).otherwise(None)).alias("intraday_return"),
        (pl.col("high") - pl.col("low")).alias("range_abs"),
        ((pl.col("high") - pl.col("low")) / pl.when(pl.col("close") != 0).then(pl.col("close")).otherwise(None)).alias("range_pct"),
        pl.col("volume_lots").rolling_mean(20).over("ticker").alias("avg_volume_lots_20d"),
        pl.col("close").rolling_mean(20).over("ticker").alias("ma20"),
        pl.col("close").rolling_mean(60).over("ticker").alias("ma60"),
    ])
    out = out.with_columns([
        ((pl.col("close") - pl.col("ma20")) / pl.when(pl.col("ma20") != 0).then(pl.col("ma20")).otherwise(None)).alias("dist_ma20"),
        ((pl.col("close") - pl.col("ma60")) / pl.when(pl.col("ma60") != 0).then(pl.col("ma60")).otherwise(None)).alias("dist_ma60"),
        pl.when(pl.col("volume_lots") > pl.col("avg_volume_lots_20d")).then(1).otherwise(0).alias("volume_above_20d"),
    ])
    return out


def zscore_expr(col: str) -> pl.Expr:
    return ((pl.col(col) - pl.col(col).mean()) / pl.col(col).std()).fill_nan(0).fill_null(0)
