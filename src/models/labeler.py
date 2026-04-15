import polars as pl


def label_ready_long_success(prices: pl.DataFrame, horizon: int = 20, target_return: float = 0.08, max_adverse: float = -0.04) -> pl.DataFrame:
    df = prices.sort(["ticker", "date"]).with_columns([
        pl.col("close").shift(-horizon).over("ticker").alias("close_fwd"),
        ((pl.col("close").shift(-horizon).over("ticker") - pl.col("close")) / pl.col("close")).alias("ret_fwd_h"),
    ])
    # Simplified placeholder label. True MFE/MAE labeling should use path-dependent future windows.
    return df.with_columns(
        pl.when(pl.col("ret_fwd_h") >= target_return).then(1).otherwise(0).alias("label_ready_long_success")
    )
