import polars as pl


def build_dry_wet_features(prices: pl.DataFrame, levels: pl.DataFrame) -> pl.DataFrame:
    df = prices.join(levels, on=["ticker", "date"], how="left")
    if "free_float_shares" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("free_float_shares"))
    df = df.with_columns([
        (pl.col("volume_shares").rolling_sum(20).over("ticker") / pl.when(pl.col("free_float_shares") > 0).then(pl.col("free_float_shares")).otherwise(None)).alias("float_rotation_20d"),
        ((pl.col("close") - pl.col("institutional_support")) / pl.when(pl.col("institutional_support") != 0).then(pl.col("institutional_support")).otherwise(None)).alias("support_distance_pct"),
        ((pl.col("institutional_resistance") - pl.col("close")) / pl.when(pl.col("close") != 0).then(pl.col("close")).otherwise(None)).alias("resistance_headroom_pct"),
    ])
    return df.with_columns([
        (
            100
            - (pl.col("float_rotation_20d").fill_null(0) * 100).clip(0, 100)
            + (pl.when(pl.col("support_distance_pct").abs() < 0.03).then(10).otherwise(0))
        ).clip(0, 100).alias("dry_score"),
        (
            (pl.col("float_rotation_20d").fill_null(0) * 100).clip(0, 100)
            + (pl.when(pl.col("support_distance_pct") < -0.03).then(15).otherwise(0))
        ).clip(0, 100).alias("wet_score"),
    ])
