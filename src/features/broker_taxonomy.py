import polars as pl


def build_broker_taxonomy_features(broker_summary: pl.DataFrame) -> pl.DataFrame:
    df = broker_summary.sort(["broker_code", "ticker", "date"])
    agg = (
        df.group_by(["date", "broker_code"])
        .agg([
            pl.col("buy_lot").sum().alias("buy_lot"),
            pl.col("sell_lot").sum().alias("sell_lot"),
            pl.col("gross_activity_lot").sum().alias("gross_activity_lot"),
            pl.col("net_lot").sum().alias("net_lot"),
            pl.col("ticker").n_unique().alias("ticker_breadth"),
        ])
        .sort(["broker_code", "date"])
    )
    return agg.with_columns([
        pl.when(pl.col("gross_activity_lot") > 0)
          .then(pl.col("net_lot").abs() / pl.col("gross_activity_lot"))
          .otherwise(0.0)
          .alias("directionality_ratio"),
        pl.col("gross_activity_lot").rolling_mean(20).over("broker_code").alias("activity_20d"),
        pl.col("ticker_breadth").rolling_mean(20).over("broker_code").alias("ticker_breadth_20d"),
    ])
