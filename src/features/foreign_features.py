import polars as pl


def build_foreign_features(foreign_daily: pl.DataFrame) -> pl.DataFrame:
    df = foreign_daily.sort(["ticker", "date"])
    return df.with_columns([
        pl.col("foreign_net_lot").rolling_sum(5).over("ticker").alias("foreign_net_5d"),
        pl.col("foreign_net_lot").rolling_sum(20).over("ticker").alias("foreign_net_20d"),
    ]).with_columns([
        pl.when(pl.col("foreign_net_20d") > 0).then(70)
          .when(pl.col("foreign_net_20d") < 0).then(30)
          .otherwise(50)
          .alias("foreign_alignment_score")
    ])
