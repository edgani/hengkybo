import polars as pl


def build_false_breakout_risk(prices: pl.DataFrame, levels: pl.DataFrame) -> pl.DataFrame:
    df = prices.join(levels, on=["ticker", "date"], how="left")
    return df.with_columns([
        pl.when(pl.col("institutional_resistance").is_null())
          .then(50)
          .when(pl.col("close") > pl.col("institutional_resistance") * 1.01)
          .then(35)
          .when((pl.col("close") <= pl.col("institutional_resistance") * 1.01) & (pl.col("close") >= pl.col("institutional_resistance") * 0.98))
          .then(60)
          .otherwise(45)
          .alias("false_breakout_risk")
    ]).select(["date", "ticker", "false_breakout_risk"])
