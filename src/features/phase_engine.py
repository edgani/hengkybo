import polars as pl
from src.utils.enums import Phase


def build_phase_features(prices: pl.DataFrame) -> pl.DataFrame:
    df = prices.sort(["ticker", "date"]).with_columns([
        pl.col("close").rolling_mean(20).over("ticker").alias("ma20"),
        pl.col("close").rolling_mean(60).over("ticker").alias("ma60"),
        pl.col("volume_lots").rolling_mean(20).over("ticker").alias("vol20"),
        pl.col("high").rolling_max(60).over("ticker").alias("hh60"),
        pl.col("low").rolling_min(60).over("ticker").alias("ll60"),
    ])
    df = df.with_columns([
        ((pl.col("close") - pl.col("ma20")) / pl.when(pl.col("ma20") != 0).then(pl.col("ma20")).otherwise(None)).alias("dist20"),
        ((pl.col("close") - pl.col("ma60")) / pl.when(pl.col("ma60") != 0).then(pl.col("ma60")).otherwise(None)).alias("dist60"),
        ((pl.col("hh60") - pl.col("ll60")) / pl.when(pl.col("close") != 0).then(pl.col("close")).otherwise(None)).alias("range60_pct"),
        (pl.col("volume_lots") / pl.when(pl.col("vol20") != 0).then(pl.col("vol20")).otherwise(None)).alias("vol_ratio20"),
    ])
    return df.with_columns([
        pl.when((pl.col("dist20") > 0.03) & (pl.col("dist60") > 0.05) & (pl.col("vol_ratio20") >= 1.0))
          .then(pl.lit(Phase.MARKUP.value))
          .when((pl.col("dist20") > 0.00) & (pl.col("dist60") > 0.00) & (pl.col("range60_pct") < 0.25))
          .then(pl.lit(Phase.EARLY_MARKUP.value))
          .when((pl.col("dist20") < -0.03) & (pl.col("dist60") < -0.05))
          .then(pl.lit(Phase.MARKDOWN.value))
          .when((pl.col("range60_pct") < 0.18) & (pl.col("vol_ratio20") < 0.9))
          .then(pl.lit(Phase.COMPRESSION.value))
          .otherwise(pl.lit(Phase.NOISY_NEUTRAL.value))
          .alias("phase"),
        pl.when(pl.col("range60_pct").is_not_null())
          .then((1.0 - (pl.col("range60_pct") / 0.40)).clip(0.2, 0.95))
          .otherwise(0.5)
          .alias("phase_confidence")
    ])
