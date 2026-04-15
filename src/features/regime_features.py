from __future__ import annotations

import polars as pl


EPS = 1e-9


def build_regime_features(prices: pl.DataFrame) -> pl.DataFrame:
    prices = prices.sort(["ticker", "date"]).with_columns([
        pl.col("close").pct_change().over("ticker").alias("ret_1d"),
        (pl.col("close") / pl.col("close").shift(5).over("ticker") - 1).alias("ret_5d"),
        (((pl.col("high") - pl.col("low")) / (pl.col("close") + EPS)) * 100).alias("range_pct"),
        pl.col("close").rolling_mean(20).over("ticker").alias("ma20"),
        pl.col("volume_shares").rolling_mean(20).over("ticker").alias("vol20"),
    ]).with_columns([
        (pl.col("close") > pl.col("ma20")).cast(pl.Float64).alias("above_ma20"),
        (pl.col("volume_shares") > pl.col("vol20")).cast(pl.Float64).alias("volume_expansion"),
        (pl.col("ret_1d") > 0).cast(pl.Float64).alias("up_day"),
    ])

    daily = (
        prices.group_by("date")
        .agg([
            pl.col("ret_1d").mean().fill_null(0).alias("market_ret_1d"),
            pl.col("ret_5d").mean().fill_null(0).alias("market_ret_5d"),
            pl.col("range_pct").mean().fill_null(0).alias("avg_range_pct"),
            pl.col("above_ma20").mean().fill_null(0).alias("breadth_above_ma20"),
            pl.col("up_day").mean().fill_null(0).alias("up_day_ratio"),
            pl.col("volume_expansion").mean().fill_null(0).alias("volume_expansion_ratio"),
            pl.col("ticker").n_unique().alias("coverage_tickers"),
        ])
        .with_columns([
            ((pl.col("market_ret_5d") * 2500) + (pl.col("breadth_above_ma20") * 50)).clip(0, 100).alias("bull_score"),
            (((-pl.col("market_ret_5d")) * 2500) + ((1 - pl.col("breadth_above_ma20")) * 50)).clip(0, 100).alias("bear_score"),
            (
                100
                - (pl.col("market_ret_5d").abs() * 2500).clip(0, 100)
                + ((1 - (pl.col("avg_range_pct") / (pl.col("avg_range_pct") + 2))) * 25)
            ).clip(0, 100).alias("chop_score"),
        ])
        .with_columns([
            pl.when((pl.col("bull_score") >= pl.col("bear_score")) & (pl.col("bull_score") >= pl.col("chop_score")))
            .then(pl.lit("BULL"))
            .when((pl.col("bear_score") >= pl.col("bull_score")) & (pl.col("bear_score") >= pl.col("chop_score")))
            .then(pl.lit("BEAR"))
            .otherwise(pl.lit("CHOP")).alias("regime_label"),
            pl.max_horizontal(["bull_score", "bear_score", "chop_score"]).alias("regime_confidence"),
            ((pl.col("breadth_above_ma20") + pl.col("up_day_ratio")) * 50).clip(0, 100).alias("breadth_score"),
        ])
    )
    return daily
