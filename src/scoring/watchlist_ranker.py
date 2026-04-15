from __future__ import annotations

import polars as pl
from src.utils.enums import Verdict


def map_verdict(df: pl.DataFrame) -> pl.DataFrame:
    long_score = (
        0.30 * pl.col("accumulation_quality_score")
        + 0.25 * pl.col("breakout_integrity_score")
        + 0.15 * pl.col("dry_score")
        + 0.15 * pl.col("microstructure_strength_score")
        + 0.15 * pl.col("macro_alignment_score")
        - 0.30 * pl.col("distribution_risk_score")
    )
    sell_score = (
        0.35 * pl.col("distribution_risk_score")
        + 0.20 * pl.col("wet_score")
        + 0.15 * pl.col("phase_deterioration_score")
        + 0.15 * pl.col("microstructure_weakness_score")
        + 0.15 * pl.col("macro_headwind_score")
    )
    return df.with_columns([
        long_score.alias("verdict_score_long"),
        sell_score.alias("verdict_score_sell"),
    ]).with_columns([
        pl.when((pl.col("verdict_score_long") >= 75) & (pl.col("distribution_risk_score") <= 35))
          .then(pl.lit(Verdict.READY_LONG.value))
          .when((pl.col("verdict_score_long") >= 60) & (pl.col("breakout_integrity_score") < 65))
          .then(pl.lit(Verdict.WATCH.value))
          .when(pl.col("verdict_score_sell") >= 75)
          .then(pl.lit(Verdict.TRIM_SELL.value))
          .when(pl.col("distribution_risk_score") >= 70)
          .then(pl.lit(Verdict.AVOID.value))
          .otherwise(pl.lit(Verdict.NEUTRAL.value))
          .alias("verdict")
    ])
