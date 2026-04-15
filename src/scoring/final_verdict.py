from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.config import load_yaml
from src.utils.enums import Verdict


DEFAULT_CONFIG = Path("config/adaptive.yaml")


def build_final_verdict(df: pl.DataFrame, config_path: str | Path = DEFAULT_CONFIG) -> pl.DataFrame:
    cfg = load_yaml(config_path)
    thresholds = cfg["thresholds"]

    out = df.with_columns([
        pl.when(pl.col("adaptive_long_score") >= pl.col("ready_long_threshold"))
          .then(pl.lit("LONG_SETUP_STRONG"))
          .when(pl.col("adaptive_long_score") >= pl.col("watch_threshold"))
          .then(pl.lit("LONG_SETUP_FORMING"))
          .when(pl.col("adaptive_sell_score") >= pl.col("sell_threshold"))
          .then(pl.lit("SELL_PRESSURE_HIGH"))
          .otherwise(pl.lit("MIXED")).alias("setup_state")
    ]).with_columns([
        pl.when((pl.col("adaptive_sell_score") >= pl.col("sell_threshold")) | (pl.col("distribution_risk_score") >= thresholds["hard_sell_distribution_risk"]))
          .then(pl.lit(Verdict.TRIM_SELL.value))
          .when(pl.col("distribution_risk_score") >= thresholds["avoid_distribution_risk"]) 
          .then(pl.lit(Verdict.AVOID.value))
          .when((pl.col("adaptive_long_score") >= pl.col("ready_long_threshold")) & (pl.col("distribution_risk_score") <= 40))
          .then(pl.lit(Verdict.READY_LONG.value))
          .when(pl.col("adaptive_long_score") >= pl.col("watch_threshold"))
          .then(pl.lit(Verdict.WATCH.value))
          .otherwise(pl.lit(Verdict.NEUTRAL.value)).alias("verdict_v3")
    ]).with_columns([
        pl.when(pl.col("verdict_confidence") < thresholds["low_confidence_cutoff"])
          .then(pl.col("verdict_v3") + pl.lit("_LOW_CONFIDENCE"))
          .otherwise(pl.col("verdict_v3")).alias("verdict_v3_display"),
        pl.when(pl.col("verdict_v3") == Verdict.READY_LONG.value)
          .then(pl.lit("Accumulation, phase, and adaptive regime weights align."))
          .when(pl.col("verdict_v3") == Verdict.WATCH.value)
          .then(pl.lit("Structure improving, but confirmation is not complete yet."))
          .when(pl.col("verdict_v3") == Verdict.AVOID.value)
          .then(pl.lit("Distribution risk is too high relative to reward."))
          .when(pl.col("verdict_v3") == Verdict.TRIM_SELL.value)
          .then(pl.lit("Risk engine sees elevated sell pressure and weak protection."))
          .otherwise(pl.lit("Mixed evidence; no strong edge right now.")).alias("verdict_reason"),
    ])
    return out
