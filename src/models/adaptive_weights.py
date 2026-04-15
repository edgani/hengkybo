from __future__ import annotations

from pathlib import Path

import polars as pl

from src.utils.config import load_yaml


DEFAULT_CONFIG = Path("config/adaptive.yaml")


def build_adaptive_weight_table(regime_df: pl.DataFrame, config_path: str | Path = DEFAULT_CONFIG) -> pl.DataFrame:
    cfg = load_yaml(config_path)
    profiles = cfg["regime_profiles"]
    rows = []
    for row in regime_df.iter_rows(named=True):
        profile = profiles.get(row["regime_label"], profiles["CHOP"])
        rows.append({"date": row["date"], "regime_label": row["regime_label"], **profile})
    return pl.DataFrame(rows)


def attach_drift_penalty(weight_df: pl.DataFrame, drift_report: pl.DataFrame | None) -> pl.DataFrame:
    if drift_report is None or drift_report.is_empty():
        return weight_df.with_columns(pl.lit(0.0).alias("drift_penalty"))

    penalty = drift_report.select(pl.col("drift_penalty").mean().fill_null(0)).item()
    return weight_df.with_columns(pl.lit(float(penalty)).alias("drift_penalty"))


def apply_adaptive_weights(features: pl.DataFrame, weight_df: pl.DataFrame) -> pl.DataFrame:
    df = features.join(weight_df, on=["date"], how="left")
    df = df.with_columns([
        pl.col("drift_penalty").fill_null(0.0).alias("drift_penalty"),
        (1 - pl.col("drift_penalty")).clip(0.70, 1.00).alias("signal_multiplier"),
    ])

    return df.with_columns([
        (
            (
                pl.col("accumulation_quality_weight") * pl.col("accumulation_quality_score")
                + pl.col("breakout_integrity_weight") * pl.col("breakout_integrity_score")
                + pl.col("dry_score_weight") * pl.col("dry_score")
                + pl.col("microstructure_strength_weight") * pl.col("microstructure_strength_score")
                + pl.col("macro_alignment_weight") * pl.col("macro_alignment_score")
            ) * pl.col("signal_multiplier")
            - (pl.col("distribution_penalty_weight") * pl.col("distribution_risk_score"))
        ).clip(0, 100).alias("adaptive_long_score"),
        (
            (
                0.45 * pl.col("distribution_risk_score")
                + 0.20 * pl.col("wet_score")
                + 0.15 * pl.col("phase_deterioration_score")
                + 0.10 * pl.col("microstructure_weakness_score")
                + 0.10 * pl.col("macro_headwind_score")
            ) * (1 + pl.col("drift_penalty"))
        ).clip(0, 100).alias("adaptive_sell_score"),
    ])
