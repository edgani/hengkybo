import polars as pl


def build_distribution_risk(features: pl.DataFrame) -> pl.DataFrame:
    return features.with_columns([
        (
            0.30 * pl.col("wet_score")
            + 0.20 * pl.col("support_break_risk")
            + 0.20 * pl.col("false_breakout_risk")
            + 0.15 * (100 - pl.col("foreign_alignment_score"))
            + 0.15 * pl.col("phase_deterioration_score")
        ).clip(0, 100).alias("distribution_risk_score")
    ])
