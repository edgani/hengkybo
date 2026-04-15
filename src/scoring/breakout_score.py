import polars as pl


def build_breakout_integrity(features: pl.DataFrame) -> pl.DataFrame:
    return features.with_columns([
        (
            0.25 * pl.col("phase_readiness_score")
            + 0.20 * pl.col("base_maturity_score")
            + 0.20 * pl.col("accumulation_quality_score")
            + 0.20 * pl.col("resistance_clearance_score")
            + 0.15 * pl.col("volume_quality_score")
        ).clip(0, 100).alias("breakout_integrity_score")
    ])
