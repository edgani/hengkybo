import polars as pl


def build_accumulation_quality(features: pl.DataFrame) -> pl.DataFrame:
    return features.with_columns([
        (
            0.35 * pl.col("inventory_strength_norm")
            + 0.20 * pl.col("dry_score")
            + 0.15 * pl.col("phase_readiness_score")
            + 0.15 * pl.col("foreign_alignment_score")
            + 0.15 * pl.col("price_resilience_score")
        ).clip(0, 100).alias("accumulation_quality_score")
    ])
