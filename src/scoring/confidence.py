import polars as pl


def build_confidence(features: pl.DataFrame) -> pl.DataFrame:
    return features.with_columns([
        (
            0.40 * pl.col("data_completeness_score")
            + 0.30 * pl.col("module_agreement_score")
            + 0.30 * pl.col("feature_stability_score")
        ).clip(0, 100).alias("verdict_confidence")
    ])
