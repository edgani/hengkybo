from __future__ import annotations

import polars as pl

from src.features.broker_taxonomy import build_broker_taxonomy_features


EPS = 1e-9


def _scale_0_100(expr: pl.Expr) -> pl.Expr:
    return (((expr - expr.min()) / (expr.max() - expr.min() + EPS)) * 100).fill_null(50)


def build_broker_reclassification(broker_summary: pl.DataFrame) -> pl.DataFrame:
    base = build_broker_taxonomy_features(broker_summary)
    base = base.with_columns([
        pl.col("directionality_ratio").rolling_mean(20).over("broker_code").fill_null(0).alias("directionality_20d"),
        pl.col("directionality_ratio").rolling_std(20).over("broker_code").fill_null(0).alias("directionality_vol_20d"),
        pl.col("net_lot").rolling_mean(20).over("broker_code").fill_null(0).alias("net_lot_20d"),
        pl.col("gross_activity_lot").rolling_mean(20).over("broker_code").fill_null(0).alias("gross_activity_20d"),
    ])

    scored = base.with_columns([
        _scale_0_100(pl.col("activity_20d")).over("date").alias("activity_rank_score"),
        _scale_0_100(pl.col("ticker_breadth_20d")).over("date").alias("breadth_rank_score"),
        _scale_0_100(pl.col("directionality_20d")).over("date").alias("directionality_rank_score"),
        (100 - _scale_0_100(pl.col("directionality_vol_20d")).over("date")).clip(0, 100).alias("stability_rank_score"),
    ]).with_columns([
        (
            0.30 * pl.col("activity_rank_score")
            + 0.20 * pl.col("breadth_rank_score")
            + 0.25 * pl.col("directionality_rank_score")
            + 0.25 * pl.col("stability_rank_score")
        ).clip(0, 100).alias("institutional_like_score"),
        (
            0.30 * (100 - pl.col("activity_rank_score"))
            + 0.20 * (100 - pl.col("breadth_rank_score"))
            + 0.25 * (100 - pl.col("stability_rank_score"))
            + 0.25 * (100 - pl.col("directionality_rank_score"))
        ).clip(0, 100).alias("retail_like_score"),
    ]).with_columns([
        (100 - (pl.col("institutional_like_score") - pl.col("retail_like_score")).abs()).clip(0, 100).alias("hybrid_score"),
        (100 - _scale_0_100(pl.col("directionality_vol_20d")).over("date")).clip(0, 100).alias("classification_stability_score"),
    ]).with_columns([
        pl.when((pl.col("institutional_like_score") >= pl.col("retail_like_score")) & (pl.col("institutional_like_score") >= pl.col("hybrid_score")))
          .then(pl.lit("INSTITUTIONAL_LIKE"))
          .when((pl.col("retail_like_score") >= pl.col("institutional_like_score")) & (pl.col("retail_like_score") >= pl.col("hybrid_score")))
          .then(pl.lit("RETAIL_LIKE"))
          .otherwise(pl.lit("HYBRID")).alias("adaptive_broker_label"),
        (
            0.55 * pl.max_horizontal(["institutional_like_score", "retail_like_score", "hybrid_score"])
            + 0.45 * pl.col("classification_stability_score")
        ).clip(0, 100).alias("broker_profile_confidence"),
    ])
    return scored.select([
        "date", "broker_code", "institutional_like_score", "retail_like_score", "hybrid_score",
        "classification_stability_score", "adaptive_broker_label", "broker_profile_confidence"
    ])


def build_ticker_broker_alignment(inventory: pl.DataFrame, broker_profiles: pl.DataFrame) -> pl.DataFrame:
    joined = inventory.join(broker_profiles, on=["date", "broker_code"], how="left")
    joined = joined.with_columns([
        pl.col("inventory_strength").fill_null(0).alias("inventory_strength"),
        pl.col("institutional_like_score").fill_null(50).alias("institutional_like_score"),
        pl.col("retail_like_score").fill_null(50).alias("retail_like_score"),
    ])

    out = (
        joined.group_by(["date", "ticker"]).agg([
            (pl.sum(pl.col("institutional_like_score") * pl.col("inventory_strength")) / (pl.sum("inventory_strength") + EPS)).alias("institutional_participation_score"),
            (pl.sum(pl.col("retail_like_score") * pl.col("inventory_strength")) / (pl.sum("inventory_strength") + EPS)).alias("retail_participation_score"),
            pl.col("broker_profile_confidence").mean().fill_null(50).alias("broker_profile_confidence"),
            pl.col("broker_code").n_unique().alias("active_brokers"),
        ])
        .with_columns([
            (pl.col("institutional_participation_score") - pl.col("retail_participation_score") + 50).clip(0, 100).alias("broker_alignment_score"),
            ((pl.col("active_brokers") / (pl.col("active_brokers") + 10)) * 100).clip(0, 100).alias("broker_participation_depth_score"),
        ])
    )
    return out
