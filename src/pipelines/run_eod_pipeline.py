from __future__ import annotations

from pathlib import Path
import polars as pl

from src.utils.config import load_yaml
from src.utils.io import ensure_dir
from src.utils.logging import get_logger
from src.ingestion.prices import load_prices
from src.ingestion.broker_summary import load_broker_summary
from src.ingestion.foreign_flow import load_foreign_flow
from src.cleaning.quality_checks import completeness_score
from src.features.price_volume import build_price_volume_features
from src.features.broker_inventory import build_broker_inventory, build_institutional_levels
from src.features.phase_engine import build_phase_features
from src.features.foreign_features import build_foreign_features
from src.analytics.dry_wet_engine import build_dry_wet_features
from src.analytics.false_breakout import build_false_breakout_risk
from src.scoring.watchlist_ranker import map_verdict


logger = get_logger(__name__)


def _normalize_score_expr(col: str) -> pl.Expr:
    return (((pl.col(col) - pl.col(col).min()) / (pl.col(col).max() - pl.col(col).min() + 1e-9)) * 100).fill_null(50)


def run() -> None:
    cfg = load_yaml("config/data_sources.yaml")
    out_dir = ensure_dir("data/features")

    logger.info("Loading raw data")
    prices = load_prices(cfg["prices_daily"])
    broker = load_broker_summary(cfg["broker_summary_daily"])
    foreign = load_foreign_flow(cfg["foreign_daily"])

    logger.info("Building feature blocks")
    pv = build_price_volume_features(prices)
    phase = build_phase_features(prices).select(["date", "ticker", "phase", "phase_confidence"])
    inv = build_broker_inventory(broker, decay=0.99)
    levels = build_institutional_levels(inv)
    fflow = build_foreign_features(foreign).select(["date", "ticker", "foreign_alignment_score"])
    dw = build_dry_wet_features(prices, levels).select([
        "date", "ticker", "dry_score", "wet_score", "institutional_support", "institutional_resistance"
    ])
    fb = build_false_breakout_risk(prices, levels)

    logger.info("Joining features")
    features = (
        pv.join(phase, on=["date", "ticker"], how="left")
          .join(fflow, on=["date", "ticker"], how="left")
          .join(dw, on=["date", "ticker"], how="left")
          .join(fb, on=["date", "ticker"], how="left")
    )

    features = features.with_columns([
        _normalize_score_expr("volume_lots").alias("volume_quality_score"),
        _normalize_score_expr("range_pct").alias("base_maturity_score"),
        _normalize_score_expr("dist_ma20").alias("phase_readiness_score"),
        (100 - _normalize_score_expr("dist_ma60").abs()).clip(0, 100).alias("price_resilience_score"),
        (100 - _normalize_score_expr("resistance_headroom_pct")).clip(0, 100).alias("resistance_clearance_score"),
        pl.lit(50.0).alias("microstructure_strength_score"),
        pl.lit(50.0).alias("microstructure_weakness_score"),
        pl.lit(50.0).alias("macro_headwind_score"),
        pl.when(pl.col("phase").is_in(["MARKDOWN", "FAILED_BREAKOUT", "DISTRIBUTION"]))
          .then(70).otherwise(35).alias("phase_deterioration_score"),
        pl.when(pl.col("close") < pl.col("institutional_support") * 0.97).then(75).otherwise(35).alias("support_break_risk"),
        pl.lit(50.0).alias("macro_alignment_score"),
        pl.lit(50.0).alias("data_completeness_score"),
        pl.lit(60.0).alias("module_agreement_score"),
        pl.lit(60.0).alias("feature_stability_score"),
    ])

    inv_strength_daily = (
        inv.group_by(["date", "ticker"]).agg(pl.col("inventory_strength").sum().alias("inventory_strength_sum"))
        .with_columns(_normalize_score_expr("inventory_strength_sum").alias("inventory_strength_norm"))
        .select(["date", "ticker", "inventory_strength_norm"])
    )
    features = features.join(inv_strength_daily, on=["date", "ticker"], how="left")

    features = features.with_columns([
        (
            0.35 * pl.col("inventory_strength_norm")
            + 0.20 * pl.col("dry_score")
            + 0.15 * pl.col("phase_readiness_score")
            + 0.15 * pl.col("foreign_alignment_score").fill_null(50)
            + 0.15 * pl.col("price_resilience_score")
        ).clip(0, 100).alias("accumulation_quality_score"),
        (
            0.25 * pl.col("phase_readiness_score")
            + 0.20 * pl.col("base_maturity_score")
            + 0.20 * pl.col("volume_quality_score")
            + 0.20 * pl.col("resistance_clearance_score")
            + 0.15 * pl.col("dry_score")
        ).clip(0, 100).alias("breakout_integrity_score"),
        (
            0.30 * pl.col("wet_score")
            + 0.20 * pl.col("support_break_risk")
            + 0.20 * pl.col("false_breakout_risk")
            + 0.15 * (100 - pl.col("foreign_alignment_score").fill_null(50))
            + 0.15 * pl.col("phase_deterioration_score")
        ).clip(0, 100).alias("distribution_risk_score"),
        (
            pl.when(pl.col("close") >= pl.col("institutional_support")).then("Broker support holding")
            .otherwise("Below broker support zone")
        ).alias("why_now"),
        (
            pl.when(pl.col("institutional_support").is_not_null())
            .then((pl.lit("Close below ") + pl.col("institutional_support").round(2).cast(pl.Utf8)))
            .otherwise(pl.lit("No clear invalidation yet"))
        ).alias("invalidation"),
        (
            0.40 * pl.col("data_completeness_score")
            + 0.30 * pl.col("module_agreement_score")
            + 0.30 * pl.col("feature_stability_score")
        ).clip(0, 100).alias("verdict_confidence"),
    ])

    features = map_verdict(features)
    latest_date = features.select(pl.col("date").max()).item()
    latest = features.filter(pl.col("date") == latest_date).sort(["verdict", "verdict_score_long"], descending=[False, True])

    logger.info("Writing outputs")
    features.write_parquet(out_dir / "ticker_scores_daily.parquet")
    latest.write_csv(out_dir / "latest_watchlist.csv")
    logger.info("Done. Latest date: %s | rows: %s", latest_date, latest.height)


if __name__ == "__main__":
    run()
