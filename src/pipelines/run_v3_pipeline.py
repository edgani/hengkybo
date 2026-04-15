from __future__ import annotations

from pathlib import Path

import polars as pl

from src.analytics.dry_wet_engine import build_dry_wet_features
from src.analytics.false_breakout import build_false_breakout_risk
from src.cleaning.quality_checks import completeness_score
from src.features.broker_inventory import build_broker_inventory, build_institutional_levels
from src.features.foreign_features import build_foreign_features
from src.features.phase_engine import build_phase_features
from src.features.price_volume import build_price_volume_features
from src.features.regime_features import build_regime_features
from src.ingestion.broker_summary import load_broker_summary
from src.ingestion.foreign_flow import load_foreign_flow
from src.ingestion.prices import load_prices
from src.models.adaptive_weights import apply_adaptive_weights, attach_drift_penalty, build_adaptive_weight_table
from src.models.broker_reclassifier import build_broker_reclassification, build_ticker_broker_alignment
from src.models.drift_monitor import build_score_drift_report
from src.scoring.final_verdict import build_final_verdict
from src.scoring.watchlist_ranker import map_verdict
from src.utils.config import load_yaml
from src.utils.io import ensure_dir
from src.utils.logging import get_logger


logger = get_logger(__name__)
EPS = 1e-9


def _normalize_score_expr(col: str) -> pl.Expr:
    return (((pl.col(col) - pl.col(col).min()) / (pl.col(col).max() - pl.col(col).min() + EPS)) * 100).fill_null(50)


def _load_intraday_optional(out_dir: Path) -> pl.DataFrame | None:
    parquet_path = out_dir / "intraday_signals.parquet"
    csv_path = out_dir / "latest_intraday_signals.csv"
    if parquet_path.exists():
        return pl.read_parquet(parquet_path)
    if csv_path.exists():
        return pl.read_csv(csv_path, try_parse_dates=True)
    return None


def run() -> None:
    cfg = load_yaml("config/data_sources.yaml")
    out_dir = ensure_dir("data/features")

    logger.info("Loading raw data for V3 pipeline")
    prices = load_prices(cfg["prices_daily"])
    broker = load_broker_summary(cfg["broker_summary_daily"])
    foreign = load_foreign_flow(cfg["foreign_daily"])

    logger.info("Building EOD feature blocks")
    pv = build_price_volume_features(prices)
    phase = build_phase_features(prices).select(["date", "ticker", "phase", "phase_confidence"])
    inv = build_broker_inventory(broker, decay=0.99)
    levels = build_institutional_levels(inv)
    fflow = build_foreign_features(foreign).select(["date", "ticker", "foreign_alignment_score"])
    dw = build_dry_wet_features(prices, levels).select([
        "date", "ticker", "dry_score", "wet_score", "institutional_support", "institutional_resistance", "resistance_headroom_pct"
    ])
    fb = build_false_breakout_risk(prices, levels)
    regime = build_regime_features(prices)
    broker_profiles = build_broker_reclassification(broker)
    broker_align = build_ticker_broker_alignment(inv, broker_profiles)

    logger.info("Joining EOD features")
    features = (
        pv.join(phase, on=["date", "ticker"], how="left")
          .join(fflow, on=["date", "ticker"], how="left")
          .join(dw, on=["date", "ticker"], how="left")
          .join(fb, on=["date", "ticker"], how="left")
          .join(broker_align, on=["date", "ticker"], how="left")
          .join(regime.select(["date", "regime_label", "regime_confidence", "breadth_score"]), on=["date"], how="left")
    )

    intraday = _load_intraday_optional(out_dir)
    if intraday is not None and not intraday.is_empty():
        keep_cols = [c for c in [
            "date", "ticker", "microstructure_strength_score", "microstructure_weakness_score",
            "spoof_risk_score", "tape_conviction_score", "breakout_tension_score",
            "transfer_suspicion_score", "offer_consumption_score"
        ] if c in intraday.columns]
        intraday = intraday.select(keep_cols)
        features = features.join(intraday, on=["date", "ticker"], how="left")

    logger.info("Constructing baseline scores")
    inv_strength_daily = (
        inv.group_by(["date", "ticker"]).agg(pl.col("inventory_strength").sum().alias("inventory_strength_sum"))
        .with_columns(_normalize_score_expr("inventory_strength_sum").alias("inventory_strength_norm"))
        .select(["date", "ticker", "inventory_strength_norm"])
    )
    features = features.join(inv_strength_daily, on=["date", "ticker"], how="left")

    features = features.with_columns([
        _normalize_score_expr("volume_lots").alias("volume_quality_score"),
        _normalize_score_expr("range_pct").alias("base_maturity_score"),
        _normalize_score_expr("dist_ma20").alias("phase_readiness_score"),
        (100 - _normalize_score_expr("dist_ma60").abs()).clip(0, 100).alias("price_resilience_score"),
        (100 - _normalize_score_expr("resistance_headroom_pct")).clip(0, 100).alias("resistance_clearance_score"),
        pl.col("microstructure_strength_score").fill_null(50),
        pl.col("microstructure_weakness_score").fill_null(50),
        (100 - pl.col("regime_confidence").fill_null(50) * 0.3).clip(20, 80).alias("macro_headwind_score"),
        pl.when(pl.col("phase").is_in(["MARKDOWN", "FAILED_BREAKOUT", "DISTRIBUTION"]))
          .then(70).otherwise(35).alias("phase_deterioration_score"),
        pl.when(pl.col("close") < pl.col("institutional_support") * 0.97).then(75).otherwise(35).alias("support_break_risk"),
        (
            0.45 * pl.col("breadth_score").fill_null(50)
            + 0.30 * pl.col("foreign_alignment_score").fill_null(50)
            + 0.25 * pl.col("broker_alignment_score").fill_null(50)
        ).clip(0, 100).alias("macro_alignment_score"),
        pl.lit(65.0).alias("module_agreement_score"),
        pl.lit(60.0).alias("feature_stability_score"),
    ])

    features = features.with_columns([
        (
            0.28 * pl.col("inventory_strength_norm")
            + 0.16 * pl.col("dry_score")
            + 0.12 * pl.col("phase_readiness_score")
            + 0.12 * pl.col("foreign_alignment_score").fill_null(50)
            + 0.12 * pl.col("price_resilience_score")
            + 0.20 * pl.col("broker_alignment_score").fill_null(50)
        ).clip(0, 100).alias("accumulation_quality_score"),
        (
            0.22 * pl.col("phase_readiness_score")
            + 0.18 * pl.col("base_maturity_score")
            + 0.18 * pl.col("volume_quality_score")
            + 0.15 * pl.col("resistance_clearance_score")
            + 0.12 * pl.col("dry_score")
            + 0.15 * pl.col("breakout_tension_score").fill_null(50)
        ).clip(0, 100).alias("breakout_integrity_score"),
        (
            0.24 * pl.col("wet_score")
            + 0.18 * pl.col("support_break_risk")
            + 0.16 * pl.col("false_breakout_risk")
            + 0.14 * (100 - pl.col("foreign_alignment_score").fill_null(50))
            + 0.12 * pl.col("phase_deterioration_score")
            + 0.16 * pl.col("transfer_suspicion_score").fill_null(50)
        ).clip(0, 100).alias("distribution_risk_score"),
        (
            0.40 * completeness_score(prices).select(pl.lit(100.0)).to_series()[0]
            + 0.30 * pl.col("module_agreement_score")
            + 0.30 * pl.col("feature_stability_score")
        ).clip(0, 100).alias("verdict_confidence"),
    ])

    features = map_verdict(features)

    logger.info("Building adaptive V3 layer")
    history_for_drift = features.select([
        "date", "ticker", "accumulation_quality_score", "breakout_integrity_score",
        "distribution_risk_score", "macro_alignment_score", "microstructure_strength_score"
    ]).sort(["date", "ticker"])
    drift_report = build_score_drift_report(history_for_drift, [
        "accumulation_quality_score", "breakout_integrity_score", "distribution_risk_score",
        "macro_alignment_score", "microstructure_strength_score"
    ])
    weight_table = build_adaptive_weight_table(regime.select(["date", "regime_label"]))
    weight_table = attach_drift_penalty(weight_table, drift_report)
    features = apply_adaptive_weights(features, weight_table)
    features = build_final_verdict(features)

    latest_date = features.select(pl.col("date").max()).item()
    latest = features.filter(pl.col("date") == latest_date).sort("adaptive_long_score", descending=True)
    latest_brokers = broker_profiles.filter(pl.col("date") == broker_profiles.select(pl.col("date").max()).item())

    logger.info("Writing V3 outputs")
    features.write_parquet(out_dir / "ticker_scores_v3.parquet")
    latest.write_csv(out_dir / "latest_watchlist_v3.csv")
    regime.write_csv(out_dir / "regime_daily.csv")
    drift_report.write_csv(out_dir / "drift_report.csv")
    latest_brokers.write_csv(out_dir / "broker_profiles_latest.csv")
    logger.info("V3 pipeline done. Latest date: %s | rows: %s", latest_date, latest.height)


if __name__ == "__main__":
    run()
