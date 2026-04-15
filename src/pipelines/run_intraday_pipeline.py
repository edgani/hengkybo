from __future__ import annotations

import polars as pl

from src.utils.config import load_yaml
from src.utils.io import ensure_dir
from src.utils.logging import get_logger
from src.ingestion.done_detail import load_done_detail
from src.ingestion.orderbook import load_orderbook
from src.features.done_detail_features import build_done_detail_features
from src.features.orderbook_features import build_orderbook_features
from src.analytics.transfer_suspicion import build_transfer_suspicion
from src.scoring.microstructure_score import build_microstructure_scores


logger = get_logger(__name__)


def run() -> None:
    cfg = load_yaml("config/data_sources.yaml")
    out_dir = ensure_dir("data/features")

    logger.info("Loading intraday raw data")
    done = load_done_detail(cfg["done_detail_intraday"])
    book = load_orderbook(cfg["orderbook_intraday"])

    logger.info("Building intraday feature blocks")
    done_f = build_done_detail_features(done)
    book_f = build_orderbook_features(book)
    transfer_f = build_transfer_suspicion(done_f, book_f)
    micro = build_microstructure_scores(done_f, book_f, transfer_f)

    micro.write_parquet(out_dir / "intraday_signals.parquet")
    latest_date = micro.select(pl.col("date").max()).item()
    latest = micro.filter(pl.col("date") == latest_date).sort("microstructure_strength_score", descending=True)
    latest.write_csv(out_dir / "latest_intraday_signals.csv")

    # optional combined watchlist if EOD features already exist
    eod_path = out_dir / "ticker_scores_daily.parquet"
    if eod_path.exists():
        eod = pl.read_parquet(eod_path)
        eod_latest_date = eod.select(pl.col("date").max()).item()
        eod_latest = eod.filter(pl.col("date") == eod_latest_date)
        combined = eod_latest.join(latest.select([
            "date", "ticker", "microstructure_strength_score", "microstructure_weakness_score",
            "spoof_risk_score", "tape_conviction_score", "breakout_tension_score",
            "transfer_suspicion_score", "offer_consumption_score"
        ]), on=["date", "ticker"], how="left")
        combined = combined.with_columns([
            (
                0.75 * pl.col("verdict_score_long") + 0.25 * pl.col("microstructure_strength_score").fill_null(50)
            ).clip(0, 100).alias("intraday_upgraded_long_score"),
            (
                0.70 * pl.col("distribution_risk_score") + 0.30 * pl.col("microstructure_weakness_score").fill_null(50)
            ).clip(0, 100).alias("intraday_upgraded_sell_score"),
            pl.when(pl.col("microstructure_strength_score").fill_null(50) >= 65)
              .then(pl.lit("Microstructure confirms"))
              .when(pl.col("microstructure_weakness_score").fill_null(50) >= 65)
              .then(pl.lit("Microstructure weak"))
              .otherwise(pl.lit("Microstructure mixed")).alias("intraday_note")
        ])
        combined.write_csv(out_dir / "combined_watchlist_intraday.csv")
        logger.info("Combined EOD + intraday watchlist written")

    logger.info("Intraday pipeline done. Latest date: %s | rows: %s", latest_date, latest.height)


if __name__ == "__main__":
    run()
