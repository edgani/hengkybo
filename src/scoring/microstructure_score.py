from __future__ import annotations

import polars as pl


def build_microstructure_scores(done_features: pl.DataFrame, orderbook_features: pl.DataFrame, transfer_features: pl.DataFrame) -> pl.DataFrame:
    df = done_features.join(orderbook_features, on=["date", "ticker"], how="outer").join(transfer_features, on=["date", "ticker"], how="left")
    return df.with_columns([
        (
            0.30 * pl.col("tape_conviction_score").fill_null(50)
            + 0.25 * pl.col("genuine_support_score").fill_null(50)
            + 0.20 * pl.col("breakout_tension_score").fill_null(50)
            + 0.10 * (pl.col("buy_aggr_ratio").fill_null(0.5) * 100)
            + 0.15 * (100 - pl.col("transfer_suspicion_score").fill_null(50))
        ).clip(0, 100).alias("microstructure_strength_score"),
        (
            0.25 * pl.col("genuine_supply_score").fill_null(50)
            + 0.20 * pl.col("spoof_risk_score").fill_null(50)
            + 0.20 * pl.col("transfer_suspicion_score").fill_null(50)
            + 0.15 * (pl.col("sell_aggr_ratio").fill_null(0.5) * 100)
            + 0.20 * (100 - pl.col("breakout_tension_score").fill_null(50))
        ).clip(0, 100).alias("microstructure_weakness_score"),
        pl.col("spoof_risk_score").fill_null(50),
        pl.col("tape_conviction_score").fill_null(50),
        pl.col("breakout_tension_score").fill_null(50),
        pl.col("transfer_suspicion_score").fill_null(50),
        (pl.col("buy_aggr_ratio").fill_null(0.5) * 100).alias("offer_consumption_score"),
    ])
