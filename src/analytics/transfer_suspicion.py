from __future__ import annotations

import polars as pl


EPS = 1e-9


def build_transfer_suspicion(done_features: pl.DataFrame, orderbook_features: pl.DataFrame | None = None) -> pl.DataFrame:
    df = done_features
    if orderbook_features is not None and not orderbook_features.is_empty():
        df = df.join(
            orderbook_features.select(["date", "ticker", "avg_spread_bps", "mid_move_pct", "spoof_risk_score"]),
            on=["date", "ticker"], how="left"
        )
    else:
        df = df.with_columns([
            pl.lit(10.0).alias("avg_spread_bps"),
            pl.lit(0.0).alias("mid_move_pct"),
            pl.lit(50.0).alias("spoof_risk_score"),
        ])

    return df.with_columns([
        (pl.col("total_lot") / (pl.col("trade_count") + EPS)).alias("lot_per_trade"),
        (
            45 * pl.col("top_pair_share").fill_null(0.5)
            + 20 * (1 - (pl.col("intraday_price_move_pct").abs() / (pl.col("intraday_price_move_pct").abs() + 1)))
            + 20 * (pl.col("avg_spread_bps") / (pl.col("avg_spread_bps") + 5))
            + 15 * (pl.col("spoof_risk_score").fill_null(50) / 100)
        ).clip(0, 100).alias("transfer_suspicion_score"),
        (
            60 * pl.col("top_pair_share").fill_null(0.5)
            + 40 * (1 - (pl.col("active_pairs") / (pl.col("active_pairs") + 5)))
        ).clip(0, 100).alias("pair_concentration_score"),
    ]).select(["date", "ticker", "transfer_suspicion_score", "pair_concentration_score"])
