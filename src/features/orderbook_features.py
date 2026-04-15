from __future__ import annotations

import polars as pl


EPS = 1e-9


def build_orderbook_features(book: pl.DataFrame) -> pl.DataFrame:
    if book.is_empty():
        return pl.DataFrame(schema={"trade_date": pl.Date, "ticker": pl.Utf8})

    weighted_bid = sum(pl.col(f"bid_{i}_lot") * (6 - i) for i in range(1, 6))
    weighted_offer = sum(pl.col(f"offer_{i}_lot") * (6 - i) for i in range(1, 6))
    top3_bid = sum(pl.col(f"bid_{i}_lot") for i in range(1, 4))
    top3_offer = sum(pl.col(f"offer_{i}_lot") for i in range(1, 4))

    snap = book.with_columns([
        ((pl.col("offer_1_price") - pl.col("bid_1_price")) / ((pl.col("offer_1_price") + pl.col("bid_1_price")) / 2 + EPS) * 10000).alias("spread_bps"),
        ((top3_bid - top3_offer) / (top3_bid + top3_offer + EPS)).alias("top3_imbalance"),
        ((weighted_bid - weighted_offer) / (weighted_bid + weighted_offer + EPS)).alias("depth_imbalance"),
        ((pl.col("bid_1_lot") > pl.col("offer_1_lot"))).cast(pl.Int64).alias("best_bid_dominant"),
        ((pl.col("offer_1_lot") > pl.col("bid_1_lot"))).cast(pl.Int64).alias("best_offer_dominant"),
        (((pl.col("offer_1_price") + pl.col("bid_1_price")) / 2)).alias("mid_price"),
        (((pl.col("bid_1_price") > pl.col("bid_2_price")) & (pl.col("bid_2_price") > pl.col("bid_3_price"))).cast(pl.Int64)).alias("clean_bid_ladder"),
        (((pl.col("offer_1_price") < pl.col("offer_2_price")) & (pl.col("offer_2_price") < pl.col("offer_3_price"))).cast(pl.Int64)).alias("clean_offer_ladder"),
        ((pl.col("offer_1_lot") >= top3_offer * 0.5)).cast(pl.Int64).alias("offer_wall_flag"),
        ((pl.col("bid_1_lot") >= top3_bid * 0.5)).cast(pl.Int64).alias("bid_wall_flag"),
    ])

    by_day = (
        snap.group_by(["trade_date", "ticker"]).agg([
            pl.col("spread_bps").mean().alias("avg_spread_bps"),
            pl.col("spread_bps").std().fill_null(0).alias("spread_bps_std"),
            pl.col("top3_imbalance").mean().alias("avg_top3_imbalance"),
            pl.col("depth_imbalance").mean().alias("avg_depth_imbalance"),
            pl.col("best_bid_dominant").mean().alias("best_bid_dominant_ratio"),
            pl.col("best_offer_dominant").mean().alias("best_offer_dominant_ratio"),
            pl.col("clean_bid_ladder").mean().alias("clean_bid_ladder_ratio"),
            pl.col("clean_offer_ladder").mean().alias("clean_offer_ladder_ratio"),
            pl.col("offer_wall_flag").mean().alias("offer_wall_persistence"),
            pl.col("bid_wall_flag").mean().alias("bid_wall_persistence"),
            pl.col("mid_price").first().alias("first_mid"),
            pl.col("mid_price").last().alias("last_mid"),
        ])
        .with_columns([
            ((pl.col("last_mid") - pl.col("first_mid")) / (pl.col("first_mid") + EPS) * 100).alias("mid_move_pct"),
            (50 + pl.col("avg_top3_imbalance") * 50 - pl.col("avg_spread_bps").clip(0, 50) * 0.75).clip(0, 100).alias("genuine_support_score"),
            (50 - pl.col("avg_top3_imbalance") * 50 - pl.col("avg_spread_bps").clip(0, 50) * 0.75).clip(0, 100).alias("genuine_supply_score"),
            (
                40 * (1 - pl.col("offer_wall_persistence"))
                + 35 * pl.col("best_bid_dominant_ratio")
                + 25 * (1 - (pl.col("avg_spread_bps") / (pl.col("avg_spread_bps") + 5)))
            ).clip(0, 100).alias("breakout_tension_score"),
            (
                35 * pl.col("offer_wall_persistence")
                + 25 * pl.col("best_offer_dominant_ratio")
                + 20 * (pl.col("spread_bps_std") / (pl.col("spread_bps_std") + 3))
                + 20 * (1 - (pl.col("mid_move_pct").abs() / (pl.col("mid_move_pct").abs() + 1)))
            ).clip(0, 100).alias("spoof_risk_score"),
        ])
        .rename({"trade_date": "date"})
    )
    return by_day
