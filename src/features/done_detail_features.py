from __future__ import annotations

import polars as pl


EPS = 1e-9


def build_done_detail_features(done: pl.DataFrame) -> pl.DataFrame:
    if done.is_empty():
        return pl.DataFrame(schema={"trade_date": pl.Date, "ticker": pl.Utf8})

    sec = done.with_columns([
        pl.col("timestamp").cast(pl.Utf8).str.slice(0, 19).alias("ts_sec"),
        pl.when(pl.col("side_aggressor") == "BUY").then(pl.col("lot")).otherwise(0.0).alias("buy_aggr_lot"),
        pl.when(pl.col("side_aggressor") == "SELL").then(pl.col("lot")).otherwise(0.0).alias("sell_aggr_lot"),
    ])

    burst = (
        sec.group_by(["trade_date", "ticker", "ts_sec"]).agg([
            pl.len().alias("trades_in_sec"),
            pl.col("lot").sum().alias("lot_in_sec"),
            pl.col("price").max().alias("sec_high"),
            pl.col("price").min().alias("sec_low"),
            pl.col("buy_aggr_lot").sum().alias("buy_aggr_lot_sec"),
            pl.col("sell_aggr_lot").sum().alias("sell_aggr_lot_sec"),
        ])
        .with_columns([
            (pl.col("sec_high") - pl.col("sec_low")).alias("sec_range"),
            (pl.col("trades_in_sec") >= 3).cast(pl.Int64).alias("burst_flag"),
            ((pl.col("buy_aggr_lot_sec") > pl.col("sell_aggr_lot_sec")) & (pl.col("sec_range") > 0)).cast(pl.Int64).alias("buy_sweep_flag"),
            ((pl.col("sell_aggr_lot_sec") > pl.col("buy_aggr_lot_sec")) & (pl.col("sec_range") > 0)).cast(pl.Int64).alias("sell_sweep_flag"),
        ])
    )

    pair_flow = (
        sec.group_by(["trade_date", "ticker", "buyer_broker", "seller_broker"])
        .agg(pl.col("lot").sum().alias("pair_lot"))
        .sort(["trade_date", "ticker", "pair_lot"], descending=[False, False, True])
    )

    pair_summary = (
        pair_flow.group_by(["trade_date", "ticker"]).agg([
            pl.col("pair_lot").sum().alias("pair_total_lot"),
            pl.col("pair_lot").max().alias("top_pair_lot"),
            pl.len().alias("active_pairs"),
        ])
        .with_columns([
            (pl.col("top_pair_lot") / (pl.col("pair_total_lot") + EPS)).alias("top_pair_share"),
            (1.0 / (pl.col("active_pairs") + EPS)).alias("pair_concentration_floor"),
        ])
    )

    daily = (
        sec.group_by(["trade_date", "ticker"]).agg([
            pl.len().alias("trade_count"),
            pl.col("lot").sum().alias("total_lot"),
            pl.col("value").sum().alias("total_value"),
            pl.col("buy_aggr_lot").sum().alias("buy_aggr_lot"),
            pl.col("sell_aggr_lot").sum().alias("sell_aggr_lot"),
            pl.col("price").mean().alias("avg_trade_price"),
            pl.col("price").first().alias("first_trade_price"),
            pl.col("price").last().alias("last_trade_price"),
            pl.col("lot").mean().alias("avg_lot"),
            pl.col("lot").max().alias("max_lot"),
        ])
        .join(
            burst.group_by(["trade_date", "ticker"]).agg([
                pl.col("burst_flag").sum().alias("burst_count"),
                pl.col("buy_sweep_flag").sum().alias("buy_sweep_count"),
                pl.col("sell_sweep_flag").sum().alias("sell_sweep_count"),
                pl.col("trades_in_sec").max().alias("max_trades_same_sec"),
            ]),
            on=["trade_date", "ticker"],
            how="left",
        )
        .join(pair_summary, on=["trade_date", "ticker"], how="left")
        .with_columns([
            (pl.col("buy_aggr_lot") / (pl.col("total_lot") + EPS)).alias("buy_aggr_ratio"),
            (pl.col("sell_aggr_lot") / (pl.col("total_lot") + EPS)).alias("sell_aggr_ratio"),
            ((pl.col("last_trade_price") - pl.col("first_trade_price")) / (pl.col("first_trade_price") + EPS) * 100).alias("intraday_price_move_pct"),
            (pl.col("burst_count") / (pl.col("trade_count") + EPS)).alias("burst_ratio"),
            (pl.col("max_trades_same_sec") / (pl.col("trade_count") + EPS) * 100).clip(0, 100).alias("child_order_cluster_score"),
            (
                (pl.col("buy_aggr_ratio") * 55)
                + ((pl.col("buy_sweep_count") / (pl.col("buy_sweep_count") + pl.col("sell_sweep_count") + 1)) * 25)
                + ((1 - pl.col("top_pair_share").fill_null(0.5)) * 20)
            ).clip(0, 100).alias("tape_conviction_score"),
        ])
        .rename({"trade_date": "date"})
    )
    return daily
