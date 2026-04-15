import polars as pl


EPS = 1e-9


def build_broker_inventory(broker_summary: pl.DataFrame, decay: float = 0.99) -> pl.DataFrame:
    """Approximate rolling broker inventory using exponential decay.

    This is an inference proxy, not true custody data.
    """
    required = {"date", "ticker", "broker_code", "buy_lot", "buy_value", "sell_lot", "sell_value"}
    missing = required - set(broker_summary.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    pdf = broker_summary.sort(["ticker", "broker_code", "date"]).to_pandas()
    out = []
    for (ticker, broker_code), g in pdf.groupby(["ticker", "broker_code"], sort=False):
        buy_lot_state = sell_lot_state = 0.0
        buy_val_state = sell_val_state = 0.0
        for row in g.itertuples(index=False):
            buy_lot_state = decay * buy_lot_state + float(row.buy_lot)
            sell_lot_state = decay * sell_lot_state + float(row.sell_lot)
            buy_val_state = decay * buy_val_state + float(row.buy_value)
            sell_val_state = decay * sell_val_state + float(row.sell_value)
            net_inventory = buy_lot_state - sell_lot_state
            avg_buy = buy_val_state / max(buy_lot_state, EPS)
            avg_sell = sell_val_state / max(sell_lot_state, EPS)
            out.append({
                "date": row.date,
                "ticker": ticker,
                "broker_code": broker_code,
                "decay_buy_lot": buy_lot_state,
                "decay_sell_lot": sell_lot_state,
                "decay_buy_value": buy_val_state,
                "decay_sell_value": sell_val_state,
                "net_inventory": net_inventory,
                "avg_buy_est": avg_buy,
                "avg_sell_est": avg_sell,
                "inventory_strength": abs(net_inventory),
            })
    return pl.from_pandas(__import__("pandas").DataFrame(out))


def build_institutional_levels(inventory: pl.DataFrame, top_n: int = 5) -> pl.DataFrame:
    pos = (
        inventory.filter(pl.col("net_inventory") > 0)
        .sort(["ticker", "date", "inventory_strength"], descending=[False, False, True])
        .group_by(["ticker", "date"]).head(top_n)
        .group_by(["ticker", "date"]) 
        .agg([
            (pl.sum(pl.col("avg_buy_est") * pl.col("inventory_strength")) / pl.sum("inventory_strength")).alias("institutional_support"),
            pl.sum("inventory_strength").alias("acc_inventory_strength"),
        ])
    )
    neg = (
        inventory.filter(pl.col("decay_sell_lot") > 0)
        .with_columns((pl.col("decay_sell_lot") - pl.col("decay_buy_lot")).abs().alias("distribution_strength"))
        .sort(["ticker", "date", "distribution_strength"], descending=[False, False, True])
        .group_by(["ticker", "date"]).head(top_n)
        .group_by(["ticker", "date"]) 
        .agg([
            (pl.sum(pl.col("avg_sell_est") * pl.col("distribution_strength")) / pl.sum("distribution_strength")).alias("institutional_resistance"),
            pl.sum("distribution_strength").alias("dist_inventory_strength"),
        ])
    )
    return pos.join(neg, on=["ticker", "date"], how="full")
