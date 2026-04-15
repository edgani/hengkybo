import polars as pl
from src.features.broker_inventory import build_broker_inventory


def test_inventory_builds():
    df = pl.DataFrame({
        "date": ["2026-01-01", "2026-01-02"],
        "ticker": ["AAA", "AAA"],
        "broker_code": ["BK", "BK"],
        "buy_lot": [100.0, 50.0],
        "buy_value": [10000.0, 5200.0],
        "sell_lot": [0.0, 10.0],
        "sell_value": [0.0, 1050.0],
    }).with_columns(pl.col("date").str.to_date())
    out = build_broker_inventory(df)
    assert out.height == 2
    assert "net_inventory" in out.columns
