import polars as pl
from src.features.done_detail_features import build_done_detail_features
from src.features.orderbook_features import build_orderbook_features


def test_done_detail_features_basic():
    df = pl.DataFrame({
        "timestamp": ["2026-04-15 09:00:01", "2026-04-15 09:00:01", "2026-04-15 09:00:02"],
        "trade_date": ["2026-04-15", "2026-04-15", "2026-04-15"],
        "ticker": ["AAA", "AAA", "AAA"],
        "price": [100.0, 100.0, 101.0],
        "lot": [10.0, 20.0, 30.0],
        "buyer_broker": ["AI", "AI", "BK"],
        "seller_broker": ["YP", "PD", "XC"],
        "side_aggressor": ["BUY", "BUY", "BUY"],
        "trade_seq": [1, 2, 3],
        "value": [100000.0, 200000.0, 303000.0],
    })
    out = build_done_detail_features(df)
    assert out.height == 1
    assert "tape_conviction_score" in out.columns


def test_orderbook_features_basic():
    base = {
        "timestamp": ["2026-04-15 09:00:00"],
        "trade_date": ["2026-04-15"],
        "ticker": ["AAA"],
    }
    for i in range(1, 6):
        base[f"bid_{i}_price"] = [100 - (i - 1)]
        base[f"bid_{i}_lot"] = [1000 - (i - 1) * 50]
        base[f"offer_{i}_price"] = [101 + (i - 1)]
        base[f"offer_{i}_lot"] = [800 - (i - 1) * 50]
    out = build_orderbook_features(pl.DataFrame(base))
    assert out.height == 1
    assert "breakout_tension_score" in out.columns
