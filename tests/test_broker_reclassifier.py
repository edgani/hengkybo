import polars as pl

from src.models.broker_reclassifier import build_broker_reclassification


def test_build_broker_reclassification_outputs_scores():
    df = pl.DataFrame({
        "date": ["2026-04-10", "2026-04-11", "2026-04-10", "2026-04-11"],
        "ticker": ["AAA", "AAA", "BBB", "BBB"],
        "broker_code": ["BK", "BK", "YP", "YP"],
        "buy_lot": [100, 120, 40, 30],
        "buy_value": [10000, 12100, 4000, 3000],
        "sell_lot": [20, 15, 60, 70],
        "sell_value": [2000, 1500, 6000, 7000],
        "gross_activity_lot": [120, 135, 100, 100],
        "net_lot": [80, 105, -20, -40],
    }).with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))

    out = build_broker_reclassification(df)
    assert "adaptive_broker_label" in out.columns
    assert "institutional_like_score" in out.columns
