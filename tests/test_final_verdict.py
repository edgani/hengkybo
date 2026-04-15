import polars as pl

from src.scoring.final_verdict import build_final_verdict


def test_final_verdict_maps_ready_long():
    df = pl.DataFrame({
        "adaptive_long_score": [85],
        "adaptive_sell_score": [20],
        "ready_long_threshold": [72],
        "watch_threshold": [58],
        "sell_threshold": [78],
        "distribution_risk_score": [25],
        "verdict_confidence": [80],
    })
    out = build_final_verdict(df)
    assert out[0, "verdict_v3"] == "READY_LONG"
