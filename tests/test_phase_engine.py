import polars as pl
from src.features.phase_engine import build_phase_features


def test_phase_engine_runs():
    df = pl.DataFrame({
        "date": [f"2026-01-{i:02d}" for i in range(1, 31)],
        "ticker": ["AAA"] * 30,
        "open": list(range(10, 40)),
        "high": list(range(11, 41)),
        "low": list(range(9, 39)),
        "close": list(range(10, 40)),
        "volume_shares": [1000] * 30,
        "volume_lots": [10] * 30,
    }).with_columns(pl.col("date").str.to_date())
    out = build_phase_features(df)
    assert "phase" in out.columns
    assert "phase_confidence" in out.columns
