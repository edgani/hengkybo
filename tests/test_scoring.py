import polars as pl
from src.scoring.watchlist_ranker import map_verdict


def test_map_verdict():
    df = pl.DataFrame({
        "accumulation_quality_score": [80.0],
        "breakout_integrity_score": [78.0],
        "dry_score": [70.0],
        "microstructure_strength_score": [50.0],
        "macro_alignment_score": [60.0],
        "distribution_risk_score": [20.0],
        "wet_score": [20.0],
        "phase_deterioration_score": [20.0],
        "microstructure_weakness_score": [30.0],
        "macro_headwind_score": [40.0],
    })
    out = map_verdict(df)
    assert out["verdict"][0] == "READY_LONG"
