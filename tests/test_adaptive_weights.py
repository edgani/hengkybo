import polars as pl

from src.models.adaptive_weights import build_adaptive_weight_table, apply_adaptive_weights


def test_adaptive_weight_application_produces_scores():
    regime = pl.DataFrame({"date": ["2026-04-15"], "regime_label": ["BULL"]}).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )
    weights = build_adaptive_weight_table(regime)
    feats = pl.DataFrame({
        "date": regime[0, "date"],
        "ticker": "AAA",
        "accumulation_quality_score": 80,
        "breakout_integrity_score": 75,
        "dry_score": 70,
        "microstructure_strength_score": 65,
        "macro_alignment_score": 60,
        "distribution_risk_score": 25,
        "wet_score": 20,
        "phase_deterioration_score": 30,
        "microstructure_weakness_score": 35,
        "macro_headwind_score": 40,
    })
    out = apply_adaptive_weights(feats, weights)
    assert "adaptive_long_score" in out.columns
    assert "adaptive_sell_score" in out.columns
