import polars as pl


def simple_hit_rate_drift(recent_scores: pl.DataFrame, baseline_hit_rate: float) -> float:
    if recent_scores.is_empty() or "hit" not in recent_scores.columns:
        return 0.0
    recent_hit_rate = recent_scores.select(pl.col("hit").mean()).item()
    return float(recent_hit_rate - baseline_hit_rate)
