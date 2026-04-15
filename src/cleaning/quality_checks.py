import polars as pl


def completeness_score(df: pl.DataFrame, required_cols: list[str]) -> float:
    if df.is_empty():
        return 0.0
    existing = [c for c in required_cols if c in df.columns]
    if not existing:
        return 0.0
    null_rates = [df[c].null_count() / max(df.height, 1) for c in existing]
    return max(0.0, 1.0 - float(sum(null_rates) / len(null_rates)))
