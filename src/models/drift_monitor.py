from __future__ import annotations

import math
from pathlib import Path

import polars as pl

from src.utils.config import load_yaml


DEFAULT_CONFIG = Path("config/adaptive.yaml")


def simple_hit_rate_drift(recent_scores: pl.DataFrame, baseline_hit_rate: float) -> float:
    if recent_scores.is_empty() or "hit" not in recent_scores.columns:
        return 0.0
    recent_hit_rate = recent_scores.select(pl.col("hit").mean()).item()
    return float(recent_hit_rate - baseline_hit_rate)


def build_score_drift_report(
    history: pl.DataFrame,
    score_cols: list[str],
    recent_rows: int = 20,
    reference_rows: int = 60,
    config_path: str | Path = DEFAULT_CONFIG,
) -> pl.DataFrame:
    cfg = load_yaml(config_path)
    drift_cfg = cfg["drift"]
    rows = []

    for col in score_cols:
        if col not in history.columns:
            continue
        ser = history.select(col).drop_nulls()
        if ser.height < max(recent_rows + 5, 10):
            continue

        recent = ser.tail(recent_rows).to_series()
        ref = ser.head(max(ser.height - recent_rows, reference_rows)).tail(reference_rows).to_series()
        recent_mean = float(recent.mean()) if recent.len() else 0.0
        ref_mean = float(ref.mean()) if ref.len() else 0.0
        recent_std = float(recent.std()) if recent.len() else 0.0
        ref_std = float(ref.std()) if ref.len() else 0.0
        mean_shift = 0.0 if ref_std == 0 else abs(recent_mean - ref_mean) / max(ref_std, 1e-9)
        std_ratio = 1.0 if ref_std == 0 else recent_std / max(ref_std, 1e-9)

        penalty = 0.0
        if mean_shift >= drift_cfg["mean_shift_warn"]:
            penalty += min(mean_shift / max(drift_cfg["mean_shift_critical"], 1e-9), 1.0) * 0.12
        if std_ratio <= drift_cfg["std_ratio_warn_low"] or std_ratio >= drift_cfg["std_ratio_warn_high"]:
            penalty += 0.08
        penalty = min(penalty, drift_cfg["max_penalty"])

        rows.append({
            "score_name": col,
            "recent_mean": recent_mean,
            "reference_mean": ref_mean,
            "recent_std": recent_std,
            "reference_std": ref_std,
            "mean_shift_sigma": mean_shift,
            "std_ratio": std_ratio,
            "drift_penalty": penalty,
        })

    return pl.DataFrame(rows) if rows else pl.DataFrame({
        "score_name": [], "recent_mean": [], "reference_mean": [], "recent_std": [],
        "reference_std": [], "mean_shift_sigma": [], "std_ratio": [], "drift_penalty": []
    })
