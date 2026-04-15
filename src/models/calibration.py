from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression

@dataclass
class CalibrationResult:
    calibrator: object | None
    method: str
    brier_before: float | None
    brier_after: float | None


def fit_calibrator(valid_df: pd.DataFrame, prob_col: str = 'model_prob', label_col: str = 'label_ready_long') -> CalibrationResult:
    df = valid_df.dropna(subset=[prob_col, label_col]).copy()
    if len(df) < 20 or df[label_col].nunique() < 2:
        return CalibrationResult(calibrator=None, method='identity', brier_before=None, brier_after=None)
    x = df[prob_col].clip(0.001,0.999).values
    y = df[label_col].astype(int).values
    iso = IsotonicRegression(out_of_bounds='clip')
    iso.fit(x, y)
    before = float(np.mean((x-y)**2))
    after_probs = iso.predict(x)
    after = float(np.mean((after_probs-y)**2))
    return CalibrationResult(calibrator=iso, method='isotonic', brier_before=before, brier_after=after)


def apply_calibration(prob: pd.Series, result: CalibrationResult) -> pd.Series:
    if result.calibrator is None:
        return prob.clip(0,1)
    return pd.Series(result.calibrator.predict(prob.values), index=prob.index).clip(0,1)
