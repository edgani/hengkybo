from __future__ import annotations
from dataclasses import dataclass
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import roc_auc_score

FEATURE_COLS = [
    'accumulation_quality_score','breakout_integrity_score','distribution_risk_score','microstructure_strength_score',
    'macro_alignment_score','dry_score','wet_score','phase_confidence','broker_alignment_score','foreign_alignment_score',
    'transfer_suspicion','spoof_risk_score','book_support_score','tape_conviction_score'
]

@dataclass
class RankerResult:
    model: Pipeline
    feature_cols: list[str]
    train_auc: float | None


def fit_ranker(train_df: pd.DataFrame) -> RankerResult:
    df = train_df.dropna(subset=['label_ready_long']).copy()
    usable = [c for c in FEATURE_COLS if c in df.columns]
    X = df[usable]
    y = df['label_ready_long'].astype(int)
    model = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        ('clf', GradientBoostingClassifier(random_state=42, n_estimators=120, learning_rate=0.05, max_depth=2))
    ])
    model.fit(X, y)
    prob = model.predict_proba(X)[:,1]
    auc = roc_auc_score(y, prob) if y.nunique() > 1 else None
    return RankerResult(model=model, feature_cols=usable, train_auc=auc)


def score_ranker(model: Pipeline, df: pd.DataFrame, feature_cols: list[str]) -> pd.Series:
    X = df[feature_cols].copy()
    return pd.Series(model.predict_proba(X)[:,1], index=df.index)


def save_ranker(result: RankerResult, path: str) -> None:
    joblib.dump({'model': result.model, 'feature_cols': result.feature_cols, 'train_auc': result.train_auc}, path)
