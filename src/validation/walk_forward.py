from __future__ import annotations
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from src.models.ranker import fit_ranker, score_ranker
from src.models.calibration import fit_calibrator, apply_calibration


def generate_walk_forward_splits(dates: list[pd.Timestamp], train: int = 100, valid: int = 30, test: int = 20):
    i=0
    while i + train + valid + test <= len(dates):
        yield dates[i:i+train], dates[i+train:i+train+valid], dates[i+train+valid:i+train+valid+test]
        i += test


def evaluate_walk_forward(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(pd.to_datetime(df['date'].dropna().unique()))
    metrics=[]
    preds=[]
    for fold, (tr, va, te) in enumerate(generate_walk_forward_splits(dates), start=1):
        train_df = df[df['date'].isin(tr)].copy()
        valid_df = df[df['date'].isin(va)].copy()
        test_df = df[df['date'].isin(te)].copy()
        if train_df['label_ready_long'].dropna().nunique() < 2 or test_df.empty:
            continue
        ranker = fit_ranker(train_df)
        valid_df['model_prob'] = score_ranker(ranker.model, valid_df, ranker.feature_cols)
        cal = fit_calibrator(valid_df)
        test_df['model_prob_raw'] = score_ranker(ranker.model, test_df, ranker.feature_cols)
        test_df['model_prob'] = apply_calibration(test_df['model_prob_raw'], cal)
        y = test_df['label_ready_long'].dropna()
        yhat = test_df.loc[y.index, 'model_prob']
        auc = roc_auc_score(y.astype(int), yhat) if y.nunique() > 1 else np.nan
        top = test_df.sort_values('model_prob', ascending=False).head(max(3, len(test_df)//5))
        hit = top['label_ready_long'].mean() if not top['label_ready_long'].dropna().empty else np.nan
        metrics.append({'fold':fold,'train_start':min(tr),'train_end':max(tr),'test_start':min(te),'test_end':max(te),'test_auc':auc,'top_bucket_hit_rate':hit,'rows_test':len(test_df),'calibration_method':cal.method})
        preds.append(test_df[['date','ticker','model_prob','label_ready_long']].assign(fold=fold))
    metrics_df = pd.DataFrame(metrics)
    preds_df = pd.concat(preds, ignore_index=True) if preds else pd.DataFrame(columns=['date','ticker','model_prob','label_ready_long','fold'])
    return metrics_df, preds_df
