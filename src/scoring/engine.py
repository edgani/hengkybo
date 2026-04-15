from __future__ import annotations
import numpy as np
import pandas as pd


def _scale_by_date(df: pd.DataFrame, col: str) -> pd.Series:
    def scale(s: pd.Series) -> pd.Series:
        return ((s - s.min()) / (s.max() - s.min() + 1e-9) * 100).fillna(50)
    return df.groupby('date')[col].transform(scale)


def build_scores(feature_df: pd.DataFrame) -> pd.DataFrame:
    df = feature_df.copy()
    for col in ['acc_inventory_strength','distribution_overhang','foreign_net_20d','volume_z20']:
        if col in df.columns:
            df[f'{col}_scaled'] = _scale_by_date(df, col)
    df['accumulation_quality_score'] = (
        0.28 * df['acc_inventory_strength_scaled'].fillna(50) +
        0.18 * df['dry_score'].fillna(50) +
        0.16 * df['phase_confidence'].fillna(50) +
        0.16 * df['broker_alignment_score'].fillna(50) +
        0.12 * df['foreign_alignment_score'].fillna(50) +
        0.10 * (100 - df['distribution_overhang_scaled'].fillna(50))
    ).clip(0,100)
    df['breakout_integrity_score'] = (
        0.24 * df['breakout_tension'].fillna(50) +
        0.18 * df['compression_score'].fillna(50) +
        0.16 * ((df['pullback_quality'].fillna(0)+1)*50) +
        0.16 * df['phase_confidence'].fillna(50) +
        0.14 * df['volume_z20_scaled'].fillna(50) +
        0.12 * (100 - df['resistance_headroom_score'].fillna(50))
    ).clip(0,100)
    df['distribution_risk_score'] = (
        0.28 * df['wet_score'].fillna(50) +
        0.22 * df['distribution_overhang_scaled'].fillna(50) +
        0.16 * df['false_breakout_risk'].fillna(50) +
        0.14 * (100 - df['foreign_alignment_score'].fillna(50)) +
        0.12 * df['phase_deterioration_score'].fillna(50) +
        0.08 * (100 - df['broker_alignment_score'].fillna(50))
    ).clip(0,100)
    if 'microstructure_strength_score' not in df.columns:
        df['microstructure_strength_score'] = 50.0
    if 'transfer_suspicion' not in df.columns:
        df['transfer_suspicion'] = 50.0
    if 'spoof_risk_score' not in df.columns:
        df['spoof_risk_score'] = 50.0
    return df


def map_verdict(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['rule_long_score'] = (
        0.32 * out['accumulation_quality_score'] + 0.24 * out['breakout_integrity_score'] + 0.16 * out['dry_score'].fillna(50) + 0.16 * out['microstructure_strength_score'].fillna(50) + 0.12 * out['macro_alignment_score'].fillna(50) - 0.22 * out['distribution_risk_score']
    ).clip(0,100)
    out['confidence'] = (
        0.32 * out['phase_confidence'].fillna(50) + 0.16 * out['broker_profile_confidence_mean'].fillna(50) + 0.16 * (100 - out['transfer_suspicion'].fillna(50)) + 0.16 * (100 - out['spoof_risk_score'].fillna(50)) + 0.20 * (out.get('calibrated_prob', pd.Series(np.nan, index=out.index)).fillna(0.5).sub(0.5).abs() * 200)
    ).clip(0,100)
    prob = out['calibrated_prob'].fillna(out.get('model_prob', pd.Series(np.nan, index=out.index))).fillna(out['rule_long_score']/100)
    out['blended_long_prob'] = (0.30 * prob + 0.70 * (out['rule_long_score'] / 100)).clip(0,1)
    out['verdict'] = np.select(
        [
            (out['blended_long_prob'] >= 0.58) & (out['distribution_risk_score'] <= 45) & (out['microstructure_strength_score'] >= 55),
            ((out['blended_long_prob'] >= 0.48) & (out['distribution_risk_score'] <= 60)) | ((out['accumulation_quality_score'] >= 65) & (out['distribution_risk_score'] <= 40)),
            (out['distribution_risk_score'] >= 82) | ((prob <= 0.20) & (out['phase'].isin(['MARKDOWN','DISTRIBUTION']))),
            (out['distribution_risk_score'] >= 70) | ((out['blended_long_prob'] <= 0.28) & (out['phase'].isin(['MARKDOWN','DISTRIBUTION']))),
        ],
        ['READY_LONG','WATCH','TRIM_SELL','AVOID'],
        default='NEUTRAL'
    )
    return out
