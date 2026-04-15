from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from src.ingestion import load_csv
from src.features.eod import build_eod_features, build_phase_features
from src.features.regime import build_regime


def scale_by_date(df: pd.DataFrame, col: str) -> pd.Series:
    def scale(s: pd.Series) -> pd.Series:
        if s.nunique(dropna=True) <= 1:
            return pd.Series(50.0, index=s.index)
        return ((s - s.min()) / (s.max() - s.min() + 1e-9) * 100).fillna(50)
    return df.groupby('date')[col].transform(scale)


def main() -> None:
    ap = argparse.ArgumentParser(description='Run an EOD-only smoke test on real IDX daily data.')
    ap.add_argument('--prices', default='data/raw/prices_daily_real.csv', help='Input daily prices CSV')
    ap.add_argument('--out', default='data/features/latest_watchlist_real_eod.csv', help='Output watchlist CSV')
    args = ap.parse_args()

    prices_path = Path(args.prices)
    if not prices_path.exists():
        raise SystemExit(f'Input not found: {prices_path}. Jalankan fetch_real_eod_yfinance dulu.')

    prices = load_csv(prices_path, ['date'])
    eod = build_eod_features(prices)
    phase = build_phase_features(eod)
    regime = build_regime(prices)
    df = eod.merge(phase, on=['date','ticker'], how='left').merge(regime[['date','regime','macro_alignment_score']], on='date', how='left')

    df['ret_20_scaled'] = scale_by_date(df, 'ret_20')
    df['volume_z20_scaled'] = scale_by_date(df, 'volume_z20')
    df['dist_ma_20_scaled'] = scale_by_date(df, 'dist_ma_20_pct')
    df['risk_from_close_loc'] = 100 - (df['close_loc_20'] * 100).clip(0, 100)

    df['accumulation_quality_score'] = (
        0.28 * df['ret_20_scaled'].fillna(50)
        + 0.24 * df['dist_ma_20_scaled'].fillna(50)
        + 0.18 * df['phase_confidence'].fillna(50)
        + 0.18 * ((df['pullback_quality'].fillna(0) + 1) * 50)
        + 0.12 * df['macro_alignment_score'].fillna(50)
    ).clip(0, 100)

    df['breakout_integrity_score'] = (
        0.30 * df['breakout_tension'].fillna(50)
        + 0.22 * df['compression_score'].fillna(50)
        + 0.20 * df['volume_z20_scaled'].fillna(50)
        + 0.16 * ((df['pullback_quality'].fillna(0) + 1) * 50)
        + 0.12 * df['phase_confidence'].fillna(50)
    ).clip(0, 100)

    df['distribution_risk_score'] = (
        0.30 * scale_by_date(df, 'atr_14_pct').fillna(50)
        + 0.26 * df['risk_from_close_loc'].fillna(50)
        + 0.24 * (100 - df['ret_20_scaled'].fillna(50))
        + 0.20 * np.where(df['phase'].isin(['MARKDOWN','DISTRIBUTION']), 80, 35)
    ).clip(0, 100)

    df['long_score'] = (
        0.40 * df['accumulation_quality_score']
        + 0.35 * df['breakout_integrity_score']
        + 0.10 * df['macro_alignment_score'].fillna(50)
        - 0.25 * df['distribution_risk_score']
    ).clip(0, 100)

    df['verdict'] = np.select(
        [
            (df['long_score'] >= 55) & (df['distribution_risk_score'] <= 45) & (~df['phase'].isin(['MARKDOWN','DISTRIBUTION'])),
            (df['long_score'] >= 48) & (df['distribution_risk_score'] <= 58),
            (df['distribution_risk_score'] >= 72) | (df['phase'].eq('MARKDOWN')),
        ],
        ['READY_LONG', 'WATCH', 'AVOID'],
        default='NEUTRAL',
    )

    latest = df['date'].max()
    latest_df = df[df['date'].eq(latest)].copy()
    latest_df['why_now'] = np.where(
        latest_df['verdict'].eq('READY_LONG'),
        'struktur harga-volume sehat; trend dan breakout tension mendukung',
        np.where(
            latest_df['verdict'].eq('WATCH'),
            'belum matang penuh, tapi struktur masih layak dipantau',
            np.where(
                latest_df['verdict'].eq('AVOID'),
                'risiko distribusi/markdown lebih dominan',
                'sinyal campuran, edge EOD saja belum kuat',
            ),
        ),
    )
    latest_df['invalidation'] = latest_df['ma_20'].round(2).astype(str).radd('close < MA20 ') 

    out_cols = [
        'date','ticker','phase','phase_confidence','regime',
        'accumulation_quality_score','breakout_integrity_score','distribution_risk_score',
        'long_score','verdict','close','ret_20','atr_14_pct','volume_z20','why_now','invalidation'
    ]
    out = latest_df[out_cols].sort_values(['verdict','long_score','ticker'], ascending=[True, False, True])
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f'Saved {len(out):,} latest rows -> {out_path}')
    print(out.head(20).to_string(index=False))


if __name__ == '__main__':
    main()
