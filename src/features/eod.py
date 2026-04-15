from __future__ import annotations
import numpy as np
import pandas as pd


def _trend_efficiency(x: pd.Series) -> float:
    x = x.dropna()
    if len(x) < 2:
        return np.nan
    return abs(x.iloc[-1] - x.iloc[0]) / (x.diff().abs().sum() + 1e-9)


def build_eod_features(prices: pd.DataFrame) -> pd.DataFrame:
    df = prices.copy().sort_values(['ticker', 'date'])
    grp = df.groupby('ticker', group_keys=False)
    df['ret_1'] = grp['close'].pct_change()
    df['ret_5'] = grp['close'].pct_change(5)
    df['ret_20'] = grp['close'].pct_change(20)
    df['ma_10'] = grp['close'].transform(lambda s: s.rolling(10, min_periods=3).mean())
    df['ma_20'] = grp['close'].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df['ma_50'] = grp['close'].transform(lambda s: s.rolling(50, min_periods=10).mean())
    df['dist_ma_20_pct'] = (df['close'] / df['ma_20'] - 1) * 100
    df['dist_ma_50_pct'] = (df['close'] / df['ma_50'] - 1) * 100
    df['range_pct'] = (df['high'] - df['low']) / df['close'] * 100
    df['true_range_pct'] = np.maximum(df['high'] - df['low'], np.maximum((df['high'] - grp['close'].shift()).abs(), (df['low'] - grp['close'].shift()).abs())) / df['close'] * 100
    df['atr_14_pct'] = grp['true_range_pct'].transform(lambda s: s.rolling(14, min_periods=5).mean())
    df['vol_20_mean'] = grp['volume_shares'].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df['vol_20_std'] = grp['volume_shares'].transform(lambda s: s.rolling(20, min_periods=5).std())
    df['volume_z20'] = (df['volume_shares'] - df['vol_20_mean']) / (df['vol_20_std'] + 1e-9)
    df['high_20'] = grp['high'].transform(lambda s: s.rolling(20, min_periods=5).max())
    df['low_20'] = grp['low'].transform(lambda s: s.rolling(20, min_periods=5).min())
    df['close_loc_20'] = (df['close'] - df['low_20']) / (df['high_20'] - df['low_20'] + 1e-9)
    df['compression_score'] = (100 - (df['atr_14_pct'] / (grp['atr_14_pct'].transform(lambda s: s.rolling(20, min_periods=5).max()) + 1e-9) * 100)).clip(0, 100)
    df['trend_eff_20'] = grp['close'].transform(lambda s: s.rolling(20, min_periods=6).apply(_trend_efficiency, raw=False))
    df['breakout_tension'] = (df['close_loc_20'] * 100).clip(0, 100)
    up_move = (df['close'] > df['open']).astype(int)
    down_move = (df['close'] < df['open']).astype(int)
    df['up_vol_10'] = grp.apply(lambda g: (g['volume_shares'] * (g['close'] > g['open']).astype(int)).rolling(10, min_periods=3).sum()).reset_index(level=0, drop=True)
    df['down_vol_10'] = grp.apply(lambda g: (g['volume_shares'] * (g['close'] < g['open']).astype(int)).rolling(10, min_periods=3).sum()).reset_index(level=0, drop=True)
    df['pullback_quality'] = (1 - (df['down_vol_10'] / (df['up_vol_10'] + 1e-9))).clip(-1, 1)
    return df


def build_phase_features(eod: pd.DataFrame) -> pd.DataFrame:
    df = eod.copy()
    conds = [
        (df['ret_20'] > 0.10) & (df['breakout_tension'] > 70) & (df['compression_score'] > 40),
        (df['ret_20'] > 0.03) & (df['dist_ma_20_pct'] > 0),
        (df['ret_20'] < -0.08) & (df['dist_ma_20_pct'] < -2),
        (df['ret_20'] < -0.02),
    ]
    vals = ['LATE_MARKUP', 'EARLY_MARKUP', 'MARKDOWN', 'DISTRIBUTION']
    df['phase'] = np.select(conds, vals, default='ACCUMULATION')
    df.loc[(df['compression_score'] > 60) & (df['breakout_tension'] < 55), 'phase'] = 'BASE_BUILDING'
    df.loc[(df['ret_5'] < 0) & (df['ret_20'] > 0) & (df['pullback_quality'] > 0), 'phase'] = 'PULLBACK_HEALTHY'
    df['phase_confidence'] = (
        0.35 * df['breakout_tension'].fillna(50)
        + 0.25 * (df['trend_eff_20'].fillna(0.5) * 100)
        + 0.20 * df['compression_score'].fillna(50)
        + 0.20 * ((df['pullback_quality'].fillna(0) + 1) * 50)
    ).clip(0, 100)
    return df[['date','ticker','phase','phase_confidence']]
