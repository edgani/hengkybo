from __future__ import annotations
import numpy as np
import pandas as pd

EPS = 1e-9


def build_broker_daily(broker: pd.DataFrame) -> pd.DataFrame:
    df = broker.copy().sort_values(['broker_code', 'ticker', 'date'])
    df['gross_activity_lot'] = df['buy_lot'] + df['sell_lot']
    df['net_lot'] = df['buy_lot'] - df['sell_lot']
    return df


def build_broker_profiles(broker_daily: pd.DataFrame) -> pd.DataFrame:
    df = broker_daily.groupby(['date','broker_code'], as_index=False).agg(
        buy_lot=('buy_lot','sum'), sell_lot=('sell_lot','sum'), gross_activity_lot=('gross_activity_lot','sum'),
        net_lot=('net_lot','sum'), ticker_breadth=('ticker','nunique')
    ).sort_values(['broker_code','date'])
    df['directionality_ratio'] = (df['net_lot'].abs() / (df['gross_activity_lot'] + EPS))
    grp = df.groupby('broker_code', group_keys=False)
    df['activity_20d'] = grp['gross_activity_lot'].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df['breadth_20d'] = grp['ticker_breadth'].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df['dir_20d'] = grp['directionality_ratio'].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df['net_stability_20d'] = 1 - grp['net_lot'].transform(lambda s: s.rolling(20, min_periods=5).std()) / (grp['gross_activity_lot'].transform(lambda s: s.rolling(20, min_periods=5).mean()) + EPS)
    def cross_section_scale(s: pd.Series) -> pd.Series:
        return ((s - s.min()) / (s.max() - s.min() + EPS) * 100).fillna(50)
    df['activity_rank_score'] = df.groupby('date')['activity_20d'].transform(cross_section_scale)
    df['breadth_rank_score'] = df.groupby('date')['breadth_20d'].transform(cross_section_scale)
    df['directionality_rank_score'] = df.groupby('date')['dir_20d'].transform(cross_section_scale)
    df['stability_rank_score'] = df.groupby('date')['net_stability_20d'].transform(cross_section_scale)
    df['institutional_like_score'] = (
        0.35 * df['activity_rank_score'] + 0.20 * df['breadth_rank_score'] + 0.25 * df['directionality_rank_score'] + 0.20 * df['stability_rank_score']
    ).clip(0,100)
    df['retail_like_score'] = (
        0.35 * (100-df['activity_rank_score']) + 0.20 * (100-df['breadth_rank_score']) + 0.25 * (100-df['stability_rank_score']) + 0.20 * (100-df['directionality_rank_score'])
    ).clip(0,100)
    df['hybrid_score'] = (100 - (df['institutional_like_score'] - df['retail_like_score']).abs()).clip(0,100)
    df['adaptive_broker_label'] = np.where(
        (df['institutional_like_score'] >= df['retail_like_score']) & (df['institutional_like_score'] >= df['hybrid_score']), 'INSTITUTIONAL_LIKE',
        np.where((df['retail_like_score'] >= df['institutional_like_score']) & (df['retail_like_score'] >= df['hybrid_score']), 'RETAIL_LIKE', 'HYBRID')
    )
    df['broker_profile_confidence'] = (0.6 * df[['institutional_like_score','retail_like_score','hybrid_score']].max(axis=1) + 0.4 * df['stability_rank_score']).clip(0,100)
    return df


def build_inventory_and_levels(broker_daily: pd.DataFrame, broker_profiles: pd.DataFrame, decay: float = 0.985) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = broker_daily.merge(broker_profiles[['date','broker_code','institutional_like_score','retail_like_score','broker_profile_confidence']], on=['date','broker_code'], how='left')
    df = df.sort_values(['ticker','broker_code','date']).copy()
    out=[]
    for (ticker, broker_code), g in df.groupby(['ticker','broker_code'], sort=False):
        buy_state=sell_state=buy_val=sell_val=0.0
        for row in g.itertuples(index=False):
            buy_state = decay * buy_state + float(row.buy_lot)
            sell_state = decay * sell_state + float(row.sell_lot)
            buy_val = decay * buy_val + float(row.buy_value)
            sell_val = decay * sell_val + float(row.sell_value)
            net_inv = buy_state - sell_state
            out.append({
                'date':row.date,'ticker':ticker,'broker_code':broker_code,
                'net_inventory':net_inv,'inventory_strength':abs(net_inv),
                'avg_buy_est':buy_val / max(buy_state, EPS), 'avg_sell_est':sell_val / max(sell_state, EPS),
                'institutional_like_score':row.institutional_like_score, 'retail_like_score':row.retail_like_score,
                'broker_profile_confidence':row.broker_profile_confidence,
            })
    inv = pd.DataFrame(out)
    pos = inv[inv['net_inventory']>0].copy()
    neg = inv.copy(); neg['distribution_strength'] = neg['net_inventory'].clip(upper=0).abs()
    def _levels(g: pd.DataFrame) -> pd.Series:
        acc = g.nlargest(5, 'inventory_strength')
        support = np.average(acc['avg_buy_est'], weights=acc['inventory_strength']) if len(acc) else np.nan
        acc_strength = acc['inventory_strength'].sum()
        inst_part = np.average(acc['institutional_like_score'], weights=acc['inventory_strength']) if len(acc) else 50
        retail_part = np.average(acc['retail_like_score'], weights=acc['inventory_strength']) if len(acc) else 50
        return pd.Series({'institutional_support':support,'acc_inventory_strength':acc_strength,'institutional_participation_score':inst_part,'retail_participation_score':retail_part})
    levels = pos.groupby(['date','ticker']).apply(_levels).reset_index()
    def _res(g: pd.DataFrame) -> pd.Series:
        dist = g[g['distribution_strength']>0].nlargest(5, 'distribution_strength')
        resistance = np.average(dist['avg_sell_est'], weights=dist['distribution_strength']) if len(dist) else np.nan
        overhang = dist['distribution_strength'].sum()
        return pd.Series({'institutional_resistance':resistance,'distribution_overhang':overhang})
    res = neg.groupby(['date','ticker']).apply(_res).reset_index()
    levels = levels.merge(res, on=['date','ticker'], how='outer')
    levels['broker_alignment_score'] = (levels['institutional_participation_score'] - levels['retail_participation_score'] + 50).clip(0,100)
    return inv, broker_profiles, levels
