from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd

from src.utils.config import load_yaml
from src.utils.io import ensure_dir
from src.ingestion import load_csv
from src.features.eod import build_eod_features, build_phase_features
from src.features.broker import build_broker_daily, build_broker_profiles, build_inventory_and_levels
from src.features.intraday import build_intraday_features
from src.features.regime import build_regime
from src.features.labels import label_forward_outcomes
from src.scoring.engine import build_scores, map_verdict
from src.scoring.explain import build_explanations
from src.models.ranker import fit_ranker, score_ranker, save_ranker
from src.models.calibration import fit_calibrator, apply_calibration
from src.validation.walk_forward import evaluate_walk_forward


def main() -> None:
    cfg = load_yaml('config/base.yaml')
    raw_dir = Path(cfg['paths']['raw'])
    feat_dir = ensure_dir(cfg['paths']['features'])
    model_dir = ensure_dir(cfg['paths']['models'])

    prices = load_csv(raw_dir/'prices_daily.csv', ['date'])
    broker = load_csv(raw_dir/'broker_summary_daily.csv', ['date'])
    foreign = load_csv(raw_dir/'foreign_daily.csv', ['date'])
    done = load_csv(raw_dir/'done_detail_intraday.csv', ['timestamp','trade_date'])
    book = load_csv(raw_dir/'orderbook_intraday.csv', ['timestamp','trade_date'])

    eod = build_eod_features(prices)
    phase = build_phase_features(eod)
    broker_daily = build_broker_daily(broker)
    broker_profiles = build_broker_profiles(broker_daily)
    inventory, broker_profiles, levels = build_inventory_and_levels(broker_daily, broker_profiles)

    foreign = foreign.copy().sort_values(['ticker','date'])
    foreign['foreign_net_lot'] = foreign['foreign_buy_lot'] - foreign['foreign_sell_lot']
    foreign['foreign_net_20d'] = foreign.groupby('ticker')['foreign_net_lot'].transform(lambda s: s.rolling(20, min_periods=5).sum())
    foreign['foreign_alignment_score'] = np.where(foreign['foreign_net_20d'] > 0, 70, np.where(foreign['foreign_net_20d'] < 0, 30, 50))

    regime = build_regime(prices)
    intraday = build_intraday_features(done, book)
    labels = label_forward_outcomes(prices, horizon_days=cfg['model']['horizon_days'], target_return=cfg['model']['target_return'], max_adverse=cfg['model']['max_adverse'])

    prof_mean = broker_profiles.groupby('date', as_index=False)['broker_profile_confidence'].mean().rename(columns={'broker_profile_confidence':'broker_profile_confidence_mean'})

    feat = eod.merge(phase, on=['date','ticker'], how='left')               .merge(levels, on=['date','ticker'], how='left')               .merge(foreign[['date','ticker','foreign_net_20d','foreign_alignment_score']], on=['date','ticker'], how='left')               .merge(regime[['date','regime','macro_alignment_score','macro_headwind_score']], on='date', how='left')               .merge(prof_mean, on='date', how='left')               .merge(labels, on=['date','ticker'], how='left')

    feat['support_distance_pct'] = (feat['close'] / feat['institutional_support'] - 1) * 100
    feat['resistance_headroom_pct'] = (feat['institutional_resistance'] / feat['close'] - 1) * 100
    feat['resistance_headroom_score'] = feat.groupby('date')['resistance_headroom_pct'].transform(lambda s: ((s - s.min()) / (s.max() - s.min() + 1e-9) * 100).fillna(50))
    feat['dry_score'] = (0.35 * (100 - feat.groupby('date')['distribution_overhang'].transform(lambda s: ((s-s.min())/(s.max()-s.min()+1e-9)*100).fillna(50))) + 0.35 * feat['compression_score'].fillna(50) + 0.30 * feat['phase_confidence'].fillna(50)).clip(0,100)
    feat['wet_score'] = (100 - feat['dry_score']).clip(0,100)
    feat['false_breakout_risk'] = ((feat['breakout_tension'].fillna(50) * 0.35) + ((100 - feat['pullback_quality'].fillna(0).add(1).mul(50)) * 0.25) + (feat['resistance_headroom_score'].fillna(50) * 0.20) + ((100 - feat['phase_confidence'].fillna(50)) * 0.20)).clip(0,100)
    feat['phase_deterioration_score'] = np.where(feat['phase'].isin(['MARKDOWN','DISTRIBUTION']), 75, np.where(feat['phase'].eq('LATE_MARKUP'), 55, 35))

    latest_intra = intraday.sort_values('date').groupby('ticker', as_index=False).tail(1)
    feat = feat.merge(latest_intra[['ticker','date','tape_conviction_score','transfer_suspicion','spoof_risk_score','book_support_score','microstructure_strength_score','microstructure_weakness_score']], on=['ticker','date'], how='left')
    feat[['tape_conviction_score','transfer_suspicion','spoof_risk_score','book_support_score','microstructure_strength_score','microstructure_weakness_score']] = feat[['tape_conviction_score','transfer_suspicion','spoof_risk_score','book_support_score','microstructure_strength_score','microstructure_weakness_score']].fillna(50)

    feat = build_scores(feat)

    metrics_df, wf_preds = evaluate_walk_forward(feat)
    metrics_df.to_csv(feat_dir/'walk_forward_metrics.csv', index=False)
    wf_preds.to_csv(feat_dir/'walk_forward_predictions.csv', index=False)

    dates = sorted(feat['date'].unique())
    train_cut = dates[-30]
    valid_cut = dates[-15]
    train_df = feat[feat['date'] < train_cut].copy()
    valid_df = feat[(feat['date'] >= train_cut) & (feat['date'] < valid_cut)].copy()
    score_df = feat.copy()

    ranker = fit_ranker(train_df)
    score_df['model_prob_raw'] = score_ranker(ranker.model, score_df, ranker.feature_cols)
    valid_df = valid_df.copy(); valid_df['model_prob'] = score_ranker(ranker.model, valid_df, ranker.feature_cols)
    cal = fit_calibrator(valid_df)
    score_df['calibrated_prob'] = apply_calibration(score_df['model_prob_raw'], cal)
    score_df = map_verdict(score_df)
    score_df = build_explanations(score_df)

    latest_date = score_df['date'].max()
    latest = score_df[score_df['date'] == latest_date].copy().sort_values(['verdict','calibrated_prob','rule_long_score'], ascending=[True, False, False])

    feature_cols_out = ['date','ticker','sector','phase','phase_confidence','regime','accumulation_quality_score','breakout_integrity_score','distribution_risk_score','microstructure_strength_score','dry_score','wet_score','macro_alignment_score','foreign_alignment_score','broker_alignment_score','institutional_support','institutional_resistance','model_prob_raw','calibrated_prob','blended_long_prob','rule_long_score','verdict','confidence','why_now','invalidation']
    score_df.to_csv(feat_dir/'feature_store_v4.csv', index=False)
    latest[feature_cols_out].to_csv(feat_dir/'latest_watchlist_v4.csv', index=False)
    broker_profiles.sort_values(['date','broker_profile_confidence'], ascending=[True,False]).groupby('broker_code', as_index=False).tail(1).to_csv(feat_dir/'broker_profiles_latest.csv', index=False)
    intraday.to_csv(feat_dir/'intraday_features_v4.csv', index=False)
    pd.DataFrame([{'method':cal.method,'brier_before':cal.brier_before,'brier_after':cal.brier_after,'train_auc':ranker.train_auc}]).to_csv(feat_dir/'calibration_report.csv', index=False)
    save_ranker(ranker, str(model_dir/'ranker.joblib'))
    with open(model_dir/'model_summary.json', 'w', encoding='utf-8') as f:
        json.dump({'train_auc': ranker.train_auc, 'calibration_method': cal.method, 'latest_date': str(latest_date.date())}, f, indent=2)
    print(f'Finished V4 pipeline. Latest date: {latest_date.date()} | watchlist rows: {len(latest)}')


if __name__ == '__main__':
    main()
