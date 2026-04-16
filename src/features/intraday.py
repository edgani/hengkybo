from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.burst import detect_burst_events

EPS = 1e-9


def build_done_detail_features(done: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = done.copy()
    df['ts_sec'] = df['timestamp'].dt.floor('s')
    side = df['side_aggressor'].astype(str).str.upper()
    df['buy_aggr_lot'] = np.where(side.eq('BUY'), df['lot'], 0)
    df['sell_aggr_lot'] = np.where(side.eq('SELL'), df['lot'], 0)
    sec = df.groupby(['trade_date', 'ticker', 'ts_sec'], as_index=False).agg(
        trades_in_sec=('lot', 'size'),
        lot_in_sec=('lot', 'sum'),
        sec_high=('price', 'max'),
        sec_low=('price', 'min'),
        buy_aggr_lot_sec=('buy_aggr_lot', 'sum'),
        sell_aggr_lot_sec=('sell_aggr_lot', 'sum')
    )
    sec['burst_flag'] = (sec['trades_in_sec'] >= 3).astype(int)
    sec['buy_sweep_flag'] = ((sec['buy_aggr_lot_sec'] > sec['sell_aggr_lot_sec']) & ((sec['sec_high'] - sec['sec_low']) > 0)).astype(int)
    sec['sell_sweep_flag'] = ((sec['sell_aggr_lot_sec'] > sec['buy_aggr_lot_sec']) & ((sec['sec_high'] - sec['sec_low']) > 0)).astype(int)

    pair = df.groupby(['trade_date', 'ticker', 'buyer_broker', 'seller_broker'], as_index=False)['lot'].sum().rename(columns={'lot': 'pair_lot'})
    pair_sum = pair.groupby(['trade_date', 'ticker'], as_index=False).agg(
        pair_total_lot=('pair_lot', 'sum'), top_pair_lot=('pair_lot', 'max'), active_pairs=('pair_lot', 'size')
    )
    pair_sum['top_pair_share'] = pair_sum['top_pair_lot'] / (pair_sum['pair_total_lot'] + EPS)

    daily = df.groupby(['trade_date', 'ticker'], as_index=False).agg(
        trade_count=('lot', 'size'), total_lot=('lot', 'sum'),
        buy_aggr_lot=('buy_aggr_lot', 'sum'), sell_aggr_lot=('sell_aggr_lot', 'sum'),
        first_trade_price=('price', 'first'), last_trade_price=('price', 'last'),
        avg_lot=('lot', 'mean'), max_lot=('lot', 'max')
    )
    burst_sum = sec.groupby(['trade_date', 'ticker'], as_index=False).agg(
        burst_count=('burst_flag', 'sum'), buy_sweep_count=('buy_sweep_flag', 'sum'),
        sell_sweep_count=('sell_sweep_flag', 'sum'), max_trades_same_sec=('trades_in_sec', 'max')
    )
    daily = daily.merge(burst_sum, on=['trade_date', 'ticker'], how='left').merge(pair_sum, on=['trade_date', 'ticker'], how='left')
    daily['buy_aggr_ratio'] = daily['buy_aggr_lot'] / (daily['total_lot'] + EPS)
    daily['sell_aggr_ratio'] = daily['sell_aggr_lot'] / (daily['total_lot'] + EPS)
    daily['intraday_price_move_pct'] = (daily['last_trade_price'] / daily['first_trade_price'] - 1) * 100
    daily['child_order_cluster_score'] = (daily['max_trades_same_sec'] / (daily['trade_count'] + EPS) * 100).clip(0, 100)
    daily['tape_conviction_score'] = (
        daily['buy_aggr_ratio'] * 55 +
        (daily['buy_sweep_count'] / (daily['buy_sweep_count'] + daily['sell_sweep_count'] + 1)) * 25 +
        (1 - daily['top_pair_share'].fillna(0.5)) * 20
    ).clip(0, 100)
    daily['tape_weakness_score'] = (
        daily['sell_aggr_ratio'] * 55 +
        (daily['sell_sweep_count'] / (daily['buy_sweep_count'] + daily['sell_sweep_count'] + 1)) * 25 +
        (1 - daily['top_pair_share'].fillna(0.5)) * 20
    ).clip(0, 100)
    daily['transfer_suspicion'] = (daily['top_pair_share'].fillna(0.5) * 70 + (daily['child_order_cluster_score'] / 100) * 30).clip(0, 100)

    burst_daily, burst_events = detect_burst_events(done)
    daily = daily.rename(columns={'trade_date': 'date'}).merge(burst_daily, on=['date', 'ticker'], how='left')
    return daily, burst_events


def build_orderbook_features(book: pd.DataFrame) -> pd.DataFrame:
    df = book.copy()
    df['top3_bid'] = df[['bid_1_lot', 'bid_2_lot', 'bid_3_lot']].sum(axis=1)
    df['top3_offer'] = df[['offer_1_lot', 'offer_2_lot', 'offer_3_lot']].sum(axis=1)
    df['weighted_bid'] = sum(df[f'bid_{i}_lot'] * (6 - i) for i in range(1, 6))
    df['weighted_offer'] = sum(df[f'offer_{i}_lot'] * (6 - i) for i in range(1, 6))
    df['imbalance'] = (df['weighted_bid'] - df['weighted_offer']) / (df['weighted_bid'] + df['weighted_offer'] + EPS)
    df['spread_bps'] = ((df['offer_1_price'] - df['bid_1_price']) / ((df['offer_1_price'] + df['bid_1_price']) / 2 + EPS) * 10000)
    daily = df.groupby(['trade_date', 'ticker'], as_index=False).agg(
        top3_bid=('top3_bid', 'mean'), top3_offer=('top3_offer', 'mean'),
        weighted_bid=('weighted_bid', 'mean'), weighted_offer=('weighted_offer', 'mean'),
        imbalance=('imbalance', 'mean'), spread_bps=('spread_bps', 'mean')
    )
    daily['book_support_score'] = (((daily['imbalance'] + 1) / 2) * 100).clip(0, 100)
    daily['book_pressure_down_score'] = (((-daily['imbalance'] + 1) / 2) * 100).clip(0, 100)
    daily['spoof_risk_score'] = ((daily['top3_offer'] / (daily['top3_bid'] + daily['top3_offer'] + EPS)) * 60 + (daily['spread_bps'] / (daily['spread_bps'].max() + EPS)) * 40).clip(0, 100)
    daily['offer_consumption_score'] = ((daily['top3_bid'] / (daily['top3_offer'] + EPS)) * 50 + (daily['imbalance'].clip(lower=0) * 50)).clip(0, 100)
    daily['bid_consumption_score'] = ((daily['top3_offer'] / (daily['top3_bid'] + EPS)) * 50 + ((-daily['imbalance']).clip(lower=0) * 50)).clip(0, 100)
    return daily.rename(columns={'trade_date': 'date'})


def build_intraday_features(done: pd.DataFrame, book: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d, events = build_done_detail_features(done)
    b = build_orderbook_features(book)
    out = d.merge(b, on=['date', 'ticker'], how='outer')
    out['microstructure_strength_score'] = (
        0.22 * out['tape_conviction_score'].fillna(50) +
        0.18 * out['book_support_score'].fillna(50) +
        0.10 * out['offer_consumption_score'].fillna(50) +
        0.12 * out['gulungan_up_score'].fillna(0) +
        0.10 * out['effort_result_up'].fillna(50) +
        0.08 * out['post_up_followthrough_score'].fillna(50) +
        0.10 * (100 - out['transfer_suspicion'].fillna(50)) +
        0.10 * (100 - out['spoof_risk_score'].fillna(50))
    ).clip(0, 100)
    out['microstructure_weakness_score'] = (
        0.22 * out['tape_weakness_score'].fillna(50) +
        0.18 * out['book_pressure_down_score'].fillna(50) +
        0.10 * out['bid_consumption_score'].fillna(50) +
        0.12 * out['gulungan_down_score'].fillna(0) +
        0.10 * out['effort_result_down'].fillna(50) +
        0.08 * out['post_down_followthrough_score'].fillna(50) +
        0.10 * out['transfer_suspicion'].fillna(50) +
        0.10 * out['spoof_risk_score'].fillna(50)
    ).clip(0, 100)
    return out, events
