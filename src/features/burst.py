from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-9


def infer_tick_size(prices: pd.Series) -> float:
    vals = np.sort(pd.Series(prices).dropna().astype(float).unique())
    if len(vals) < 2:
        return 1.0
    diffs = np.diff(vals)
    diffs = diffs[diffs > 0]
    return float(np.min(diffs)) if len(diffs) else 1.0


def _clip01(x: float) -> float:
    return float(np.clip(x, 0.0, 1.0))


def _safe_fill(df: pd.DataFrame, col: str, default: float) -> pd.Series:
    if col not in df.columns:
        df[col] = default
    return df[col].fillna(default)


def _event_label(row: pd.Series) -> str:
    """Pure intraday label before EOD context is applied."""
    if row.get('direction') == 'UP':
        if row['bull_trap_score_intraday'] >= 70:
            return 'UP_FALSE_BREAKOUT_RISK'
        if row['bullish_burst_score_intraday'] >= 68:
            return 'UP_CONTINUATION_BURST'
        return 'UP_INITIATIVE_SWEEP'
    if row.get('direction') == 'DOWN':
        if row['bear_trap_score_intraday'] >= 70:
            return 'DOWN_BEAR_TRAP_RISK'
        if row['bearish_burst_score_intraday'] >= 68:
            return 'DOWN_CONTINUATION_BREAK'
        return 'DOWN_INITIATIVE_SWEEP'
    return 'NO_BURST'


def detect_burst_events(done: pd.DataFrame, window_secs: int = 4, follow_secs: int = 3) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Detect bidirectional burst events from done-detail.

    Returns
    -------
    daily : per ticker/day summary of strongest up/down burst features.
    events : event-level table with directional burst labels.
    """
    summary_cols = [
        'date', 'ticker', 'burst_event_count', 'up_burst_event_count', 'down_burst_event_count',
        'valid_bullish_burst_count', 'trap_burst_count', 'valid_bearish_burst_count', 'bear_trap_count',
        'gulungan_up_score', 'gulungan_down_score', 'effort_result_up', 'effort_result_down',
        'post_up_followthrough_score', 'post_down_followthrough_score',
        'absorption_after_up_score', 'absorption_after_down_score',
        'bullish_burst_score_intraday', 'bearish_burst_score_intraday',
        'bull_trap_score_intraday', 'bear_trap_score_intraday',
        'dominant_burst_direction', 'dominant_event_label', 'strongest_burst_ts',
    ]
    if done.empty:
        return pd.DataFrame(columns=summary_cols), pd.DataFrame(columns=[])

    df = done.copy().sort_values(['trade_date', 'ticker', 'timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['ts_sec'] = df['timestamp'].dt.floor('s')
    side = df['side_aggressor'].astype(str).str.upper()
    df['buy_aggr_lot'] = np.where(side.eq('BUY'), df['lot'], 0.0)
    df['sell_aggr_lot'] = np.where(side.eq('SELL'), df['lot'], 0.0)

    sec = df.groupby(['trade_date', 'ticker', 'ts_sec'], as_index=False).agg(
        sec_open=('price', 'first'),
        sec_close=('price', 'last'),
        sec_high=('price', 'max'),
        sec_low=('price', 'min'),
        trades_in_sec=('lot', 'size'),
        lot_in_sec=('lot', 'sum'),
        buy_aggr_lot_sec=('buy_aggr_lot', 'sum'),
        sell_aggr_lot_sec=('sell_aggr_lot', 'sum'),
    )

    daily_rows: list[dict] = []
    events: list[dict] = []

    for (trade_date, ticker), g in sec.groupby(['trade_date', 'ticker'], sort=False):
        g = g.sort_values('ts_sec').reset_index(drop=True)
        if len(g) < window_secs:
            daily_rows.append({
                'date': trade_date,
                'ticker': ticker,
                'burst_event_count': 0,
                'up_burst_event_count': 0,
                'down_burst_event_count': 0,
                'valid_bullish_burst_count': 0,
                'trap_burst_count': 0,
                'valid_bearish_burst_count': 0,
                'bear_trap_count': 0,
                'gulungan_up_score': 0.0,
                'gulungan_down_score': 0.0,
                'effort_result_up': 50.0,
                'effort_result_down': 50.0,
                'post_up_followthrough_score': 50.0,
                'post_down_followthrough_score': 50.0,
                'absorption_after_up_score': 50.0,
                'absorption_after_down_score': 50.0,
                'bullish_burst_score_intraday': 20.0,
                'bearish_burst_score_intraday': 20.0,
                'bull_trap_score_intraday': 20.0,
                'bear_trap_score_intraday': 20.0,
                'dominant_burst_direction': 'NONE',
                'dominant_event_label': 'NO_BURST',
                'strongest_burst_ts': pd.NaT,
            })
            continue

        tick = infer_tick_size(pd.concat([g['sec_open'], g['sec_close'], g['sec_high'], g['sec_low']]))
        roll_buy = [float(g.iloc[i - window_secs + 1:i + 1]['buy_aggr_lot_sec'].sum()) for i in range(window_secs - 1, len(g))]
        roll_sell = [float(g.iloc[i - window_secs + 1:i + 1]['sell_aggr_lot_sec'].sum()) for i in range(window_secs - 1, len(g))]
        roll_trade = [float(g.iloc[i - window_secs + 1:i + 1]['trades_in_sec'].sum()) for i in range(window_secs - 1, len(g))]
        buy_baseline = max(float(np.quantile(roll_buy, 0.80)), float(np.median(roll_buy)), 1.0)
        sell_baseline = max(float(np.quantile(roll_sell, 0.80)), float(np.median(roll_sell)), 1.0)
        trade_baseline = max(float(np.quantile(roll_trade, 0.70)), 3.0)

        ev_rows: list[dict] = []
        for i in range(window_secs - 1, len(g)):
            gw = g.iloc[i - window_secs + 1:i + 1]
            buy = float(gw['buy_aggr_lot_sec'].sum())
            sell = float(gw['sell_aggr_lot_sec'].sum())
            trades = float(gw['trades_in_sec'].sum())
            p_open = float(gw['sec_open'].iloc[0])
            p_close = float(gw['sec_close'].iloc[-1])
            p_high = float(gw['sec_high'].max())
            p_low = float(gw['sec_low'].min())
            up_ticks = max((p_close - p_open) / max(tick, EPS), 0.0)
            down_ticks = max((p_open - p_close) / max(tick, EPS), 0.0)
            buy_dom = buy / (buy + sell + EPS)
            sell_dom = sell / (buy + sell + EPS)
            trade_intensity = trades / (trade_baseline + EPS)
            up_candidate = (
                buy >= buy_baseline and
                buy / (sell + 1.0) >= 2.0 and
                up_ticks > 0 and
                trades >= max(3.0, trade_baseline * 0.8)
            )
            down_candidate = (
                sell >= sell_baseline and
                sell / (buy + 1.0) >= 2.0 and
                down_ticks > 0 and
                trades >= max(3.0, trade_baseline * 0.8)
            )
            if not (up_candidate or down_candidate):
                continue

            next_end = min(i + follow_secs, len(g) - 1)
            future = g.iloc[i + 1:next_end + 1]
            next_close = float(g['sec_close'].iloc[next_end])
            next_low = float(future['sec_low'].min()) if not future.empty else p_close
            next_high = float(future['sec_high'].max()) if not future.empty else p_close
            next_buy = float(future['buy_aggr_lot_sec'].sum()) if not future.empty else 0.0
            next_sell = float(future['sell_aggr_lot_sec'].sum()) if not future.empty else 0.0
            upper_wick_ratio = (p_high - max(p_open, p_close)) / max((p_high - p_low), EPS)
            lower_wick_ratio = (min(p_open, p_close) - p_low) / max((p_high - p_low), EPS)

            if up_candidate:
                follow_ticks = (next_close - p_close) / max(tick, EPS)
                pullback_ticks = max((p_close - next_low) / max(tick, EPS), 0.0)
                effort_eff = up_ticks / max(np.log1p(buy), EPS)
                gulungan_up_score = 100.0 * (
                    0.30 * _clip01((buy / (buy_baseline + EPS)) / 3.0) +
                    0.20 * _clip01((buy_dom - 0.5) / 0.5) +
                    0.20 * _clip01(up_ticks / 6.0) +
                    0.15 * _clip01((gw['sec_close'].diff() > 0).sum() / max(len(gw) - 1, 1)) +
                    0.15 * _clip01(trade_intensity / 3.0)
                )
                effort_result_up = 100.0 * _clip01(effort_eff / 1.4)
                post_up_followthrough = 100.0 * (
                    0.45 * _clip01((follow_ticks + 1.0) / 4.0) +
                    0.35 * (1.0 - _clip01(pullback_ticks / 6.0)) +
                    0.20 * (1.0 - _clip01(upper_wick_ratio))
                )
                absorption_after_up = 100.0 * (
                    0.30 * _clip01(next_sell / max(buy, 1.0)) +
                    0.25 * _clip01(pullback_ticks / 6.0) +
                    0.25 * (1.0 - _clip01((follow_ticks + 1.0) / 4.0)) +
                    0.20 * _clip01(upper_wick_ratio)
                )
                bullish_burst = np.clip(
                    0.30 * gulungan_up_score +
                    0.25 * effort_result_up +
                    0.25 * post_up_followthrough -
                    0.20 * absorption_after_up,
                    0.0, 100.0,
                )
                bull_trap = np.clip(
                    0.30 * gulungan_up_score +
                    0.25 * (100.0 - effort_result_up) +
                    0.20 * (100.0 - post_up_followthrough) +
                    0.25 * absorption_after_up,
                    0.0, 100.0,
                )
                row = {
                    'date': trade_date,
                    'ticker': ticker,
                    'burst_ts': gw['ts_sec'].iloc[-1],
                    'window_open_ts': gw['ts_sec'].iloc[0],
                    'direction': 'UP',
                    'buy_aggr_lot_w': buy,
                    'sell_aggr_lot_w': sell,
                    'trade_count_w': trades,
                    'buy_dom_ratio': buy_dom,
                    'price_progress_ticks': up_ticks,
                    'follow_return_ticks': follow_ticks,
                    'counter_move_ticks': pullback_ticks,
                    'upper_wick_ratio': upper_wick_ratio,
                    'lower_wick_ratio': lower_wick_ratio,
                    'gulungan_up_score': gulungan_up_score,
                    'gulungan_down_score': 0.0,
                    'effort_result_up': effort_result_up,
                    'effort_result_down': 50.0,
                    'post_up_followthrough_score': post_up_followthrough,
                    'post_down_followthrough_score': 50.0,
                    'absorption_after_up_score': absorption_after_up,
                    'absorption_after_down_score': 50.0,
                    'bullish_burst_score_intraday': bullish_burst,
                    'bearish_burst_score_intraday': 20.0,
                    'bull_trap_score_intraday': bull_trap,
                    'bear_trap_score_intraday': 20.0,
                    'valid_bullish_burst': int((bullish_burst >= 65.0) and (bull_trap < 55.0)),
                    'trap_burst': int(bull_trap >= 65.0),
                    'valid_bearish_burst': 0,
                    'bear_trap': 0,
                }
                row['event_label'] = _event_label(pd.Series(row))
                ev_rows.append(row)

            if down_candidate:
                follow_ticks = (p_close - next_close) / max(tick, EPS)
                rebound_ticks = max((next_high - p_close) / max(tick, EPS), 0.0)
                effort_eff = down_ticks / max(np.log1p(sell), EPS)
                gulungan_down_score = 100.0 * (
                    0.30 * _clip01((sell / (sell_baseline + EPS)) / 3.0) +
                    0.20 * _clip01((sell_dom - 0.5) / 0.5) +
                    0.20 * _clip01(down_ticks / 6.0) +
                    0.15 * _clip01((gw['sec_close'].diff() < 0).sum() / max(len(gw) - 1, 1)) +
                    0.15 * _clip01(trade_intensity / 3.0)
                )
                effort_result_down = 100.0 * _clip01(effort_eff / 1.4)
                post_down_followthrough = 100.0 * (
                    0.45 * _clip01((follow_ticks + 1.0) / 4.0) +
                    0.35 * (1.0 - _clip01(rebound_ticks / 6.0)) +
                    0.20 * (1.0 - _clip01(lower_wick_ratio))
                )
                absorption_after_down = 100.0 * (
                    0.30 * _clip01(next_buy / max(sell, 1.0)) +
                    0.25 * _clip01(rebound_ticks / 6.0) +
                    0.25 * (1.0 - _clip01((follow_ticks + 1.0) / 4.0)) +
                    0.20 * _clip01(lower_wick_ratio)
                )
                bearish_burst = np.clip(
                    0.30 * gulungan_down_score +
                    0.25 * effort_result_down +
                    0.25 * post_down_followthrough -
                    0.20 * absorption_after_down,
                    0.0, 100.0,
                )
                bear_trap = np.clip(
                    0.30 * gulungan_down_score +
                    0.25 * (100.0 - effort_result_down) +
                    0.20 * (100.0 - post_down_followthrough) +
                    0.25 * absorption_after_down,
                    0.0, 100.0,
                )
                row = {
                    'date': trade_date,
                    'ticker': ticker,
                    'burst_ts': gw['ts_sec'].iloc[-1],
                    'window_open_ts': gw['ts_sec'].iloc[0],
                    'direction': 'DOWN',
                    'buy_aggr_lot_w': buy,
                    'sell_aggr_lot_w': sell,
                    'trade_count_w': trades,
                    'sell_dom_ratio': sell_dom,
                    'price_progress_ticks': down_ticks,
                    'follow_return_ticks': follow_ticks,
                    'counter_move_ticks': rebound_ticks,
                    'upper_wick_ratio': upper_wick_ratio,
                    'lower_wick_ratio': lower_wick_ratio,
                    'gulungan_up_score': 0.0,
                    'gulungan_down_score': gulungan_down_score,
                    'effort_result_up': 50.0,
                    'effort_result_down': effort_result_down,
                    'post_up_followthrough_score': 50.0,
                    'post_down_followthrough_score': post_down_followthrough,
                    'absorption_after_up_score': 50.0,
                    'absorption_after_down_score': absorption_after_down,
                    'bullish_burst_score_intraday': 20.0,
                    'bearish_burst_score_intraday': bearish_burst,
                    'bull_trap_score_intraday': 20.0,
                    'bear_trap_score_intraday': bear_trap,
                    'valid_bullish_burst': 0,
                    'trap_burst': 0,
                    'valid_bearish_burst': int((bearish_burst >= 65.0) and (bear_trap < 55.0)),
                    'bear_trap': int(bear_trap >= 65.0),
                }
                row['event_label'] = _event_label(pd.Series(row))
                ev_rows.append(row)

        if ev_rows:
            ev = pd.DataFrame(ev_rows)
            up_ev = ev[ev['direction'] == 'UP'].sort_values(['bullish_burst_score_intraday', 'gulungan_up_score'], ascending=False)
            down_ev = ev[ev['direction'] == 'DOWN'].sort_values(['bearish_burst_score_intraday', 'gulungan_down_score'], ascending=False)
            top_up = up_ev.iloc[0] if not up_ev.empty else None
            top_down = down_ev.iloc[0] if not down_ev.empty else None
            dominant = None
            if top_up is not None and top_down is not None:
                up_strength = float(top_up['bullish_burst_score_intraday']) - 0.35 * float(top_up['bull_trap_score_intraday'])
                down_strength = float(top_down['bearish_burst_score_intraday']) - 0.35 * float(top_down['bear_trap_score_intraday'])
                dominant = top_up if up_strength >= down_strength else top_down
            else:
                dominant = top_up if top_up is not None else top_down

            daily_rows.append({
                'date': trade_date,
                'ticker': ticker,
                'burst_event_count': int(len(ev)),
                'up_burst_event_count': int(len(up_ev)),
                'down_burst_event_count': int(len(down_ev)),
                'valid_bullish_burst_count': int(ev['valid_bullish_burst'].sum()),
                'trap_burst_count': int(ev['trap_burst'].sum()),
                'valid_bearish_burst_count': int(ev['valid_bearish_burst'].sum()),
                'bear_trap_count': int(ev['bear_trap'].sum()),
                'gulungan_up_score': float(top_up['gulungan_up_score']) if top_up is not None else 0.0,
                'gulungan_down_score': float(top_down['gulungan_down_score']) if top_down is not None else 0.0,
                'effort_result_up': float(top_up['effort_result_up']) if top_up is not None else 50.0,
                'effort_result_down': float(top_down['effort_result_down']) if top_down is not None else 50.0,
                'post_up_followthrough_score': float(top_up['post_up_followthrough_score']) if top_up is not None else 50.0,
                'post_down_followthrough_score': float(top_down['post_down_followthrough_score']) if top_down is not None else 50.0,
                'absorption_after_up_score': float(top_up['absorption_after_up_score']) if top_up is not None else 50.0,
                'absorption_after_down_score': float(top_down['absorption_after_down_score']) if top_down is not None else 50.0,
                'bullish_burst_score_intraday': float(top_up['bullish_burst_score_intraday']) if top_up is not None else 20.0,
                'bearish_burst_score_intraday': float(top_down['bearish_burst_score_intraday']) if top_down is not None else 20.0,
                'bull_trap_score_intraday': float(top_up['bull_trap_score_intraday']) if top_up is not None else 20.0,
                'bear_trap_score_intraday': float(top_down['bear_trap_score_intraday']) if top_down is not None else 20.0,
                'dominant_burst_direction': str(dominant['direction']) if dominant is not None else 'NONE',
                'dominant_event_label': str(dominant['event_label']) if dominant is not None else 'NO_BURST',
                'strongest_burst_ts': dominant['burst_ts'] if dominant is not None else pd.NaT,
            })
            events.extend(ev.to_dict('records'))
        else:
            daily_rows.append({
                'date': trade_date,
                'ticker': ticker,
                'burst_event_count': 0,
                'up_burst_event_count': 0,
                'down_burst_event_count': 0,
                'valid_bullish_burst_count': 0,
                'trap_burst_count': 0,
                'valid_bearish_burst_count': 0,
                'bear_trap_count': 0,
                'gulungan_up_score': 0.0,
                'gulungan_down_score': 0.0,
                'effort_result_up': 50.0,
                'effort_result_down': 50.0,
                'post_up_followthrough_score': 50.0,
                'post_down_followthrough_score': 50.0,
                'absorption_after_up_score': 50.0,
                'absorption_after_down_score': 50.0,
                'bullish_burst_score_intraday': 20.0,
                'bearish_burst_score_intraday': 20.0,
                'bull_trap_score_intraday': 20.0,
                'bear_trap_score_intraday': 20.0,
                'dominant_burst_direction': 'NONE',
                'dominant_event_label': 'NO_BURST',
                'strongest_burst_ts': pd.NaT,
            })

    return pd.DataFrame(daily_rows), pd.DataFrame(events)


def add_burst_context_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Blend intraday burst into broader EOD context.

    Produces continuation/trap/climax/capitulation scores and final burst labels.
    """
    out = df.copy()
    defaults = {
        'gulungan_up_score': 0.0,
        'gulungan_down_score': 0.0,
        'effort_result_up': 50.0,
        'effort_result_down': 50.0,
        'post_up_followthrough_score': 50.0,
        'post_down_followthrough_score': 50.0,
        'absorption_after_up_score': 50.0,
        'absorption_after_down_score': 50.0,
        'bullish_burst_score_intraday': 20.0,
        'bearish_burst_score_intraday': 20.0,
        'bull_trap_score_intraday': 20.0,
        'bear_trap_score_intraday': 20.0,
        'dry_score': 50.0,
        'wet_score': 50.0,
        'accumulation_quality_score': 50.0,
        'distribution_risk_score': 50.0,
        'microstructure_strength_score': 50.0,
        'microstructure_weakness_score': 50.0,
        'macro_alignment_score': 50.0,
        'macro_headwind_score': 50.0,
        'breakout_integrity_score': 50.0,
        'breakout_tension': 50.0,
        'resistance_headroom_score': 50.0,
        'phase_confidence': 50.0,
        'phase_deterioration_score': 50.0,
        'false_breakout_risk': 50.0,
    }
    for col, default in defaults.items():
        out[col] = _safe_fill(out, col, default)

    continuation_quality = (
        0.25 * out['breakout_integrity_score'] +
        0.20 * out['accumulation_quality_score'] +
        0.20 * out['dry_score'] +
        0.20 * out['macro_alignment_score'] +
        0.15 * out['phase_confidence']
    ).clip(0, 100)
    out['climax_up_risk'] = (
        0.20 * out['wet_score'] +
        0.20 * out['distribution_risk_score'] +
        0.15 * (100 - out['resistance_headroom_score']) +
        0.15 * out['breakout_tension'] +
        0.15 * out['false_breakout_risk'] +
        0.15 * out['phase_deterioration_score']
    ).clip(0, 100)
    out['climax_vs_continuation_score'] = (50 + 0.5 * (continuation_quality - out['climax_up_risk'])).clip(0, 100)

    out['capitulation_down_risk'] = (
        0.20 * out['microstructure_weakness_score'] +
        0.20 * out['distribution_risk_score'] +
        0.15 * out['macro_headwind_score'] +
        0.15 * out['phase_deterioration_score'] +
        0.15 * out['absorption_after_down_score'] +
        0.15 * (100 - out['dry_score'])
    ).clip(0, 100)

    out['bullish_burst_score'] = np.clip(
        0.20 * out['bullish_burst_score_intraday'] +
        0.15 * out['gulungan_up_score'] +
        0.15 * out['effort_result_up'] +
        0.15 * out['post_up_followthrough_score'] +
        0.10 * out['microstructure_strength_score'] +
        0.10 * out['accumulation_quality_score'] +
        0.10 * out['dry_score'] +
        0.05 * out['climax_vs_continuation_score'] -
        0.15 * out['absorption_after_up_score'] -
        0.10 * out['distribution_risk_score'],
        0.0, 100.0,
    )
    out['bull_trap_score'] = np.clip(
        0.20 * out['bull_trap_score_intraday'] +
        0.20 * out['gulungan_up_score'] +
        0.15 * (100 - out['effort_result_up']) +
        0.15 * (100 - out['post_up_followthrough_score']) +
        0.15 * out['absorption_after_up_score'] +
        0.10 * out['wet_score'] +
        0.05 * out['distribution_risk_score'] +
        0.10 * out['climax_up_risk'],
        0.0, 100.0,
    )
    out['bearish_burst_score'] = np.clip(
        0.20 * out['bearish_burst_score_intraday'] +
        0.15 * out['gulungan_down_score'] +
        0.15 * out['effort_result_down'] +
        0.15 * out['post_down_followthrough_score'] +
        0.10 * out['microstructure_weakness_score'] +
        0.10 * out['distribution_risk_score'] +
        0.10 * out['macro_headwind_score'] +
        0.05 * out['phase_deterioration_score'] -
        0.15 * out['absorption_after_down_score'] -
        0.10 * out['capitulation_down_risk'],
        0.0, 100.0,
    )
    out['bear_trap_score'] = np.clip(
        0.20 * out['bear_trap_score_intraday'] +
        0.20 * out['gulungan_down_score'] +
        0.15 * (100 - out['effort_result_down']) +
        0.15 * (100 - out['post_down_followthrough_score']) +
        0.15 * out['absorption_after_down_score'] +
        0.05 * out['dry_score'] +
        0.10 * out['capitulation_down_risk'],
        0.0, 100.0,
    )

    out['burst_signal'] = np.select(
        [out['bullish_burst_score'] >= 70, out['bullish_burst_score'] >= 55],
        ['VALID_BULLISH_BURST', 'WATCH_BURST'],
        default='WEAK_OR_NO_UP_BURST'
    )
    out['trap_signal'] = np.select(
        [out['bull_trap_score'] >= 70, out['bull_trap_score'] >= 55],
        ['BURST_TRAP_RISK', 'CAUTION'],
        default='LOW_TRAP_RISK'
    )
    out['down_burst_signal'] = np.select(
        [out['bearish_burst_score'] >= 70, out['bearish_burst_score'] >= 55],
        ['VALID_BEARISH_BURST', 'WATCH_DOWN_BURST'],
        default='WEAK_OR_NO_DOWN_BURST'
    )
    out['bear_trap_signal'] = np.select(
        [out['bear_trap_score'] >= 70, out['bear_trap_score'] >= 55],
        ['BEAR_TRAP_OR_CAPITULATION', 'CAUTION_REBOUND'],
        default='LOW_BEAR_TRAP_RISK'
    )

    labels = []
    for row in out.itertuples(index=False):
        if getattr(row, 'bull_trap_score', 0) >= 70 and getattr(row, 'gulungan_up_score', 0) >= 55:
            labels.append('UP_FALSE_BREAKOUT_RISK')
        elif getattr(row, 'bullish_burst_score', 0) >= 70 and getattr(row, 'climax_up_risk', 0) < 65:
            labels.append('UP_CONTINUATION_BURST')
        elif getattr(row, 'climax_up_risk', 0) >= 70 and getattr(row, 'gulungan_up_score', 0) >= 55:
            labels.append('UP_CLIMAX_RISK')
        elif getattr(row, 'bear_trap_score', 0) >= 70 and getattr(row, 'gulungan_down_score', 0) >= 55:
            labels.append('DOWN_CAPITULATION_RISK')
        elif getattr(row, 'bearish_burst_score', 0) >= 70 and getattr(row, 'capitulation_down_risk', 0) < 65:
            labels.append('DOWN_CONTINUATION_BREAK')
        elif getattr(row, 'gulungan_down_score', 0) >= 60:
            labels.append('DOWN_INITIATIVE_SWEEP')
        elif getattr(row, 'gulungan_up_score', 0) >= 60:
            labels.append('UP_INITIATIVE_SWEEP')
        else:
            labels.append('NO_BURST')
    out['dominant_burst_label_context'] = labels
    return out
