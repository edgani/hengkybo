import pandas as pd

from src.features.burst import detect_burst_events, add_burst_context_scores


def test_detect_bidirectional_bursts_basic():
    ts = pd.date_range('2026-04-15 09:00:00', periods=12, freq='1s')
    done = pd.DataFrame({
        'timestamp': list(ts),
        'trade_date': ['2026-04-15'] * 12,
        'ticker': ['TEST'] * 12,
        'price': [100,101,102,103,104,104,103,102,101,100,99,98],
        'lot':   [10,20,25,30,35,10,15,25,30,35,40,45],
        'side_aggressor': ['BUY','BUY','BUY','BUY','BUY','SELL','SELL','SELL','SELL','SELL','SELL','SELL'],
        'buyer_broker': ['AI'] * 12,
        'seller_broker': ['PD'] * 12,
    })
    daily, events = detect_burst_events(done, window_secs=4, follow_secs=2)
    assert not daily.empty
    assert 'gulungan_up_score' in daily.columns
    assert 'gulungan_down_score' in daily.columns
    assert daily.loc[0, 'up_burst_event_count'] >= 1
    assert daily.loc[0, 'down_burst_event_count'] >= 1
    assert set(events['direction']).issuperset({'UP', 'DOWN'})


def test_add_burst_context_scores_outputs_labels():
    df = pd.DataFrame({
        'date': ['2026-04-15'],
        'ticker': ['TEST'],
        'gulungan_up_score': [82],
        'gulungan_down_score': [15],
        'effort_result_up': [78],
        'effort_result_down': [40],
        'post_up_followthrough_score': [80],
        'post_down_followthrough_score': [45],
        'absorption_after_up_score': [20],
        'absorption_after_down_score': [35],
        'bullish_burst_score_intraday': [76],
        'bearish_burst_score_intraday': [20],
        'bull_trap_score_intraday': [18],
        'bear_trap_score_intraday': [22],
        'dry_score': [70],
        'wet_score': [30],
        'accumulation_quality_score': [72],
        'distribution_risk_score': [28],
        'microstructure_strength_score': [75],
        'microstructure_weakness_score': [25],
        'macro_alignment_score': [60],
        'macro_headwind_score': [35],
        'breakout_integrity_score': [68],
        'breakout_tension': [66],
        'resistance_headroom_score': [55],
        'phase_confidence': [72],
        'phase_deterioration_score': [30],
        'false_breakout_risk': [25],
    })
    out = add_burst_context_scores(df)
    assert 'bullish_burst_score' in out.columns
    assert 'dominant_burst_label_context' in out.columns
    assert out.loc[0, 'bullish_burst_score'] > out.loc[0, 'bearish_burst_score']
