import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
from src.utils.app_data import load_workspace, active_tickers

st.set_page_config(page_title='Ticker Detail', layout='wide')
st.title('Ticker Detail')
ws = load_workspace()

if ws.prices.empty or 'ticker' not in ws.prices.columns:
    st.warning('Run pipeline or import prices first.')
    st.stop()

tickers = active_tickers(ws.prices)
ticker = st.selectbox('Ticker', tickers)

px = ws.prices[ws.prices['ticker'].astype(str).str.upper() == ticker].copy()
if 'date' in px.columns:
    px['date'] = pd.to_datetime(px['date'])
    px = px.sort_values('date')

st.subheader('Recent price rows')
st.dataframe(px.tail(30), use_container_width=True, hide_index=True)

watch = ws.watch
if not watch.empty and 'ticker' in watch.columns:
    row = watch[watch['ticker'].astype(str).str.upper() == ticker]
    if not row.empty:
        st.subheader('Latest watch verdict')
        show_cols = [c for c in [
            'date','ticker','verdict','confidence','phase','accumulation_quality_score','breakout_integrity_score','distribution_risk_score',
            'gulungan_up_score','gulungan_down_score','bullish_burst_score','bearish_burst_score','bull_trap_score','bear_trap_score',
            'dominant_burst_label_context','burst_note','why_now','invalidation'
        ] if c in row.columns]
        st.dataframe(row[show_cols] if show_cols else row, use_container_width=True, hide_index=True)

intraday = ws.intraday
if not intraday.empty and 'ticker' in intraday.columns:
    ir = intraday[intraday['ticker'].astype(str).str.upper() == ticker].copy()
    if not ir.empty:
        st.subheader('Intraday burst summary')
        icols = [c for c in [
            'date','dominant_burst_direction','dominant_event_label','gulungan_up_score','gulungan_down_score','effort_result_up','effort_result_down',
            'post_up_followthrough_score','post_down_followthrough_score','absorption_after_up_score','absorption_after_down_score',
            'bullish_burst_score_intraday','bearish_burst_score_intraday','bull_trap_score_intraday','bear_trap_score_intraday',
            'microstructure_strength_score','microstructure_weakness_score'
        ] if c in ir.columns]
        st.dataframe(ir[icols].tail(10) if icols else ir.tail(10), use_container_width=True, hide_index=True)

if not ws.burst_events.empty and 'ticker' in ws.burst_events.columns:
    be = ws.burst_events[ws.burst_events['ticker'].astype(str).str.upper() == ticker].copy()
    if not be.empty:
        st.subheader('Burst event tape')
        ecols = [c for c in [
            'date','burst_ts','direction','event_label','price_progress_ticks','follow_return_ticks','counter_move_ticks',
            'gulungan_up_score','gulungan_down_score','effort_result_up','effort_result_down',
            'bullish_burst_score_intraday','bearish_burst_score_intraday','bull_trap_score_intraday','bear_trap_score_intraday'
        ] if c in be.columns]
        st.dataframe(be[ecols].tail(50) if ecols else be.tail(50), use_container_width=True, hide_index=True)

brokers = ws.brokers
if not brokers.empty and {'ticker','broker_code'}.issubset(brokers.columns):
    b = brokers[brokers['ticker'].astype(str).str.upper() == ticker].copy()
    if not b.empty:
        sort_col = 'net_value' if 'net_value' in b.columns else ('net_lot' if 'net_lot' in b.columns else b.columns[0])
        try:
            b = b.sort_values(sort_col, ascending=False)
        except Exception:
            pass
        st.subheader('Broker summary snapshot')
        st.dataframe(b.head(40), use_container_width=True, hide_index=True)
