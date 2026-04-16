import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace

st.set_page_config(page_title='Intraday Microstructure', layout='wide')
st.title('Intraday Microstructure')
ws = load_workspace()

if ws.intraday.empty:
    st.info('No intraday feature file detected yet. Run intraday pipeline when done-detail/orderbook real data sudah ada.')
else:
    st.caption(f'Rows: {len(ws.intraday)}')
    cols = [c for c in [
        'date','ticker','tape_conviction_score','tape_weakness_score','microstructure_strength_score','microstructure_weakness_score',
        'gulungan_up_score','gulungan_down_score','bullish_burst_score_intraday','bearish_burst_score_intraday',
        'bull_trap_score_intraday','bear_trap_score_intraday','dominant_burst_direction','dominant_event_label'
    ] if c in ws.intraday.columns]
    st.subheader('Daily intraday summary')
    st.dataframe(ws.intraday[cols] if cols else ws.intraday, use_container_width=True, hide_index=True)

if not ws.burst_events.empty:
    st.subheader('Burst events')
    event_cols = [c for c in [
        'date','ticker','burst_ts','direction','event_label','gulungan_up_score','gulungan_down_score',
        'effort_result_up','effort_result_down','post_up_followthrough_score','post_down_followthrough_score',
        'absorption_after_up_score','absorption_after_down_score','bullish_burst_score_intraday','bearish_burst_score_intraday',
        'bull_trap_score_intraday','bear_trap_score_intraday'
    ] if c in ws.burst_events.columns]
    st.dataframe(ws.burst_events[event_cols] if event_cols else ws.burst_events, use_container_width=True, hide_index=True)
