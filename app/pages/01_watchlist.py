import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace

st.set_page_config(page_title='Watchlist', layout='wide')
st.title('Watchlist')
ws = load_workspace()
watch = ws.watch.copy()

if watch.empty:
    st.warning('No watchlist file found.')
    st.stop()

if 'verdict' in watch.columns:
    verdicts = ['ALL'] + sorted(watch['verdict'].dropna().astype(str).unique().tolist())
    verdict = st.selectbox('Verdict', verdicts, index=0)
    if verdict != 'ALL':
        watch = watch[watch['verdict'].astype(str) == verdict]

sort_col = 'blended_long_prob' if 'blended_long_prob' in watch.columns else None
if sort_col and sort_col in watch.columns:
    try:
        watch = watch.sort_values(sort_col, ascending=False)
    except Exception:
        pass

st.caption(f'Active source: {ws.watch_mode} | rows: {len(watch)}')
st.dataframe(watch, use_container_width=True, hide_index=True)
