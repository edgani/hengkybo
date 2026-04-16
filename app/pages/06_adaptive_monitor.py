import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace

st.set_page_config(page_title='Adaptive Monitor', layout='wide')
st.title('Adaptive Monitor')
ws = load_workspace()

c1, c2 = st.columns(2)
with c1:
    st.subheader('Regime')
    if ws.regime.empty:
        st.info('No regime file yet.')
    else:
        st.dataframe(ws.regime, use_container_width=True, hide_index=True)
with c2:
    st.subheader('Drift')
    if ws.drift.empty:
        st.info('No drift report yet.')
    else:
        st.dataframe(ws.drift, use_container_width=True, hide_index=True)
