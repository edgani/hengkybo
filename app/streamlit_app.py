import pandas as pd
from pathlib import Path

try:
    import streamlit as st
except Exception:
    raise SystemExit('Streamlit is not installed. Install requirements and run `streamlit run app/streamlit_app.py`.')

BASE = Path(__file__).resolve().parents[1] / 'data' / 'features'

st.set_page_config(page_title='IDX Flow Engine V4', layout='wide')
st.title('IDX Flow Engine V4')

watch = pd.read_csv(BASE / 'latest_watchlist_v4.csv') if (BASE / 'latest_watchlist_v4.csv').exists() else pd.DataFrame()
metrics = pd.read_csv(BASE / 'walk_forward_metrics.csv') if (BASE / 'walk_forward_metrics.csv').exists() else pd.DataFrame()
profiles = pd.read_csv(BASE / 'broker_profiles_latest.csv') if (BASE / 'broker_profiles_latest.csv').exists() else pd.DataFrame()

st.subheader('Latest Watchlist')
st.dataframe(watch, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader('Walk-forward Metrics')
    st.dataframe(metrics, use_container_width=True)
with col2:
    st.subheader('Latest Broker Profiles')
    st.dataframe(profiles, use_container_width=True)
