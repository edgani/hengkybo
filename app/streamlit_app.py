import pandas as pd
from pathlib import Path

try:
    import streamlit as st
except Exception:
    raise SystemExit('Streamlit is not installed. Install requirements and run `streamlit run app/streamlit_app.py`.')

BASE = Path(__file__).resolve().parents[1] / 'data' / 'features'
RAW = Path(__file__).resolve().parents[1] / 'data' / 'raw'

st.set_page_config(page_title='IDX Flow Engine V4.1', layout='wide')
st.title('IDX Flow Engine V4.1')
st.warning('This build ships with synthetic demo data by default. AAA..HHH and the broker list are placeholders until you replace data/raw/*.csv with real feeds.')

watch = pd.read_csv(BASE / 'latest_watchlist_v4.csv') if (BASE / 'latest_watchlist_v4.csv').exists() else pd.DataFrame()
metrics = pd.read_csv(BASE / 'walk_forward_metrics.csv') if (BASE / 'walk_forward_metrics.csv').exists() else pd.DataFrame()
profiles = pd.read_csv(BASE / 'broker_profiles_latest.csv') if (BASE / 'broker_profiles_latest.csv').exists() else pd.DataFrame()

prices = pd.read_csv(RAW / 'prices_daily.csv') if (RAW / 'prices_daily.csv').exists() else pd.DataFrame()
brokers = pd.read_csv(RAW / 'broker_summary_daily.csv') if (RAW / 'broker_summary_daily.csv').exists() else pd.DataFrame()

c1, c2, c3 = st.columns(3)
with c1:
    st.metric('Raw tickers', int(prices['ticker'].nunique()) if 'ticker' in prices.columns else 0)
with c2:
    st.metric('Raw brokers', int(brokers['broker_code'].nunique()) if 'broker_code' in brokers.columns else 0)
with c3:
    st.metric('Rows in watchlist', len(watch))

if 'ticker' in prices.columns:
    st.caption('Tickers loaded: ' + ', '.join(map(str, sorted(prices['ticker'].unique())[:20])))
if 'broker_code' in brokers.columns:
    st.caption('Broker codes loaded: ' + ', '.join(map(str, sorted(brokers['broker_code'].unique())[:50])))

st.subheader('Latest Watchlist')
st.dataframe(watch, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader('Walk-forward Metrics')
    st.dataframe(metrics, use_container_width=True)
with col2:
    st.subheader('Latest Broker Profiles')
    st.dataframe(profiles, use_container_width=True)
