import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace, data_quality_report

ws = load_workspace()
qa = data_quality_report(ws.prices, ws.brokers, ws.broker_master)

st.set_page_config(page_title='IDX Flow Engine V4.5', layout='wide')
st.title('IDX Flow Engine V4.5')

if ws.prices_mode == 'DEMO' or ws.brokers_mode == 'DEMO':
    st.warning('Workspace masih campur demo atau sample data. Untuk hasil real, import prices dan broker summary real dulu.')
else:
    st.success('Workspace sudah deteksi raw prices dan broker summary real.')

m1, m2, m3, m4 = st.columns(4)
m1.metric('Prices source', ws.prices_mode)
m2.metric('Broker source', ws.brokers_mode)
m3.metric('Watchlist source', ws.watch_mode)
m4.metric('Raw tickers', qa['ticker_count'])

m5, m6, m7, m8 = st.columns(4)
m5.metric('Raw brokers', qa['broker_count'])
m6.metric('Broker master rows', qa['broker_master_count'])
m7.metric('Broker master loaded', qa['broker_master_loaded_count'])
m8.metric('Watchlist rows', len(ws.watch))

st.caption('Prices file: ' + str(ws.prices_path))
st.caption('Broker file: ' + str(ws.brokers_path))

c1, c2 = st.columns(2)
with c1:
    st.subheader('Ticker coverage')
    st.write(', '.join(qa['tickers_preview']) if qa['tickers_preview'] else 'No tickers loaded.')
with c2:
    st.subheader('Broker coverage')
    st.write(', '.join(qa['brokers_preview']) if qa['brokers_preview'] else 'No broker codes loaded.')

if not ws.watch.empty:
    st.subheader('Latest Watchlist')
    st.dataframe(ws.watch, use_container_width=True, hide_index=True)

x1, x2 = st.columns(2)
with x1:
    st.subheader('Walk-forward Metrics')
    st.dataframe(ws.metrics, use_container_width=True, hide_index=True)
with x2:
    st.subheader('Calibration Report')
    st.dataframe(ws.calibration, use_container_width=True, hide_index=True)
