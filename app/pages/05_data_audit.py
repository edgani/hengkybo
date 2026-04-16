import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.utils.app_data import load_workspace, data_quality_report, summarize_columns

st.set_page_config(page_title='Data Audit', layout='wide')
st.title('Data Audit')
ws = load_workspace()
qa = data_quality_report(ws.prices, ws.brokers, ws.broker_master)

m1, m2, m3, m4 = st.columns(4)
m1.metric('Price rows', qa['price_rows'])
m2.metric('Broker rows', qa['broker_rows'])
m3.metric('Ticker count', qa['ticker_count'])
m4.metric('Broker count', qa['broker_count'])

m5, m6, m7, m8 = st.columns(4)
m5.metric('Price start', qa['prices_min_date'])
m6.metric('Price end', qa['prices_max_date'])
m7.metric('Broker start', qa['brokers_min_date'])
m8.metric('Broker end', qa['brokers_max_date'])

s1, s2, s3 = st.columns(3)
with s1:
    st.subheader('Price columns')
    st.write(summarize_columns(ws.prices))
with s2:
    st.subheader('Broker columns')
    st.write(summarize_columns(ws.brokers))
with s3:
    st.subheader('Broker master columns')
    st.write(summarize_columns(ws.broker_master))
