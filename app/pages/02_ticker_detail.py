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
        st.dataframe(row, use_container_width=True, hide_index=True)

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
