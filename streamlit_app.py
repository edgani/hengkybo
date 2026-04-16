import streamlit as st
import pandas as pd
from datetime import date, timedelta

st.set_page_config(page_title='IDX Free EOD Smoke', layout='wide')
st.title('IDX Free EOD Smoke')
st.caption('Single-file app. No pages/ directory. Uses yfinance .JK for quick smoke tests.')

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data(tickers, start):
    import yfinance as yf
    out = []
    for t in tickers:
        symbol = t.strip().upper()
        if not symbol:
            continue
        df = yf.download(f'{symbol}.JK', start=start, progress=False, auto_adjust=False, interval='1d')
        if df is None or df.empty:
            continue
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        df = df.reset_index().rename(columns={'Date':'date','Open':'open','High':'high','Low':'low','Close':'close','Adj Close':'adj_close','Volume':'volume'})
        keep = [c for c in ['date','open','high','low','close','adj_close','volume'] if c in df.columns]
        df = df[keep].copy()
        df['ticker'] = symbol
        out.append(df)
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)

col1, col2 = st.columns([3,1])
with col1:
    tickers_text = st.text_input('Tickers IDX (comma-separated, without .JK)', 'BBCA,BBRI,BMRI,TLKM,ASII,ICBP,INDF,ANTM,PANI,BJTM')
with col2:
    start = st.date_input('Start date', value=date.today() - timedelta(days=365))

if st.button('Fetch free EOD'):
    tickers = [x.strip() for x in tickers_text.split(',')]
    try:
        df = fetch_data(tickers, start.isoformat())
    except Exception as e:
        st.error(f'Fetch failed: {e}')
        st.stop()
    if df.empty:
        st.warning('No data returned. Check ticker symbols or network access in the deployed environment.')
        st.stop()
    latest = df.sort_values(['ticker','date']).groupby('ticker', as_index=False).tail(1).copy()
    latest['ret_20d_pct'] = None
    for t in latest['ticker'].unique():
        sub = df[df['ticker']==t].sort_values('date')
        if len(sub) > 20:
            ret = sub['close'].iloc[-1] / sub['close'].iloc[-21] - 1
            latest.loc[latest['ticker']==t, 'ret_20d_pct'] = ret * 100
    latest = latest[['ticker','date','open','high','low','close','volume','ret_20d_pct']].sort_values('ticker')
    st.success(f'Loaded {latest.ticker.nunique()} tickers and {len(df):,} daily rows.')
    st.dataframe(latest, use_container_width=True, hide_index=True)
    st.download_button('Download latest snapshot CSV', latest.to_csv(index=False).encode('utf-8'), file_name='latest_idx_eod_snapshot.csv', mime='text/csv')

st.markdown('### Deploy checks')
st.code('''
1. App file path must be: streamlit_app.py
2. There must be NO pages/ directory next to streamlit_app.py
3. Do not deploy on top of an old multipage repo
4. This app intentionally does not use st.navigation
''')
