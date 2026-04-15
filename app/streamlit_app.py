import pandas as pd
from pathlib import Path

try:
    import streamlit as st
except Exception:
    raise SystemExit('Streamlit is not installed. Install requirements and run `streamlit run app/streamlit_app.py`.')

DATA = Path(__file__).resolve().parents[1] / 'data'
FEATURES = DATA / 'features'
RAW = DATA / 'raw'


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def choose_raw_file(real_name: str, demo_name: str) -> tuple[Path, str]:
    real_path = RAW / real_name
    demo_path = RAW / demo_name
    if real_path.exists():
        return real_path, 'REAL'
    return demo_path, 'DEMO'


prices_path, prices_mode = choose_raw_file('prices_daily_real.csv', 'prices_daily.csv')
brokers_path, brokers_mode = choose_raw_file('broker_summary_daily_real.csv', 'broker_summary_daily.csv')
watch_real = FEATURES / 'latest_watchlist_real_eod.csv'
watch_demo = FEATURES / 'latest_watchlist_v4.csv'
watch_path = watch_real if watch_real.exists() else watch_demo
watch_mode = 'REAL_EOD_SMOKE' if watch_real.exists() else 'DEMO_V4'

st.set_page_config(page_title='IDX Flow Engine V4.4', layout='wide')
st.title('IDX Flow Engine V4.4')

watch = read_csv_if_exists(watch_path)
metrics = read_csv_if_exists(FEATURES / 'walk_forward_metrics.csv')
profiles = read_csv_if_exists(FEATURES / 'broker_profiles_latest.csv')
prices = read_csv_if_exists(prices_path)
brokers = read_csv_if_exists(brokers_path)

if prices_mode == 'DEMO' or brokers_mode == 'DEMO':
    st.warning('Part of the current workspace still uses demo data. Replace data/raw/*.csv or import real files with the adapter pipelines.')
else:
    st.success('Real raw files detected for both prices and broker summary.')

meta1, meta2, meta3 = st.columns(3)
with meta1:
    st.metric('Prices source', prices_mode)
with meta2:
    st.metric('Broker source', brokers_mode)
with meta3:
    st.metric('Watchlist source', watch_mode)

info1, info2, info3 = st.columns(3)
with info1:
    st.metric('Raw tickers', int(prices['ticker'].nunique()) if 'ticker' in prices.columns else 0)
with info2:
    st.metric('Raw brokers', int(brokers['broker_code'].nunique()) if 'broker_code' in brokers.columns else 0)
with info3:
    st.metric('Rows in watchlist', len(watch))

if 'ticker' in prices.columns:
    tickers = ', '.join(map(str, sorted(prices['ticker'].dropna().astype(str).unique())[:40]))
    st.caption('Tickers loaded: ' + tickers)
if 'broker_code' in brokers.columns:
    codes = ', '.join(map(str, sorted(brokers['broker_code'].dropna().astype(str).unique())[:60]))
    st.caption('Broker codes loaded: ' + codes)

if not watch.empty and 'verdict' in watch.columns:
    st.subheader('Watchlist breakdown')
    vc = watch['verdict'].value_counts().rename_axis('verdict').reset_index(name='count')
    st.dataframe(vc, use_container_width=True, hide_index=True)

st.subheader('Latest Watchlist')
st.dataframe(watch, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    st.subheader('Walk-forward Metrics')
    st.dataframe(metrics, use_container_width=True)
with col2:
    st.subheader('Latest Broker Profiles')
    st.dataframe(profiles, use_container_width=True)
