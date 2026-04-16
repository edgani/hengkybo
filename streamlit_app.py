import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import streamlit as st
from src.utils.app_data import (
    load_workspace,
    data_quality_report,
    broker_coverage_report,
    active_tickers,
    summarize_columns,
)

st.set_page_config(page_title='IDX Flow Engine V4.10 Clean Deploy', layout='wide')

ws = load_workspace()
qa = data_quality_report(ws.prices, ws.brokers, ws.broker_master)


def render_home() -> None:
    st.title('IDX Flow Engine V4.10 Clean Deploy')
    st.caption('Single-file app mode. Ini sengaja dibuat tanpa `st.navigation` dan tanpa folder `pages/` supaya lebih tahan error di deployment.')

    if ws.prices_mode == 'DEMO' or ws.brokers_mode == 'DEMO':
        st.warning('Workspace masih campur demo/sample data. Untuk hasil real, fetch/import EOD real dulu, lalu kalau ada import broker summary real.')
    else:
        st.success('Workspace sudah membaca raw prices dan broker summary real.')

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

    st.caption(f'Prices file: {ws.prices_path}')
    st.caption(f'Broker file: {ws.brokers_path}')

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


def render_watchlist() -> None:
    st.title('Watchlist')
    watch = ws.watch.copy()
    if watch.empty:
        st.warning('No watchlist file found. Jalankan pipeline real EOD dulu.')
        st.code('python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01\npython -m src.pipelines.run_real_eod_smoke', language='bash')
        return

    if 'verdict' in watch.columns:
        verdicts = ['ALL'] + sorted(watch['verdict'].dropna().astype(str).unique().tolist())
        verdict = st.selectbox('Verdict', verdicts, index=0)
        if verdict != 'ALL':
            watch = watch[watch['verdict'].astype(str) == verdict]

    sort_col = 'blended_long_prob' if 'blended_long_prob' in watch.columns else ('long_score' if 'long_score' in watch.columns else None)
    if sort_col and sort_col in watch.columns:
        try:
            watch = watch.sort_values(sort_col, ascending=False)
        except Exception:
            pass

    st.caption(f'Active source: {ws.watch_mode} | rows: {len(watch)}')
    st.dataframe(watch, use_container_width=True, hide_index=True)


def render_ticker_detail() -> None:
    st.title('Ticker Detail')
    if ws.prices.empty or 'ticker' not in ws.prices.columns:
        st.warning('Run pipeline or import prices first.')
        return

    tickers = active_tickers(ws.prices)
    if not tickers:
        st.warning('No tickers in current prices file.')
        return
    ticker = st.selectbox('Ticker', tickers)

    px = ws.prices[ws.prices['ticker'].astype(str).str.upper() == ticker].copy()
    if 'date' in px.columns:
        px['date'] = pd.to_datetime(px['date'])
        px = px.sort_values('date')

    st.subheader('Recent price rows')
    st.dataframe(px.tail(60), use_container_width=True, hide_index=True)

    watch = ws.watch
    if not watch.empty and 'ticker' in watch.columns:
        row = watch[watch['ticker'].astype(str).str.upper() == ticker]
        if not row.empty:
            st.subheader('Latest watch verdict')
            show_cols = [c for c in [
                'date','ticker','verdict','confidence','phase','accumulation_quality_score','breakout_integrity_score','distribution_risk_score',
                'gulungan_up_score','gulungan_down_score','bullish_burst_score','bearish_burst_score','bull_trap_score','bear_trap_score',
                'dominant_burst_label_context','burst_note','why_now','invalidation'
            ] if c in row.columns]
            st.dataframe(row[show_cols] if show_cols else row, use_container_width=True, hide_index=True)

    if not ws.intraday.empty and 'ticker' in ws.intraday.columns:
        ir = ws.intraday[ws.intraday['ticker'].astype(str).str.upper() == ticker].copy()
        if not ir.empty:
            st.subheader('Intraday burst summary')
            st.dataframe(ir.tail(20), use_container_width=True, hide_index=True)

    if not ws.burst_events.empty and 'ticker' in ws.burst_events.columns:
        be = ws.burst_events[ws.burst_events['ticker'].astype(str).str.upper() == ticker].copy()
        if not be.empty:
            st.subheader('Burst event tape')
            st.dataframe(be.tail(100), use_container_width=True, hide_index=True)

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
            st.dataframe(b.head(60), use_container_width=True, hide_index=True)


def render_microstructure() -> None:
    st.title('Intraday Microstructure')
    if ws.intraday.empty:
        st.info('No intraday feature file detected yet. Untuk sekarang fokus dulu ke EOD real via yfinance. Layer done-detail/orderbook nanti bisa diisi begitu data real ada.')
        st.code('python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01\npython -m src.pipelines.run_real_eod_smoke', language='bash')
        return
    st.subheader('Daily intraday summary')
    st.dataframe(ws.intraday, use_container_width=True, hide_index=True)
    if not ws.burst_events.empty:
        st.subheader('Burst events')
        st.dataframe(ws.burst_events, use_container_width=True, hide_index=True)


def render_broker_profiles() -> None:
    st.title('Broker Profiles')
    cov = broker_coverage_report(ws.brokers, ws.broker_master)
    left, right = st.columns(2)
    with left:
        st.subheader('Latest broker profiles')
        if ws.profiles.empty:
            st.warning('No broker profile file yet.')
        else:
            st.dataframe(ws.profiles, use_container_width=True, hide_index=True)
    with right:
        st.subheader('Broker coverage vs master')
        if cov.empty:
            st.warning('No broker master or broker summary coverage yet.')
        else:
            st.dataframe(cov, use_container_width=True, hide_index=True)


def render_data_audit() -> None:
    st.title('Data Audit')
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

    st.subheader('Fetch real EOD first')
    st.code(
        'python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01\n'
        'python -m src.pipelines.run_real_eod_smoke\n'
        'python -m src.pipelines.audit_raw_data',
        language='bash'
    )


def render_adaptive_monitor() -> None:
    st.title('Adaptive Monitor')
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


PAGES = {
    'Home': render_home,
    'Watchlist': render_watchlist,
    'Ticker Detail': render_ticker_detail,
    'Microstructure': render_microstructure,
    'Broker Profiles': render_broker_profiles,
    'Data Audit': render_data_audit,
    'Adaptive Monitor': render_adaptive_monitor,
}

st.sidebar.title('IDX Flow Engine')
page = st.sidebar.radio('Page', list(PAGES.keys()), index=0)
PAGES[page]()
