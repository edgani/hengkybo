# IDX Flow Engine V4.1 (Clarified Demo)

This package is a **demo-runnable research scaffold**. It is **not** preloaded with real IDX market data.

## Important truth
- The included tickers (`AAA`..`HHH`) are **synthetic demo placeholders**.
- The included broker codes (`AI`, `BK`, `CC`, `KK`, `PD`, `RX`, `XC`, `XL`, `YP`, `YU`) are **sample broker codes only**, not a complete broker universe.
- Any watchlist, broker profile, score, regime, or verdict generated from the included CSV files is **demo output only**.

## What to replace for real use
Replace files inside `data/raw/` with your real feeds:
- `prices_daily.csv`
- `broker_summary_daily.csv`
- `foreign_daily.csv`
- `done_detail_intraday.csv`
- `orderbook_intraday.csv`
- `broker_master.csv` (optional but recommended)

## Run
```bash
python -m src.pipelines.run_v4_pipeline
streamlit run app/streamlit_app.py
```


## Real EOD smoke test (quickest path)
This package now includes a **real EOD smoke-test path** using `yfinance` as the fastest adapter for `.JK` tickers.

### Files added
- `src/pipelines/fetch_real_eod_yfinance.py`
- `src/pipelines/run_real_eod_smoke.py`
- `data/raw/ticker_universe_smoke.csv`

### Quick run
```bash
pip install -r requirements.txt
python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01
python -m src.pipelines.run_real_eod_smoke
```

### What this does
- Downloads **real daily OHLCV** for the starter ticker universe in `ticker_universe_smoke.csv`
- Writes `data/raw/prices_daily_real.csv`
- Builds an **EOD-only** smoke-test watchlist at `data/features/latest_watchlist_real_eod.csv`

### Important limits
- This smoke test is **price-side only**.
- It does **not** infer broker accumulation, done-detail, order-book, or crossing yet.
- For the full broker-flow engine, you still need real `broker_summary_daily.csv`, `done_detail_intraday.csv`, and `orderbook_intraday.csv`.


## Real-data adapters (v4.4)

This build now includes adapter pipelines so you can replace demo CSVs with real files without rewriting the schema.

### 1) Bootstrap template files
```bash
python -m src.pipelines.bootstrap_real_workspace
```

### 2) Import a real EOD prices CSV into engine schema
```bash
python -m src.pipelines.import_prices_csv --input /path/to/your_prices.csv --output data/raw/prices_daily_real.csv
```
If your source file has no ticker column and contains only one ticker, add `--ticker BBCA`.

### 3) Import a real broker summary CSV into engine schema
```bash
python -m src.pipelines.import_broker_summary_csv --input /path/to/your_broker_summary.csv --output data/raw/broker_summary_daily_real.csv
```
You can optionally pass `--format stockbit_like` or `--format idx_like`, plus `--ticker` / `--date` when the source file omits them.

### 4) Fetch a quick real EOD smoke test from Yahoo Finance
```bash
python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01
python -m src.pipelines.run_real_eod_smoke --prices data/raw/prices_daily_real.csv
```

### Notes
- `prices_daily.csv` and `broker_summary_daily.csv` in `data/raw/` are still demo files unless you replace them.
- `prices_daily_real.csv` and `broker_summary_daily_real.csv` are the intended real-data paths for smoke tests.
- The app will show which dataset is active and how many real tickers / brokers were loaded.


## V4.5 additions
- Multipage Streamlit app with working pages:
  - Watchlist
  - Ticker Detail
  - Intraday Microstructure
  - Broker Profiles
  - Data Audit
  - Adaptive Monitor
- Better workspace detection for REAL vs DEMO data
- `python -m src.pipelines.audit_raw_data` now writes:
  - `data/features/raw_data_audit.json`
  - `data/features/broker_coverage_report.csv`
- New helper: `python -m src.pipelines.rebuild_workspace`
- New optional fetcher for broker master from official IDX member pages:
  - `python -m src.pipelines.fetch_idx_broker_master`
