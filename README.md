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
