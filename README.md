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
