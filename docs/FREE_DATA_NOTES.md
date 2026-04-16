# Free / low-friction data path

## EOD first
Use `yfinance` for `.JK` tickers as the fastest free smoke-test path.

```bash
python -m src.pipelines.fetch_real_eod_yfinance --start 2025-01-01
python -m src.pipelines.run_real_eod_smoke
streamlit run app/streamlit_app.py
```

## Why this path exists
The app was changed to a single-file Streamlit app to avoid multipage navigation issues in some deployments.

## Next step after EOD works
Import real broker summary CSV into `data/raw/broker_summary_daily_real.csv`.

## Important
Free Yahoo EOD is good for smoke tests and prototyping, not for claiming exchange-grade truth.
