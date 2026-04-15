# IDX Flow Engine V4

V4 is the most complete scaffold in this series. It upgrades the earlier research scaffold into a **demo-runnable pandas engine** with:

- daily broker-flow features
- dynamic broker reclassification
- broker inventory and institutional levels
- intraday tape and order-book confirmation
- rule-based scores
- walk-forward ranking model
- isotonic calibration
- verdict mapping and explanations
- dashboard starter

## What this is
A probability engine for **Ready Long / Watch / Avoid / Trim-Sell** style decision support.

## What this is not
It is not a magical tool that knows true custody or true intent of every participant.

## Run
```bash
python -m src.pipelines.run_v4_pipeline
streamlit run app/streamlit_app.py
```

## Main outputs
Inside `data/features/`:
- `feature_store_v4.csv`
- `latest_watchlist_v4.csv`
- `walk_forward_metrics.csv`
- `walk_forward_predictions.csv`
- `broker_profiles_latest.csv`
- `intraday_features_v4.csv`
- `calibration_report.csv`

## Key design notes
- synthetic-but-structured demo data is included so the package can be run immediately
- replace `data/raw/*.csv` with real feeds to move from demo to real research
- the most fragile inference remains transfer/crossing detection and true inventory estimation
