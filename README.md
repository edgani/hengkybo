# IDX Flow Engine v1

A research-first Python scaffold for an adaptive IDX broker-flow watchlist engine.

## Scope of v1

This version is intentionally **EOD-first**:
- Daily price/volume ingestion
- Daily broker summary ingestion
- Daily foreign flow ingestion
- Broker inventory / cost-basis estimates with decay
- Phase / subphase detection
- Dry vs wet supply-pressure proxy
- Distribution risk / breakout integrity / accumulation quality scoring
- Final watchlist verdicts: `READY_LONG`, `WATCH`, `AVOID`, `TRIM_SELL`, `NEUTRAL`

What it does **not** do yet:
- Live bid-offer / top-5 depth interpretation
- Done-detail clustering
- Crossing graph / transfer suspicion from tape
- Live microstructure confirmation

Those belong in v2+.

## Design principles

1. **Probabilistic, not mystical**
   - No hard claim about “bandar identity” or hidden intent.
   - Broker labels are behavior-based and should decay over time.
2. **Modular**
   - Inventory, phase, dry/wet, scoring, and validation are separate.
3. **Adaptive**
   - Regime-aware weights are configurable.
4. **Testable**
   - Every verdict should be traceable to features and thresholds.

## Suggested workflow

1. Put raw CSV/Parquet files into `data/raw/`
2. Configure source paths in `config/data_sources.yaml`
3. Run:

```bash
python -m src.pipelines.run_eod_pipeline
```

4. Start dashboard:

```bash
streamlit run app/streamlit_app.py
```

## Raw data expectations

### prices_daily
Required columns:
- `date`, `ticker`, `open`, `high`, `low`, `close`, `volume_shares`
Optional:
- `turnover_value`, `free_float_shares`, `sector`, `industry`, `board`

### broker_summary_daily
Required columns:
- `date`, `ticker`, `broker_code`, `buy_lot`, `buy_value`, `sell_lot`, `sell_value`

### foreign_daily
Required columns:
- `date`, `ticker`, `foreign_buy_lot`, `foreign_sell_lot`
Optional:
- `foreign_ownership_pct`, `foreign_buy_value`, `foreign_sell_value`

## Project layout

- `src/features/` feature builders
- `src/analytics/` higher-level research logic
- `src/scoring/` score aggregation and verdict mapping
- `src/models/` labeling and walk-forward validation tools
- `src/pipelines/` batch jobs
- `app/` Streamlit UI
- `db/` schema and views
- `config/` YAML config

## Important caveats

- v1 estimates inventory using **decayed broker flow**, not true custody data.
- Institutional support/resistance are **inference bands**, not exact hidden inventory.
- Without intraday raw feed, breakout integrity is still incomplete.
- Before trusting the watchlist, run walk-forward validation per regime, sector, and liquidity bucket.
