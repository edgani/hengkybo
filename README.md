# IDX Flow Engine v2

A research-first Python scaffold for an adaptive IDX broker-flow engine with **EOD + intraday microstructure** blocks.

## Scope of v2

This version adds starter implementations for:
- done-detail ingestion
- orderbook top-5 ingestion
- tape feature extraction
- orderbook imbalance / tension / spoof-risk proxies
- transfer-suspicion proxy
- intraday microstructure scoring
- combined EOD + intraday watchlist output

## What is still starter-grade

This is still a **research scaffold**, not a finished production platform.
What remains inferential / incomplete:
- true broker identity
- true custody inventory
- true hidden-order detection
- definitive crossing proof
- real-time queue update / cancel event stream logic

## Commands

Run EOD pipeline first:

```bash
python -m src.pipelines.run_eod_pipeline
```

Run intraday pipeline:

```bash
python -m src.pipelines.run_intraday_pipeline
```

Run dashboard:

```bash
streamlit run app/streamlit_app.py
```

## New outputs

Written to `data/features/`:
- `intraday_signals.parquet`
- `latest_intraday_signals.csv`
- `combined_watchlist_intraday.csv` (if EOD output exists)

## Data expectations

### done_detail_intraday
Required columns:
- `timestamp`, `trade_date`, `ticker`, `price`, `lot`, `buyer_broker`, `seller_broker`, `side_aggressor`, `trade_seq`

### orderbook_intraday
Required columns:
- `timestamp`, `trade_date`, `ticker`
- `bid_1_price` ... `bid_5_price`
- `bid_1_lot` ... `bid_5_lot`
- `offer_1_price` ... `offer_5_price`
- `offer_1_lot` ... `offer_5_lot`

## New logic blocks

### Done detail features
- buy/sell aggressor ratio
- same-second burst count
- sweep-like detection
- child-order cluster score
- top pair concentration
- tape conviction proxy

### Orderbook features
- top-3 and depth imbalance
- spread level and stability
- offer wall persistence
- bid wall persistence
- breakout tension proxy
- spoof-risk proxy

### Combined interpretation
The engine now distinguishes between:
- EOD setup is good but microstructure is weak
- EOD setup is good and microstructure confirms
- EOD setup is mediocre but tape/orderbook is improving

## Caveats

The intraday layer currently uses **snapshot inference**, not full queue-event replay.
So it is useful as a confirmation block, but still not enough to claim hidden intent as fact.
