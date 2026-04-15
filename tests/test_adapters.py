import pandas as pd

from src.adapters.eod_sources import standardize_prices_df
from src.adapters.broker_importers import normalize_broker_summary


def test_standardize_prices_df_basic():
    raw = pd.DataFrame({
        'Date': ['2026-04-15'],
        'Open': [100],
        'High': [110],
        'Low': [95],
        'Close': [105],
        'Volume': [1000],
        'Ticker': ['BBCA'],
    })
    out = standardize_prices_df(raw)
    assert list(out['ticker']) == ['BBCA']
    assert int(out.loc[0, 'volume_lots']) == 10


def test_normalize_broker_summary_basic():
    raw = pd.DataFrame({
        'date': ['2026-04-15'],
        'ticker': ['BBCA'],
        'broker': ['AI'],
        'buy_lot': [100],
        'buy_value': [91000000],
        'sell_lot': [20],
        'sell_value': [18200000],
    })
    out = normalize_broker_summary(raw)
    assert list(out['broker_code']) == ['AI']
    assert int(out.loc[0, 'net_lot']) == 80
