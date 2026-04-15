from __future__ import annotations

from pathlib import Path
import pandas as pd

PRICE_COLUMNS = [
    'date','ticker','open','high','low','close','adj_close','volume_shares','volume_lots','turnover_value',
    'sector','industry','board','free_float_shares','market_cap','is_suspended','corporate_action_flag'
]


def _lower_map(columns: list[str]) -> dict[str, str]:
    return {str(c).strip().lower().replace(' ', '_'): str(c) for c in columns}


def standardize_prices_df(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    cols = _lower_map(list(df.columns))
    mapping: dict[str, str] = {}

    aliases = {
        'date': ['date', 'datetime', 'timestamp'],
        'open': ['open', 'open_price'],
        'high': ['high', 'high_price'],
        'low': ['low', 'low_price'],
        'close': ['close', 'close_price', 'last', 'last_price'],
        'adj_close': ['adj_close', 'adjusted_close', 'adjclose'],
        'volume_shares': ['volume_shares', 'volume', 'shares_traded', 'vol'],
        'ticker': ['ticker', 'symbol', 'code'],
    }
    for target, names in aliases.items():
        for name in names:
            if name in cols:
                mapping[cols[name]] = target
                break

    out = df.rename(columns=mapping).copy()
    missing = [c for c in ['date', 'open', 'high', 'low', 'close'] if c not in out.columns]
    if missing:
        raise ValueError(f'prices CSV missing required columns: {missing}')

    if 'ticker' not in out.columns:
        if not ticker:
            raise ValueError('ticker column missing and no --ticker override provided')
        out['ticker'] = ticker
    if 'adj_close' not in out.columns:
        out['adj_close'] = out['close']
    if 'volume_shares' not in out.columns:
        out['volume_shares'] = 0

    out['date'] = pd.to_datetime(out['date']).dt.normalize()
    out['ticker'] = out['ticker'].astype(str).str.replace('.JK','', regex=False).str.upper()
    for c in ['open','high','low','close','adj_close','volume_shares']:
        out[c] = pd.to_numeric(out[c], errors='coerce')
    out['volume_shares'] = out['volume_shares'].fillna(0).astype('int64')
    out['volume_lots'] = (out['volume_shares'] // 100).astype('int64')
    out['turnover_value'] = pd.to_numeric(out.get('turnover_value', pd.Series(index=out.index, dtype='float64')), errors='coerce')
    out['turnover_value'] = out['turnover_value'].fillna(out['close'] * out['volume_shares'])

    defaults = {
        'sector': pd.NA, 'industry': pd.NA, 'board': pd.NA, 'free_float_shares': pd.NA, 'market_cap': pd.NA,
        'is_suspended': False, 'corporate_action_flag': pd.NA
    }
    for col, val in defaults.items():
        if col not in out.columns:
            out[col] = val

    return out[PRICE_COLUMNS].sort_values(['ticker','date']).reset_index(drop=True)


def load_prices_csv(path: str | Path, ticker: str | None = None) -> pd.DataFrame:
    return standardize_prices_df(pd.read_csv(path), ticker=ticker)


def fetch_yfinance_prices(ticker: str, start: str, end: str | None = None) -> pd.DataFrame:
    try:
        import yfinance as yf
    except Exception as exc:
        raise SystemExit('yfinance belum terpasang.') from exc

    tk = f'{ticker}.JK' if not ticker.endswith('.JK') else ticker
    hist = yf.download(tk, start=start, end=end, auto_adjust=False, progress=False, threads=False)
    if hist.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]
    hist = hist.reset_index()
    return standardize_prices_df(hist, ticker=ticker.replace('.JK',''))
