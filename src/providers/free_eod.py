from __future__ import annotations

import time
from pathlib import Path
import pandas as pd


def normalize_yf_history(symbol: str, hist: pd.DataFrame) -> pd.DataFrame:
    if hist.empty:
        return pd.DataFrame(columns=['date','ticker','open','high','low','close','adj_close','volume_shares','volume_lots','turnover_value'])
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]
    hist = hist.reset_index()
    cols = {str(c).lower().replace(' ', '_'): c for c in hist.columns}
    date_col = cols.get('date') or cols.get('datetime')
    if not date_col:
        raise ValueError('History frame missing date column.')
    out = pd.DataFrame({
        'date': pd.to_datetime(hist[date_col]).dt.normalize(),
        'ticker': symbol.replace('.JK', '').upper(),
        'open': pd.to_numeric(hist[cols.get('open')], errors='coerce'),
        'high': pd.to_numeric(hist[cols.get('high')], errors='coerce'),
        'low': pd.to_numeric(hist[cols.get('low')], errors='coerce'),
        'close': pd.to_numeric(hist[cols.get('close')], errors='coerce'),
        'adj_close': pd.to_numeric(hist[cols.get('adj_close', cols.get('adj_close**', cols.get('adj_close*', cols.get('adj_close_'))))] if cols.get('adj_close', cols.get('adj_close**', cols.get('adj_close*', cols.get('adj_close_')))) else hist[cols.get('close')], errors='coerce'),
        'volume_shares': pd.to_numeric(hist[cols.get('volume')], errors='coerce').fillna(0).astype('int64'),
    })
    out['volume_lots'] = (out['volume_shares'] // 100).astype('int64')
    out['turnover_value'] = out['close'] * out['volume_shares']
    out['sector'] = pd.NA
    out['industry'] = pd.NA
    out['board'] = pd.NA
    out['free_float_shares'] = pd.NA
    out['market_cap'] = pd.NA
    out['is_suspended'] = False
    out['corporate_action_flag'] = pd.NA
    return out


def fetch_yfinance(tickers: list[str], start: str, end: str | None = None, pause: float = 0.25) -> tuple[pd.DataFrame, list[str]]:
    import yfinance as yf

    frames: list[pd.DataFrame] = []
    misses: list[str] = []
    for sym in tickers:
        tk = f'{sym}.JK' if not str(sym).upper().endswith('.JK') else str(sym).upper()
        try:
            hist = yf.download(tk, start=start, end=end, auto_adjust=False, progress=False, threads=False)
            out = normalize_yf_history(tk, hist)
            if out.empty:
                misses.append(sym)
            else:
                frames.append(out)
        except Exception:
            misses.append(sym)
        time.sleep(pause)
    return (pd.concat(frames, ignore_index=True).sort_values(['ticker', 'date']) if frames else pd.DataFrame(), misses)


def load_ticker_file(path: str | Path) -> list[str]:
    p = Path(path)
    df = pd.read_csv(p)
    if 'ticker' not in df.columns:
        raise SystemExit('Ticker file harus punya kolom `ticker`.')
    return df['ticker'].dropna().astype(str).str.replace('.JK', '', regex=False).str.upper().tolist()
