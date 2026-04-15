from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().replace(' ', '_'): c for c in df.columns}
    mapping = {}
    for target in ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']:
        for k, orig in cols.items():
            if k == target:
                mapping[orig] = target
    out = df.rename(columns=mapping).copy()
    if 'adj_close' not in out.columns and 'close' in out.columns:
        out['adj_close'] = out['close']
    if 'volume' not in out.columns:
        out['volume'] = 0
    return out


def fetch_one(symbol: str, start: str, end: str | None) -> pd.DataFrame:
    try:
        import yfinance as yf
    except Exception as exc:
        raise SystemExit('yfinance belum terpasang. Jalankan `pip install yfinance` atau install requirements baru.') from exc

    tk = f'{symbol}.JK' if not symbol.endswith('.JK') else symbol
    hist = yf.download(tk, start=start, end=end, auto_adjust=False, progress=False, threads=False)
    if hist.empty:
        return pd.DataFrame(columns=['date','ticker','open','high','low','close','adj_close','volume_shares','volume_lots','turnover_value'])
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = [c[0] if isinstance(c, tuple) else c for c in hist.columns]
    hist = hist.reset_index()
    hist = _normalize_columns(hist)
    out = pd.DataFrame({
        'date': pd.to_datetime(hist['date']).dt.normalize(),
        'ticker': symbol.replace('.JK','').upper(),
        'open': pd.to_numeric(hist['open'], errors='coerce'),
        'high': pd.to_numeric(hist['high'], errors='coerce'),
        'low': pd.to_numeric(hist['low'], errors='coerce'),
        'close': pd.to_numeric(hist['close'], errors='coerce'),
        'adj_close': pd.to_numeric(hist['adj_close'], errors='coerce'),
        'volume_shares': pd.to_numeric(hist['volume'], errors='coerce').fillna(0).astype('int64'),
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


def main() -> None:
    ap = argparse.ArgumentParser(description='Fetch real IDX EOD data via yfinance for a smoke test.')
    ap.add_argument('--tickers-file', default='data/raw/ticker_universe_smoke.csv', help='CSV with a `ticker` column.')
    ap.add_argument('--start', default='2025-01-01', help='Start date YYYY-MM-DD')
    ap.add_argument('--end', default=None, help='Optional end date YYYY-MM-DD')
    ap.add_argument('--out', default='data/raw/prices_daily_real.csv', help='Output CSV path')
    args = ap.parse_args()

    tickers_path = Path(args.tickers_file)
    if not tickers_path.exists():
        raise SystemExit(f'Ticker file tidak ditemukan: {tickers_path}')
    tickers_df = pd.read_csv(tickers_path)
    if 'ticker' not in tickers_df.columns:
        raise SystemExit('Ticker file harus punya kolom `ticker`.')

    frames = []
    misses = []
    for sym in tickers_df['ticker'].dropna().astype(str).str.upper().tolist():
        df = fetch_one(sym, args.start, args.end)
        if df.empty:
            misses.append(sym)
        else:
            frames.append(df)

    if not frames:
        raise SystemExit('Tidak ada data yang berhasil diunduh.')

    out = pd.concat(frames, ignore_index=True).sort_values(['ticker', 'date'])
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f'Saved {len(out):,} rows for {out["ticker"].nunique()} tickers -> {out_path}')
    if misses:
        print('Missed tickers:', ', '.join(misses))
    latest = out['date'].max()
    print('Latest date in file:', latest.date())
    snap = out[out['date'].eq(latest)][['ticker','close','volume_shares']].sort_values('ticker')
    print(snap.to_string(index=False))


if __name__ == '__main__':
    main()
