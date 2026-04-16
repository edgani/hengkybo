from __future__ import annotations

import argparse
from pathlib import Path

from src.providers.free_eod import fetch_yfinance, load_ticker_file


def main() -> None:
    ap = argparse.ArgumentParser(description='Fetch real IDX EOD data via yfinance for a smoke test.')
    ap.add_argument('--tickers-file', default='data/raw/ticker_universe_smoke.csv', help='CSV with a `ticker` column.')
    ap.add_argument('--start', default='2025-01-01', help='Start date YYYY-MM-DD')
    ap.add_argument('--end', default=None, help='Optional end date YYYY-MM-DD')
    ap.add_argument('--out', default='data/raw/prices_daily_real.csv', help='Output CSV path')
    args = ap.parse_args()

    tickers = load_ticker_file(args.tickers_file)
    out, misses = fetch_yfinance(tickers=tickers, start=args.start, end=args.end)
    if out.empty:
        raise SystemExit('Tidak ada data yang berhasil diunduh dari yfinance.')

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
