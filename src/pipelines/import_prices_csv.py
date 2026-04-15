from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.adapters.eod_sources import load_prices_csv


def main() -> None:
    ap = argparse.ArgumentParser(description='Standardize a real EOD prices CSV into the engine schema.')
    ap.add_argument('--input', required=True, help='Path to raw prices CSV')
    ap.add_argument('--output', default='data/raw/prices_daily_real.csv', help='Standardized output CSV')
    ap.add_argument('--ticker', default=None, help='Override ticker if input file contains only one ticker and no ticker column')
    ap.add_argument('--append', action='store_true', help='Append into output instead of overwrite')
    args = ap.parse_args()

    out = load_prices_csv(args.input, ticker=args.ticker)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.append and out_path.exists():
        old = pd.read_csv(out_path, parse_dates=['date'])
        out = pd.concat([old, out], ignore_index=True)
        out = out.drop_duplicates(subset=['date','ticker'], keep='last').sort_values(['ticker','date'])

    out.to_csv(out_path, index=False)
    print(f'Saved standardized prices -> {out_path} ({len(out):,} rows, {out["ticker"].nunique()} tickers)')


if __name__ == '__main__':
    main()
