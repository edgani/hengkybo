from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

from src.adapters.broker_importers import load_broker_summary_csv


def main() -> None:
    ap = argparse.ArgumentParser(description='Standardize broker summary CSV into the engine schema.')
    ap.add_argument('--input', required=True, help='Path to raw broker summary CSV')
    ap.add_argument('--output', default='data/raw/broker_summary_daily_real.csv', help='Standardized output CSV')
    ap.add_argument('--format', default=None, help='Optional explicit format: generic/stockbit_like/idx_like')
    ap.add_argument('--ticker', default=None, help='Default ticker if not present in CSV')
    ap.add_argument('--date', default=None, help='Default date if not present in CSV')
    ap.add_argument('--append', action='store_true', help='Append into output instead of overwrite')
    args = ap.parse_args()

    out = load_broker_summary_csv(args.input, source_format=args.format, default_ticker=args.ticker, default_date=args.date)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.append and out_path.exists():
        old = pd.read_csv(out_path, parse_dates=['date'])
        out = pd.concat([old, out], ignore_index=True)
        out = out.drop_duplicates(subset=['date','ticker','broker_code'], keep='last').sort_values(['date','ticker','broker_code'])

    out.to_csv(out_path, index=False)
    print(f'Saved standardized broker summary -> {out_path} ({len(out):,} rows, {out["broker_code"].nunique()} brokers)')


if __name__ == '__main__':
    main()
