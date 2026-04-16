from __future__ import annotations

import argparse
import subprocess
import sys


def main() -> None:
    ap = argparse.ArgumentParser(description='Fetch free EOD via yfinance, then build the real EOD smoke watchlist.')
    ap.add_argument('--start', default='2025-01-01')
    ap.add_argument('--tickers-file', default='data/raw/ticker_universe_smoke.csv')
    args = ap.parse_args()

    subprocess.check_call([
        sys.executable, '-m', 'src.pipelines.fetch_real_eod_yfinance',
        '--start', args.start,
        '--tickers-file', args.tickers_file,
    ])
    subprocess.check_call([
        sys.executable, '-m', 'src.pipelines.run_real_eod_smoke'
    ])


if __name__ == '__main__':
    main()
