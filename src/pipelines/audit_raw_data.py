from pathlib import Path
import pandas as pd

RAW = Path(__file__).resolve().parents[2] / 'data' / 'raw'

def main():
    prices = pd.read_csv(RAW / 'prices_daily.csv')
    brokers = pd.read_csv(RAW / 'broker_summary_daily.csv')
    print('Tickers:', sorted(prices['ticker'].unique().tolist()))
    print('Ticker count:', prices['ticker'].nunique())
    print('Broker codes:', sorted(brokers['broker_code'].unique().tolist()))
    print('Broker count:', brokers['broker_code'].nunique())

if __name__ == '__main__':
    main()
