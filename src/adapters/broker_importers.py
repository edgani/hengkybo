from __future__ import annotations

from pathlib import Path
import pandas as pd

BROKER_SUMMARY_COLUMNS = [
    'date','ticker','broker_code','buy_lot','buy_value','sell_lot','sell_value','buy_avg','sell_avg','net_lot','net_value','gross_activity_lot','gross_activity_value'
]


def _canon(name: str) -> str:
    return str(name).strip().lower().replace(' ', '_').replace('-', '_')


def detect_broker_summary_format(df: pd.DataFrame) -> str:
    cols = {_canon(c) for c in df.columns}
    if {'buy_lot','sell_lot','buy_value','sell_value','broker_code','ticker'}.issubset(cols):
        return 'generic'
    if {'broker','buy_lot','sell_lot','buy_value','sell_value','ticker'}.issubset(cols):
        return 'generic_broker_alias'
    if {'broker_code','buy_volume','sell_volume','buy_avg','sell_avg','ticker'}.issubset(cols):
        return 'stockbit_like'
    if {'broker_code','buy_lot','sell_lot','buy_value','sell_value'}.issubset(cols):
        return 'idx_like'
    return 'unknown'


def normalize_broker_summary(df: pd.DataFrame, source_format: str | None = None, default_ticker: str | None = None, default_date: str | None = None) -> pd.DataFrame:
    source_format = source_format or detect_broker_summary_format(df)
    cols = {_canon(c): c for c in df.columns}
    mapping: dict[str, str] = {}

    alias_map = {
        'date': ['date', 'trade_date'],
        'ticker': ['ticker', 'symbol', 'code'],
        'broker_code': ['broker_code', 'broker'],
        'buy_lot': ['buy_lot', 'buy_volume', 'blot', 'b_lot'],
        'buy_value': ['buy_value', 'buy_val', 'bval', 'buy_amount'],
        'sell_lot': ['sell_lot', 'sell_volume', 'slot', 's_lot'],
        'sell_value': ['sell_value', 'sell_val', 'sval', 'sell_amount'],
        'buy_avg': ['buy_avg', 'bavg', 'average_buy'],
        'sell_avg': ['sell_avg', 'savg', 'average_sell'],
    }
    for target, aliases in alias_map.items():
        for alias in aliases:
            if alias in cols:
                mapping[cols[alias]] = target
                break

    out = df.rename(columns=mapping).copy()
    if 'ticker' not in out.columns:
        if not default_ticker:
            raise ValueError('ticker column missing in broker summary and no default_ticker provided')
        out['ticker'] = default_ticker
    if 'date' not in out.columns:
        if not default_date:
            raise ValueError('date column missing in broker summary and no default_date provided')
        out['date'] = default_date

    required = ['broker_code', 'buy_lot', 'sell_lot']
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f'broker summary missing required columns: {missing}')

    if 'buy_value' not in out.columns:
        if 'buy_avg' in out.columns:
            out['buy_value'] = pd.to_numeric(out['buy_avg'], errors='coerce') * pd.to_numeric(out['buy_lot'], errors='coerce') * 100
        else:
            out['buy_value'] = 0
    if 'sell_value' not in out.columns:
        if 'sell_avg' in out.columns:
            out['sell_value'] = pd.to_numeric(out['sell_avg'], errors='coerce') * pd.to_numeric(out['sell_lot'], errors='coerce') * 100
        else:
            out['sell_value'] = 0

    out['date'] = pd.to_datetime(out['date']).dt.normalize()
    out['ticker'] = out['ticker'].astype(str).str.replace('.JK','', regex=False).str.upper()
    out['broker_code'] = out['broker_code'].astype(str).str.upper().str.strip()
    for c in ['buy_lot','buy_value','sell_lot','sell_value','buy_avg','sell_avg']:
        if c not in out.columns:
            out[c] = 0
        out[c] = pd.to_numeric(out[c], errors='coerce').fillna(0)

    out['net_lot'] = out['buy_lot'] - out['sell_lot']
    out['net_value'] = out['buy_value'] - out['sell_value']
    out['gross_activity_lot'] = out['buy_lot'] + out['sell_lot']
    out['gross_activity_value'] = out['buy_value'] + out['sell_value']

    # derive avg when missing
    zero_buy_avg = (out['buy_avg'] == 0) | (out['buy_avg'].isna())
    zero_sell_avg = (out['sell_avg'] == 0) | (out['sell_avg'].isna())
    out.loc[zero_buy_avg & (out['buy_lot'] > 0), 'buy_avg'] = out.loc[zero_buy_avg & (out['buy_lot'] > 0), 'buy_value'] / (out.loc[zero_buy_avg & (out['buy_lot'] > 0), 'buy_lot'] * 100)
    out.loc[zero_sell_avg & (out['sell_lot'] > 0), 'sell_avg'] = out.loc[zero_sell_avg & (out['sell_lot'] > 0), 'sell_value'] / (out.loc[zero_sell_avg & (out['sell_lot'] > 0), 'sell_lot'] * 100)

    return out[BROKER_SUMMARY_COLUMNS].sort_values(['date','ticker','broker_code']).reset_index(drop=True)


def load_broker_summary_csv(path: str | Path, source_format: str | None = None, default_ticker: str | None = None, default_date: str | None = None) -> pd.DataFrame:
    return normalize_broker_summary(pd.read_csv(path), source_format=source_format, default_ticker=default_ticker, default_date=default_date)
