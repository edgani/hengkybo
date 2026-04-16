from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / 'data'
RAW = DATA / 'raw'
FEATURES = DATA / 'features'


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def choose_raw_file(real_name: str, demo_name: str) -> tuple[Path, str]:
    real_path = RAW / real_name
    demo_path = RAW / demo_name
    if real_path.exists() and real_path.stat().st_size > 0:
        return real_path, 'REAL'
    return demo_path, 'DEMO'


@dataclass
class WorkspaceSnapshot:
    prices_path: Path
    prices_mode: str
    brokers_path: Path
    brokers_mode: str
    prices: pd.DataFrame
    brokers: pd.DataFrame
    watch_path: Path
    watch_mode: str
    watch: pd.DataFrame
    metrics: pd.DataFrame
    profiles: pd.DataFrame
    intraday: pd.DataFrame
    burst_events: pd.DataFrame
    calibration: pd.DataFrame
    regime: pd.DataFrame
    drift: pd.DataFrame
    broker_master: pd.DataFrame


def load_workspace() -> WorkspaceSnapshot:
    prices_path, prices_mode = choose_raw_file('prices_daily_real.csv', 'prices_daily.csv')
    brokers_path, brokers_mode = choose_raw_file('broker_summary_daily_real.csv', 'broker_summary_daily.csv')

    watch_real = FEATURES / 'latest_watchlist_real_eod.csv'
    watch_demo = FEATURES / 'latest_watchlist_v4.csv'
    if watch_real.exists() and watch_real.stat().st_size > 0:
        watch_path = watch_real
        watch_mode = 'REAL_EOD_SMOKE'
    else:
        watch_path = watch_demo
        watch_mode = 'DEMO_V4'

    return WorkspaceSnapshot(
        prices_path=prices_path,
        prices_mode=prices_mode,
        brokers_path=brokers_path,
        brokers_mode=brokers_mode,
        prices=read_csv_if_exists(prices_path),
        brokers=read_csv_if_exists(brokers_path),
        watch_path=watch_path,
        watch_mode=watch_mode,
        watch=read_csv_if_exists(watch_path),
        metrics=read_csv_if_exists(FEATURES / 'walk_forward_metrics.csv'),
        profiles=read_csv_if_exists(FEATURES / 'broker_profiles_latest.csv'),
        intraday=read_csv_if_exists(FEATURES / 'intraday_features_v4.csv'),
        burst_events=read_csv_if_exists(FEATURES / 'burst_events_v47.csv'),
        calibration=read_csv_if_exists(FEATURES / 'calibration_report.csv'),
        regime=read_csv_if_exists(FEATURES / 'regime_daily.csv'),
        drift=read_csv_if_exists(FEATURES / 'drift_report.csv'),
        broker_master=read_csv_if_exists(RAW / 'broker_master.csv'),
    )


def summarize_columns(df: pd.DataFrame) -> list[str]:
    return [str(c) for c in df.columns.tolist()]


def active_tickers(prices: pd.DataFrame) -> list[str]:
    if 'ticker' not in prices.columns:
        return []
    vals = sorted(prices['ticker'].dropna().astype(str).str.upper().unique().tolist())
    return vals


def active_brokers(brokers: pd.DataFrame) -> list[str]:
    if 'broker_code' not in brokers.columns:
        return []
    vals = sorted(brokers['broker_code'].dropna().astype(str).str.upper().unique().tolist())
    return vals


def broker_coverage_report(brokers: pd.DataFrame, broker_master: pd.DataFrame) -> pd.DataFrame:
    if brokers.empty or 'broker_code' not in brokers.columns:
        return pd.DataFrame()
    seen = pd.DataFrame({'broker_code': active_brokers(brokers)})
    seen['loaded'] = True
    if broker_master.empty or 'broker_code' not in broker_master.columns:
        return seen
    master = broker_master.copy()
    master['broker_code'] = master['broker_code'].astype(str).str.upper().str.strip()
    if 'broker_name' not in master.columns:
        if 'name' in master.columns:
            master['broker_name'] = master['name']
        else:
            master['broker_name'] = ''
    out = master[['broker_code', 'broker_name']].drop_duplicates().merge(seen, how='left', on='broker_code')
    out['loaded'] = out['loaded'].eq(True)
    return out.sort_values(['loaded','broker_code'], ascending=[False, True]).reset_index(drop=True)


def data_quality_report(prices: pd.DataFrame, brokers: pd.DataFrame, broker_master: pd.DataFrame) -> dict:
    tickers = active_tickers(prices)
    broker_codes = active_brokers(brokers)
    coverage_df = broker_coverage_report(brokers, broker_master)
    loaded_master = int(coverage_df['loaded'].sum()) if not coverage_df.empty and 'loaded' in coverage_df.columns else len(broker_codes)
    total_master = int(len(coverage_df)) if not coverage_df.empty else len(broker_codes)
    return {
        'ticker_count': len(tickers),
        'broker_count': len(broker_codes),
        'broker_master_count': total_master,
        'broker_master_loaded_count': loaded_master,
        'price_rows': int(len(prices)),
        'broker_rows': int(len(brokers)),
        'prices_min_date': str(pd.to_datetime(prices['date']).min().date()) if not prices.empty and 'date' in prices.columns else '',
        'prices_max_date': str(pd.to_datetime(prices['date']).max().date()) if not prices.empty and 'date' in prices.columns else '',
        'brokers_min_date': str(pd.to_datetime(brokers['date']).min().date()) if not brokers.empty and 'date' in brokers.columns else '',
        'brokers_max_date': str(pd.to_datetime(brokers['date']).max().date()) if not brokers.empty and 'date' in brokers.columns else '',
        'tickers_preview': tickers[:40],
        'brokers_preview': broker_codes[:60],
    }
