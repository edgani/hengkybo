from pathlib import Path
import pandas as pd


def test_outputs_exist():
    base = Path('data/features')
    assert (base / 'latest_watchlist_v4.csv').exists()
    assert (base / 'walk_forward_metrics.csv').exists()


def test_watchlist_has_columns():
    df = pd.read_csv('data/features/latest_watchlist_v4.csv')
    required = {'ticker','verdict','calibrated_prob','accumulation_quality_score','distribution_risk_score'}
    assert required.issubset(df.columns)
