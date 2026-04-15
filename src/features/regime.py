from __future__ import annotations
import numpy as np
import pandas as pd


def build_regime(prices: pd.DataFrame) -> pd.DataFrame:
    mkt = prices.pivot(index='date', columns='ticker', values='close').sort_index()
    ew = mkt.mean(axis=1)
    ret20 = ew.pct_change(20)
    vol20 = ew.pct_change().rolling(20, min_periods=5).std() * np.sqrt(252)
    regime = pd.DataFrame({'date': ew.index, 'market_ret20': ret20.values, 'market_vol20': vol20.values})
    regime['regime'] = np.where(regime['market_ret20'] > 0.04, 'BULL', np.where(regime['market_ret20'] < -0.04, 'BEAR', 'CHOP'))
    regime['macro_alignment_score'] = np.where(regime['regime'].eq('BULL'), 70, np.where(regime['regime'].eq('BEAR'), 30, 50))
    regime['macro_headwind_score'] = 100 - regime['macro_alignment_score']
    return regime
