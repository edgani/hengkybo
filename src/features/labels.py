from __future__ import annotations
import pandas as pd
import numpy as np


def label_forward_outcomes(prices: pd.DataFrame, horizon_days: int = 10, target_return: float = 0.08, max_adverse: float = -0.04) -> pd.DataFrame:
    df = prices[['date','ticker','close','low','high']].copy().sort_values(['ticker','date'])
    out=[]
    for ticker, g in df.groupby('ticker', sort=False):
        g = g.reset_index(drop=True)
        closes = g['close'].values
        highs = g['high'].values
        lows = g['low'].values
        for i in range(len(g)):
            f = slice(i+1, min(i+1+horizon_days, len(g)))
            if i+1 >= len(g):
                max_ret = np.nan; min_ret = np.nan; label = np.nan
            else:
                max_ret = highs[f].max()/closes[i] - 1
                min_ret = lows[f].min()/closes[i] - 1
                label = 1.0 if (max_ret >= target_return and min_ret >= max_adverse) else 0.0
            out.append({'date':g.loc[i,'date'],'ticker':ticker,'future_max_return':max_ret,'future_min_return':min_ret,'label_ready_long':label})
    return pd.DataFrame(out)
