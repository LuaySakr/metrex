import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class BTCTrendSlope(MetricProtocol):
    name = "btc_trend_slope"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        import numpy as np
        btc_names = ['BTC_USDT','BTCUSDT','BTC']
        btc_df = market_df[market_df['pair'].isin(btc_names)].copy()
        btc_df = btc_df.sort_values('date')
        def rolling_slope(series, window=20):
            arr = series.values
            slopes = np.full(arr.shape, np.nan)
            for i in range(window-1, len(arr)):
                y = arr[i-window+1:i+1]
                x = np.arange(window)
                if np.any(np.isnan(y)):
                    continue
                A = np.vstack([x, np.ones(window)]).T
                m, _ = np.linalg.lstsq(A, y, rcond=None)[0]
                slopes[i] = m
            return pd.Series(slopes, index=series.index)
        btc_df['btc_trend_slope'] = rolling_slope(btc_df['close'], window=20)
        return btc_df[['date','btc_trend_slope']].sort_values('date')

from . import register
register(BTCTrendSlope())
