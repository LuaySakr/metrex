import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class AvgCorrelationBTC(MetricProtocol):
    name = "avg_correlation_btc"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        btc_names = ['BTC_USDT','BTCUSDT','BTC']
        btc_df = market_df[market_df['pair'].isin(btc_names)].copy()
        btc_df = btc_df.sort_values('date')
        btc_ret = btc_df.set_index('date')['close'].pct_change().rename('btc_ret')
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['ret'] = df.groupby('pair')['close'].pct_change()
        # Merge BTC returns
        df = df.merge(btc_ret, left_on='date', right_index=True, how='left')
        def corr_func(x):
            if x['ret'].isnull().all() or x['btc_ret'].isnull().all():
                return float('nan')
            return x['ret'].rolling(50, min_periods=10).corr(x['btc_ret']).mean()
        avg_corr = df.groupby('date').apply(corr_func).reset_index(name='avg_corr_btc')
        return avg_corr.sort_values('date')

from . import register
register(AvgCorrelationBTC())
