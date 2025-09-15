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
        # Merge BTC returns per timestamp
        df = df.merge(btc_ret, left_on='date', right_index=True, how='left')

        # Rolling correlation per pair versus BTC, then average cross-sectionally per date
        def _corr_vs_btc(g: pd.DataFrame) -> pd.DataFrame:
            g = g.sort_values('date')
            g['corr_btc'] = g['ret'].rolling(50, min_periods=10).corr(g['btc_ret'])
            return g

        df = df.groupby('pair', group_keys=False).apply(_corr_vs_btc)
        avg_corr = df.groupby('date')['corr_btc'].mean().reset_index(name='avg_corr_btc')
        return avg_corr.sort_values('date')

from . import register
register(AvgCorrelationBTC())
