import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class MarketReturnMA(MetricProtocol):
    name = "market_return_ma"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['ret'] = df.groupby('pair')['close'].pct_change()
        mkt_ret = df.groupby('date')['ret'].mean().rename('mkt_ret')
        mkt_ret_sma20 = mkt_ret.rolling(20, min_periods=1).mean().rename('mkt_ret_sma20')
        res = pd.DataFrame({'date': mkt_ret.index, 'mkt_ret': mkt_ret.values, 'mkt_ret_sma20': mkt_ret_sma20.values})
        return res.sort_values('date')

from . import register
register(MarketReturnMA())
