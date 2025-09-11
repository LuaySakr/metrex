import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class BreadthSMA50(MetricProtocol):
    name = "breadth_sma50"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        # For each date, % of pairs with close > SMA50
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['sma50'] = df.groupby('pair')['close'].transform(lambda x: x.rolling(50, min_periods=1).mean())
        df['above_sma50'] = df['close'] > df['sma50']
        res = df.groupby('date')['above_sma50'].mean().reset_index()
        res['breadth_above_sma_50'] = res['above_sma50'] * 100
        return res[['date','breadth_above_sma_50']].sort_values('date')

# Register
from . import register
register(BreadthSMA50())
