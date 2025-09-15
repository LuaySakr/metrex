import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class NewHighsLows(MetricProtocol):
    name = "new_highs_lows"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['high_50'] = df.groupby('pair')['high'].transform(lambda x: x.rolling(50, min_periods=1).max())
        df['low_50'] = df.groupby('pair')['low'].transform(lambda x: x.rolling(50, min_periods=1).min())
        df['new_high_50'] = df['high'] == df['high_50']
        df['new_low_50'] = df['low'] == df['low_50']
        res = df.groupby('date').agg(new_highs_50=('new_high_50','sum'), new_lows_50=('new_low_50','sum')).reset_index()
        return res.sort_values('date')

from . import register
register(NewHighsLows())
