import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class VolumeSurgeRatio(MetricProtocol):
    name = "volume_surge_ratio"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['vol_sma20'] = df.groupby('pair')['volume'].transform(lambda x: x.rolling(20, min_periods=1).mean())
        df['vol_surge'] = df['volume'] / df['vol_sma20']
        res = df.groupby('date')['vol_surge'].mean().reset_index()
        res = res.rename(columns={'vol_surge':'volume_surge_ratio'})
        return res.sort_values('date')

from . import register
register(VolumeSurgeRatio())
