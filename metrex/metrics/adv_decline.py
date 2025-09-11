import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class AdvDecline(MetricProtocol):
    name = "adv_decline"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        df = market_df.copy()
        df = df.sort_values(['date','pair'])
        df['ret'] = df.groupby('pair')['close'].pct_change()
        adv = df.groupby('date')['ret'].apply(lambda x: (x > 0).sum())
        decl = df.groupby('date')['ret'].apply(lambda x: (x < 0).sum())
        res = pd.DataFrame({'date': adv.index, 'adv_count': adv.values, 'decl_count': decl.values})
        res['adv_decline_diff'] = res['adv_count'] - res['decl_count']
        res['adv_decline_line'] = res['adv_decline_diff'].cumsum()
        return res.sort_values('date')

from . import register
register(AdvDecline())
