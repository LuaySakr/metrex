import pandas as pd
from typing import Dict, Any
from .base import MetricProtocol

class MarketVolRegime(MetricProtocol):
    name = "market_vol_regime"
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        btc_names = ['BTC_USDT','BTCUSDT','BTC']
        btc_df = market_df[market_df['pair'].isin(btc_names)].copy()
        btc_df = btc_df.sort_values('date')
        btc_df['returns'] = btc_df['close'].pct_change()
        btc_df['vol'] = btc_df['returns'].rolling(20, min_periods=1).std()
        # Percentile regime
        vol = btc_df['vol'].dropna()
        p33, p67 = vol.quantile([0.33,0.67])
        def regime(v):
            if v < p33: return 'low'
            elif v < p67: return 'medium'
            else: return 'high'
        btc_df['vol_regime'] = btc_df['vol'].apply(regime)
        btc_df['vol_zscore'] = (btc_df['vol'] - vol.mean()) / vol.std()
        return btc_df[['date','vol_regime','vol_zscore']].rename(columns={'vol_regime':'market_vol_regime'}).sort_values('date')

from . import register
register(MarketVolRegime())
