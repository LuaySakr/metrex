"""Build reusable context values from metrics DataFrame."""
from __future__ import annotations
import pandas as pd
from typing import Dict, Any

def build_context(df: pd.DataFrame) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    # Precompute rolling stats for commonly used metrics if present
    if 'breadth_above_sma_50' in df:
        ctx['breadth_mean'] = df['breadth_above_sma_50'].mean()
        ctx['breadth_median'] = df['breadth_above_sma_50'].median()
        ctx['breadth_std'] = df['breadth_above_sma_50'].std()
        q = df['breadth_above_sma_50'].quantile
        ctx['breadth_q25'] = q(0.25)
        ctx['breadth_q75'] = q(0.75)
        ctx['breadth_q90'] = q(0.90)
    if 'mkt_ret' in df:
        ctx['mkt_ret_mean'] = df['mkt_ret'].mean()
        ctx['mkt_ret_std'] = df['mkt_ret'].std()
        q = df['mkt_ret'].quantile
        ctx['mkt_ret_q25'] = q(0.25)
        ctx['mkt_ret_q75'] = q(0.75)
    if 'vol_zscore' in df:
        ctx['vol_mean'] = df['vol_zscore'].mean()
        ctx['vol_std'] = df['vol_zscore'].std()
        q = df['vol_zscore'].quantile
        ctx['vol_q25'] = q(0.25)
        ctx['vol_q75'] = q(0.75)
        ctx['vol_q90'] = q(0.90)
    if 'volume_surge_ratio' in df:
        q = df['volume_surge_ratio'].quantile
        ctx['volsurge_q50'] = q(0.50)
        ctx['volsurge_q75'] = q(0.75)
        ctx['volsurge_q90'] = q(0.90)
    if 'avg_corr_btc' in df:
        q = df['avg_corr_btc'].quantile
        ctx['corr_q25'] = q(0.25)
        ctx['corr_q75'] = q(0.75)
        ctx['corr_q90'] = q(0.90)
    return ctx
