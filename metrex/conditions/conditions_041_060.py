from __future__ import annotations
import pandas as pd
from .base import Condition, register

# Utility

def _s(df):
    return pd.Series(False, index=df.index)

# Momentum / slope approximations using diffs
# 41 Breadth 3-bar momentum > 0
register(Condition(41, 'breadth_mom3_pos', lambda df, ctx: (df['breadth_above_sma_50'].diff(3) > 0) if 'breadth_above_sma_50' in df else _s(df)))
# 42 Breadth 3-bar momentum < 0
register(Condition(42, 'breadth_mom3_neg', lambda df, ctx: (df['breadth_above_sma_50'].diff(3) < 0) if 'breadth_above_sma_50' in df else _s(df)))
# 43 BTC slope rising (slope diff > 0)
register(Condition(43, 'btc_slope_rising', lambda df, ctx: (df['btc_trend_slope'].diff() > 0) if 'btc_trend_slope' in df else _s(df)))
# 44 BTC slope falling
register(Condition(44, 'btc_slope_falling', lambda df, ctx: (df['btc_trend_slope'].diff() < 0) if 'btc_trend_slope' in df else _s(df)))
# 45 Vol zscore > q90
register(Condition(45, 'vol_z_gt_q90', lambda df, ctx: (df['vol_zscore'] > ctx.get('vol_q90', 1e9)) if 'vol_zscore' in df else _s(df)))
# 46 Vol zscore < q25
register(Condition(46, 'vol_z_lt_q25', lambda df, ctx: (df['vol_zscore'] < ctx.get('vol_q25', -1e9)) if 'vol_zscore' in df else _s(df)))
# 47 Market return > q75
register(Condition(47, 'mkt_ret_gt_q75', lambda df, ctx: (df['mkt_ret'] > ctx.get('mkt_ret_q75', 1e9)) if 'mkt_ret' in df else _s(df)))
# 48 Market return < q25
register(Condition(48, 'mkt_ret_lt_q25', lambda df, ctx: (df['mkt_ret'] < ctx.get('mkt_ret_q25', -1e9)) if 'mkt_ret' in df else _s(df)))
# 49 Market return volatility spike: |ret| > mean + 2*std
register(Condition(49, 'mkt_ret_spike', lambda df, ctx: (df['mkt_ret'].abs() > (ctx.get('mkt_ret_mean', 0) + 2*ctx.get('mkt_ret_std', 1))) if 'mkt_ret' in df else _s(df)))
# 50 Market return squeeze: |ret| < std/2
register(Condition(50, 'mkt_ret_squeeze', lambda df, ctx: (df['mkt_ret'].abs() < (0.5*ctx.get('mkt_ret_std', 1))) if 'mkt_ret' in df else _s(df)))
# 51 High breadth + low vol regime
register(Condition(51, 'breadth_high_low_vol', lambda df, ctx: ((df['breadth_above_sma_50'] > ctx.get('breadth_q75', 1e9)) & (df['market_vol_regime'] == 'low')) if 'breadth_above_sma_50' in df and 'market_vol_regime' in df else _s(df)))
# 52 Low breadth + high vol regime
register(Condition(52, 'breadth_low_high_vol', lambda df, ctx: ((df['breadth_above_sma_50'] < ctx.get('breadth_q25', -1e9)) & (df['market_vol_regime'] == 'high')) if 'breadth_above_sma_50' in df and 'market_vol_regime' in df else _s(df)))
# 53 Correlation expansion: avg_corr_btc rising 5 bars
register(Condition(53, 'corr_expansion', lambda df, ctx: (df['avg_corr_btc'].diff(5) > 0) if 'avg_corr_btc' in df else _s(df)))
# 54 Correlation contraction: avg_corr_btc falling 5 bars
register(Condition(54, 'corr_contraction', lambda df, ctx: (df['avg_corr_btc'].diff(5) < 0) if 'avg_corr_btc' in df else _s(df)))
# 55 Breadth thrust: breadth > q75 and momentum positive
register(Condition(55, 'breadth_thrust', lambda df, ctx: ((df['breadth_above_sma_50'] > ctx.get('breadth_q75', 1e9)) & (df['breadth_above_sma_50'].diff() > 0)) if 'breadth_above_sma_50' in df else _s(df)))
# 56 Breadth collapse: breadth < q25 and momentum negative
register(Condition(56, 'breadth_collapse', lambda df, ctx: ((df['breadth_above_sma_50'] < ctx.get('breadth_q25', -1e9)) & (df['breadth_above_sma_50'].diff() < 0)) if 'breadth_above_sma_50' in df else _s(df)))
# 57 Internals strengthening: adv_decline_line rising 3 bars
register(Condition(57, 'adv_line_strengthening', lambda df, ctx: (df['adv_decline_line'].diff().rolling(3).sum() > 0) if 'adv_decline_line' in df else _s(df)))
# 58 Internals weakening: adv_decline_line falling 3 bars
register(Condition(58, 'adv_line_weakening', lambda df, ctx: (df['adv_decline_line'].diff().rolling(3).sum() < 0) if 'adv_decline_line' in df else _s(df)))
# 59 Rotation + volatility compression: corr mid band & vol regime low
register(Condition(59, 'rotation_low_vol', lambda df, ctx: (((df['avg_corr_btc'] >= ctx.get('corr_q25', -1e9)) & (df['avg_corr_btc'] <= ctx.get('corr_q75', 1e9))) & (df['market_vol_regime'] == 'low')) if 'avg_corr_btc' in df and 'market_vol_regime' in df else _s(df)))
# 60 Rotation + volatility expansion: corr mid band & vol regime high
register(Condition(60, 'rotation_high_vol', lambda df, ctx: (((df['avg_corr_btc'] >= ctx.get('corr_q25', -1e9)) & (df['avg_corr_btc'] <= ctx.get('corr_q75', 1e9))) & (df['market_vol_regime'] == 'high')) if 'avg_corr_btc' in df and 'market_vol_regime' in df else _s(df)))
