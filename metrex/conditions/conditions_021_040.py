from __future__ import annotations
import pandas as pd
from .base import Condition, register

_DEF = lambda df: pd.Series(False, index=df.index)

# 21 Breadth > 75th percentile
register(Condition(21, 'breadth_gt_q75', lambda df, ctx: (df['breadth_above_sma_50'] > ctx.get('breadth_q75', 1e9)) if 'breadth_above_sma_50' in df else _DEF(df)))
# 22 Breadth < 25th percentile
register(Condition(22, 'breadth_lt_q25', lambda df, ctx: (df['breadth_above_sma_50'] < ctx.get('breadth_q25', -1e9)) if 'breadth_above_sma_50' in df else _DEF(df)))
# 23 Breadth IQR band (between q25 and q75)
register(Condition(23, 'breadth_iqr_band', lambda df, ctx: ((df['breadth_above_sma_50'] >= ctx.get('breadth_q25', -1e9)) & (df['breadth_above_sma_50'] <= ctx.get('breadth_q75', 1e9))) if 'breadth_above_sma_50' in df else _DEF(df)))
# 24 High volatility regime + negative market return
register(Condition(24, 'high_vol_neg_ret', lambda df, ctx: ((df['market_vol_regime'] == 'high') & (df['mkt_ret'] < 0)) if 'market_vol_regime' in df and 'mkt_ret' in df else _DEF(df)))
# 25 Low volatility + positive market return
register(Condition(25, 'low_vol_pos_ret', lambda df, ctx: ((df['market_vol_regime'] == 'low') & (df['mkt_ret'] > 0)) if 'market_vol_regime' in df and 'mkt_ret' in df else _DEF(df)))
# 26 Volume surge extreme > q90
register(Condition(26, 'vol_surge_gt_q90', lambda df, ctx: (df['volume_surge_ratio'] > ctx.get('volsurge_q90', 1e9)) if 'volume_surge_ratio' in df else _DEF(df)))
# 27 Volume surge high band > q75
register(Condition(27, 'vol_surge_gt_q75', lambda df, ctx: (df['volume_surge_ratio'] > ctx.get('volsurge_q75', 1e9)) if 'volume_surge_ratio' in df else _DEF(df)))
# 28 Volume consolidation < median
register(Condition(28, 'vol_surge_lt_median', lambda df, ctx: (df['volume_surge_ratio'] < ctx.get('volsurge_q50', -1e9)) if 'volume_surge_ratio' in df else _DEF(df)))
# 29 Correlation clustering high > q75
register(Condition(29, 'corr_gt_q75', lambda df, ctx: (df['avg_corr_btc'] > ctx.get('corr_q75', 1e9)) if 'avg_corr_btc' in df else _DEF(df)))
# 30 Correlation dispersion low < q25
register(Condition(30, 'corr_lt_q25', lambda df, ctx: (df['avg_corr_btc'] < ctx.get('corr_q25', -1e9)) if 'avg_corr_btc' in df else _DEF(df)))
# 31 Correlation mid band
register(Condition(31, 'corr_mid_band', lambda df, ctx: ((df['avg_corr_btc'] >= ctx.get('corr_q25', -1e9)) & (df['avg_corr_btc'] <= ctx.get('corr_q75', 1e9))) if 'avg_corr_btc' in df else _DEF(df)))
# 32 Divergent breadth (breadth rising vs price negative) approx: breadth > mean & mkt_ret < 0
register(Condition(32, 'breadth_divergence_pos', lambda df, ctx: ((df['breadth_above_sma_50'] > ctx.get('breadth_mean', 1e9)) & (df['mkt_ret'] < 0)) if 'breadth_above_sma_50' in df and 'mkt_ret' in df else _DEF(df)))
# 33 Divergent breadth negative: breadth < mean & mkt_ret > 0
register(Condition(33, 'breadth_divergence_neg', lambda df, ctx: ((df['breadth_above_sma_50'] < ctx.get('breadth_mean', -1e9)) & (df['mkt_ret'] > 0)) if 'breadth_above_sma_50' in df and 'mkt_ret' in df else _DEF(df)))
# 34 Vol spike (vol_zscore > vol_mean + vol_std)
register(Condition(34, 'vol_spike', lambda df, ctx: (df['vol_zscore'] > (ctx.get('vol_mean', 0) + ctx.get('vol_std', 1))) if 'vol_zscore' in df else _DEF(df)))
# 35 Vol crush (vol_zscore < vol_mean - vol_std)
register(Condition(35, 'vol_crush', lambda df, ctx: (df['vol_zscore'] < (ctx.get('vol_mean', 0) - ctx.get('vol_std', 1))) if 'vol_zscore' in df else _DEF(df)))
# 36 AdvDecl breadth expansion: adv_decl_diff > rolling median (approx > 0 and rising)
register(Condition(36, 'adv_decl_expansion', lambda df, ctx: ((df['adv_decline_diff'] > 0) & (df['adv_decline_diff'].diff() > 0)) if 'adv_decline_diff' in df else _DEF(df)))
# 37 AdvDecl breadth contraction: diff < 0 and falling
register(Condition(37, 'adv_decl_contraction', lambda df, ctx: ((df['adv_decline_diff'] < 0) & (df['adv_decline_diff'].diff() < 0)) if 'adv_decline_diff' in df else _DEF(df)))
# 38 New highs expansion: new_highs_50 rising 3 bars
register(Condition(38, 'new_highs_expanding', lambda df, ctx: (df['new_highs_50'].diff().rolling(3).sum() > 0) if 'new_highs_50' in df else _DEF(df)))
# 39 New lows expansion: new_lows_50 rising 3 bars
register(Condition(39, 'new_lows_expanding', lambda df, ctx: (df['new_lows_50'].diff().rolling(3).sum() > 0) if 'new_lows_50' in df else _DEF(df)))
# 40 Internal rotation: corr low & breadth high
register(Condition(40, 'internal_rotation', lambda df, ctx: ((df['avg_corr_btc'] < ctx.get('corr_q25', -1e9)) & (df['breadth_above_sma_50'] > ctx.get('breadth_q75', 1e9))) if 'avg_corr_btc' in df and 'breadth_above_sma_50' in df else _DEF(df)))
