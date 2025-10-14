from __future__ import annotations
import pandas as pd
from .base import Condition, register


def _s(df, default=False):
	return pd.Series(default, index=df.index)

# 1 Breadth above its global mean
register(Condition(
	1,
	'breadth_gt_mean',
	lambda df, ctx: (df['breadth_above_sma_50'] > ctx.get('breadth_mean', float('inf')))
	if 'breadth_above_sma_50' in df else _s(df)
))
# 2 Breadth above median
register(Condition(
	2,
	'breadth_gt_median',
	lambda df, ctx: (df['breadth_above_sma_50'] > ctx.get('breadth_median', float('inf')))
	if 'breadth_above_sma_50' in df else _s(df)
))
# 3 Breadth > mean + std
register(Condition(
	3,
	'breadth_gt_mean_plus_std',
	lambda df, ctx: (df['breadth_above_sma_50'] > (ctx.get('breadth_mean', 0)+ctx.get('breadth_std', 1)))
	if 'breadth_above_sma_50' in df else _s(df)
))
# 4 Breadth < mean
register(Condition(
	4,
	'breadth_lt_mean',
	lambda df, ctx: (df['breadth_above_sma_50'] < ctx.get('breadth_mean', float('-inf')))
	if 'breadth_above_sma_50' in df else _s(df)
))
# 5 Market return > 0
register(Condition(
	5,
	'mkt_ret_gt0',
	lambda df, ctx: (df['mkt_ret'] > 0) if 'mkt_ret' in df else _s(df)
))
# 6 Market return < 0
register(Condition(
	6,
	'mkt_ret_lt0',
	lambda df, ctx: (df['mkt_ret'] < 0) if 'mkt_ret' in df else _s(df)
))
# 7 Vol regime high
register(Condition(
	7,
	'vol_regime_high',
	lambda df, ctx: (df['market_vol_regime'] == 'high') if 'market_vol_regime' in df else _s(df)
))
# 8 Vol regime low
register(Condition(
	8,
	'vol_regime_low',
	lambda df, ctx: (df['market_vol_regime'] == 'low') if 'market_vol_regime' in df else _s(df)
))
# 9 BTC slope > 0
register(Condition(
	9,
	'btc_slope_gt0',
	lambda df, ctx: (df['btc_trend_slope'] > 0) if 'btc_trend_slope' in df else _s(df)
))
# 10 BTC slope < 0
register(Condition(
	10,
	'btc_slope_lt0',
	lambda df, ctx: (df['btc_trend_slope'] < 0) if 'btc_trend_slope' in df else _s(df)
))
# 11 Adv > Decl
register(Condition(
	11,
	'adv_gt_decl',
	lambda df, ctx: (df['adv_count'] > df['decl_count']) if 'adv_count' in df and 'decl_count' in df else _s(df)
))
# 12 AdvDecl diff > 0
register(Condition(
	12,
	'adv_decl_diff_pos',
	lambda df, ctx: (df['adv_decline_diff'] > 0) if 'adv_decline_diff' in df else _s(df)
))
# 13 AdvDecl line rising
register(Condition(
	13,
	'adv_decl_line_rising',
	lambda df, ctx: (df['adv_decline_line'].diff() > 0) if 'adv_decline_line' in df else _s(df)
))
# 14 New highs > lows
register(Condition(
	14,
	'new_highs_gt_lows',
	lambda df, ctx: (df['new_highs_50'] > df['new_lows_50']) if 'new_highs_50' in df and 'new_lows_50' in df else _s(df)
))
# 15 Volume surge > 1
register(Condition(
	15,
	'vol_surge_gt1',
	lambda df, ctx: (df['volume_surge_ratio'] > 1) if 'volume_surge_ratio' in df else _s(df)
))
# 16 Volume surge > 1.5
register(Condition(
	16,
	'vol_surge_gt1_5',
	lambda df, ctx: (df['volume_surge_ratio'] > 1.5) if 'volume_surge_ratio' in df else _s(df)
))
# 17 Avg corr > 0.5
register(Condition(
	17,
	'avg_corr_high',
	lambda df, ctx: (df['avg_corr_btc'] > 0.5) if 'avg_corr_btc' in df else _s(df)
))
# 18 Avg corr < 0.2
register(Condition(
	18,
	'avg_corr_low',
	lambda df, ctx: (df['avg_corr_btc'] < 0.2) if 'avg_corr_btc' in df else _s(df)
))
# 19 mkt_ret > mkt_ret_sma20
register(Condition(
	19,
	'mkt_ret_gt_sma20',
	lambda df, ctx: (df['mkt_ret'] > df['mkt_ret_sma20']) if 'mkt_ret' in df and 'mkt_ret_sma20' in df else _s(df)
))
# 20 mkt_ret < mkt_ret_sma20
register(Condition(
	20,
	'mkt_ret_lt_sma20',
	lambda df, ctx: (df['mkt_ret'] < df['mkt_ret_sma20']) if 'mkt_ret' in df and 'mkt_ret_sma20' in df else _s(df)
))
