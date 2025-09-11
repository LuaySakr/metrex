
from typing import Dict, List
from .base import MetricProtocol

REGISTRY: Dict[str, MetricProtocol] = {}

def register(metric: MetricProtocol):
    REGISTRY[metric.name] = metric

def get_selected(names: List[str]) -> List[MetricProtocol]:
    return [REGISTRY[n] for n in names if n in REGISTRY]

def all_names() -> List[str]:
    return list(REGISTRY.keys())

# Import all metric modules to ensure registration
from . import breadth_sma50, btc_trend_slope, market_vol_regime, adv_decline, new_highs_lows, volume_surge_ratio, avg_correlation_btc, market_return_ma
