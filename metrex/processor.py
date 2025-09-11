"""
Processor orchestrates: load -> filter -> run metrics -> merge -> save
"""
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from .io import load_feathers, save
from .timeutils import filter_timerange
from .metrics import get_selected, all_names, REGISTRY

def load_market(datafolder: Path, timeframe: str) -> pd.DataFrame:
    return load_feathers(datafolder, timeframe)

def run_metrics(df: pd.DataFrame, metric_names: List[str], ctx: Dict[str, Any]) -> pd.DataFrame:
    metrics = get_selected(metric_names)
    metric_frames = [m.compute(df, ctx) for m in metrics]
    # Outer join on date, then ffill, dropna(how='all') on metric columns
    result = metric_frames[0].set_index('date')
    for mf in metric_frames[1:]:
        result = result.join(mf.set_index('date'), how='outer')
    result = result.sort_index().ffill().dropna(how='all')
    result = result.reset_index()
    return result

def process(datafolder: Path, timeframe: str, timerange: str, metric_names: List[str], output: Path, ctx: Dict[str, Any] = {}):
    df = load_market(datafolder, timeframe)
    df = filter_timerange(df, timerange)
    result = run_metrics(df, metric_names, ctx)
    save(result, output)
