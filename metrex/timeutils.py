"""
Time utilities for metrex: timerange parsing, timezone handling
"""
from datetime import datetime
from typing import Tuple
import pandas as pd

def parse_timerange(timerange: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    if '-' not in timerange:
        raise ValueError(f"Invalid timerange format: {timerange}")
    start_str, end_str = timerange.split('-', 1)
    start = pd.Timestamp(datetime.strptime(start_str, '%Y%m%d'))
    end = pd.Timestamp(datetime.strptime(end_str, '%Y%m%d'))
    return start, end

def filter_timerange(df: pd.DataFrame, timerange: str) -> pd.DataFrame:
    start, end = parse_timerange(timerange)
    # Align tz-awareness: if df['date'] is tz-aware, localize start/end to UTC
    if pd.api.types.is_datetime64tz_dtype(df['date']):
        start = start.tz_localize('UTC') if start.tzinfo is None else start
        end = end.tz_localize('UTC') if end.tzinfo is None else end
    mask = (df['date'] >= start) & (df['date'] <= end)
    return df.loc[mask].copy()
