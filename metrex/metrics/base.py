from typing import Protocol, Dict, Any
import pandas as pd

class MetricProtocol(Protocol):
    name: str
    def compute(self, market_df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.DataFrame:
        """
        Returns DataFrame with columns: ['date', '<metric_columns...>'] sorted by date.
        No pair column in the result (market-level metrics).
        """
        ...
