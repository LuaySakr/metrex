from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Dict, Any, List
import pandas as pd

class ConditionProtocol(Protocol):
    id: int
    name: str
    def evaluate(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.Series:
        """
        Return boolean Series indexed like df (by positional index of metric rows).
        """
        ...

@dataclass
class Condition:
    id: int
    name: str
    func: Any
    def evaluate(self, df: pd.DataFrame, ctx: Dict[str, Any]) -> pd.Series:
        return self.func(df, ctx)

REGISTRY: Dict[int, Condition] = {}

def register(condition: Condition):
    if condition.id in REGISTRY:
        raise ValueError(f"Duplicate condition id {condition.id}")
    REGISTRY[condition.id] = condition

def all_conditions() -> List[Condition]:
    return [REGISTRY[k] for k in sorted(REGISTRY.keys())]

def get_condition_ids() -> List[int]:
    return sorted(REGISTRY.keys())
