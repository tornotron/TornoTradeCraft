from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any
from pandas import DataFrame
from tornotradingcraft.providers.providers import DataProvider


class Backtester(ABC):
    """Abstract backtester contract.

    Minimal contract used by the project. Concrete implementations should
    implement methods to prepare provider data into the backtester format
    and run the backtest.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_data_from_provider(self, provider: DataProvider, data: DataFrame, to_btfeed: bool = False, **kwargs) -> DataFrame:
        """Normalize provider-specific DataFrame into the backtest-ready OHLCV DataFrame.

        Implementations should inspect `provider` (type or metadata) and normalize
        `data` columns into ['open','high','low','close','volume'] and return the
        normalized DataFrame. This lets Backtester implementations handle provider
        differences.
        
        If `to_btfeed` is True this returns a backtrader feed object (line).
        Returns an object suitable to be consumed by `run` (for backtrader this
        will typically be a Cerebro-compatible OHLCV line object).
        Otherwise it returns the normalized DataFrame.
        
        """
        raise NotImplementedError

    @abstractmethod
    def run(self, prepared_data: Any, strategy: Any = None, **kwargs) -> Any:
        """Run a backtest using the prepared data and optional strategy.

        Returns backend-specific results object.
        """
        raise NotImplementedError

    @abstractmethod
    def build_jupyter_chart(self, results: Any, *args, **kwargs) -> Any:
        """Build and return a plotting/chart object plus any helper dataframes.

        Implementations should return at minimum a chart object. Concrete
        backtesters may return additional helper dataframes as a tuple.
        """
        raise NotImplementedError


__all__ = ["Backtester"]
