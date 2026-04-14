"""Abstract data provider interfaces and DTOs for market data.

This module defines a small set of data-transfer objects and an abstract
base class `DataProvider` that concrete provider implementations should
inherit from (e.g. YahooFinanceProvider, AlphaVantageProvider, StockScreenerProvider).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Iterable
from pandas import DataFrame
from enum import Enum

@dataclass
class SymbolInfo:
    """Metadata for a tradable symbol."""

    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    category_name: Optional[str] = None
    country: Optional[str] = None
    currency: Optional[str] = None


@dataclass
class Quote:
    """Latest price/quote for a symbol."""

    symbol: str
    timestamp: datetime
    price: float
    bid: Optional[float] = None
    ask: Optional[float] = None
    volume: Optional[float] = None
    raw: Optional[dict] = None  # provider-specific raw payload


@dataclass
class OHLCV:
    """One historical bar (open-high-low-close-volume)."""

    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    raw: Optional[dict] = None


class DataProvider(ABC):
    """Abstract base class for market data providers.

    Implementations should be synchronous and raise informative exceptions
    on errors. Asynchronous providers can wrap an async client and expose
    the same sync interface (or you can provide a separate AsyncDataProvider).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable provider name (e.g. "YahooFinance")."""
        raise NotImplementedError
    
    @abstractmethod
    def update_supported_symbols(self, filepath: str) -> None:
        """Update the cached list of supported symbols from the provider.

        This is optional; if not implemented, `get_supported_symbols` will
        return the cached list.
        """
        raise NotImplementedError

    @abstractmethod
    def get_supported_symbols(self) -> List[str]:
        """Return a (possibly cached) list of supported symbols.

        Note: this may be expensive for some providers; implementations
        can return a cached subset or raise if not supported.
        """
        raise NotImplementedError

    @abstractmethod
    def search_symbols(self, query: str, limit: int = 10) -> List[SymbolInfo]:
        """Search symbols by a free-text query and return matching SymbolInfo."""
        raise NotImplementedError

    @abstractmethod
    def get_symbol_info(self) -> List[SymbolInfo]:
        """Return metadata for all supported symbols.

        This is optional; if not implemented, users can call `search_symbols`
        or `get_symbol_info(symbol)` for individual symbols.
        """
        raise NotImplementedError

    @abstractmethod
    def get_quote(self, symbol: str) -> Quote:
        """Return the latest Quote for the given symbol."""
        raise NotImplementedError

    @abstractmethod
    def fetch_bulk_quotes(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        """Return latest quotes for multiple symbols as a dict keyed by symbol."""
        raise NotImplementedError

    @abstractmethod
    def get_historical_data(
        self,
        symbol: str,
        period: Optional[Enum] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        interval: Optional[Enum] = None,
    ) -> DataFrame:
        """Return historical data in a pandas Dataframe for the symbol between start and end.

        Interval is provider-specific (e.g. "1d", "1h", "1m").
        """
        raise NotImplementedError

    @abstractmethod
    def convert_to_jupyter_chart_format(self, data: DataFrame) -> DataFrame:
        """Convert provider-specific historical DataFrame to a JupyterChart-friendly DataFrame.

        Expected returned columns: ['time', 'open', 'high', 'low', 'close', 'volume'].
        Implementations should normalize column names and ensure 'time' is a column
        (convert DatetimeIndex to a column if necessary).
        """
        raise NotImplementedError


__all__ = ["SymbolInfo", "Quote", "OHLCV", "DataProvider"]
