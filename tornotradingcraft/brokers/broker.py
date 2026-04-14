from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable


class BrokerError(Exception):
    """Base exception for broker related errors."""


class Broker(ABC):
    """Abstract Broker interface used by the framework.

    Implementations should be thin adapters around broker-specific SDKs
    (for example Interactive Brokers / Alpaca / paper brokers). The goal is
    to provide a consistent API for connect/disconnect, order management and
    data subscriptions so the rest of the framework can swap brokers easily.

    Minimal contract (inputs/outputs):
    - connect(connect_args...) -> None (raises BrokerError on failure)
    - disconnect() -> None
    - is_connected() -> bool
    - place_order(order_payload: dict) -> str  # returns order id
    - cancel_order(order_id: str) -> None
    - get_positions() -> Iterable[Dict[str, Any]]
    - get_account_summary() -> Dict[str, Any]

    Error modes: raise BrokerError on errors.
    """

    @abstractmethod
    def connect(self, *args, **kwargs) -> None:
        """Open a connection to the broker. May block until connected.

        For brokers with special initialization (for example IBKR which
        requires the TWS / IB Gateway to be running and an event loop), the
        implementation is responsible for handling initialization and
        synchronization.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection and cleanup resources."""

    @abstractmethod
    def is_connected(self) -> bool:
        """Return True when the broker connection is alive and usable."""

    @abstractmethod
    def place_order(self, order_payload: Dict[str, Any]) -> str:
        """Place an order and return a broker-assigned order id.

        order_payload is a broker-agnostic dict the implementation will translate
        into the provider's native order object. Keep keys simple (symbol, qty,
        side, type, price, time_in_force, ...) so implementations can map them.
        """

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        """Cancel an existing order by id."""

    @abstractmethod
    def get_positions(self) -> Iterable[Dict[str, Any]]:
        """Return current positions as an iterable of dicts."""

    @abstractmethod
    def get_account_summary(self) -> Dict[str, Any]:
        """Return account-level summary information (balances, margin, etc.)."""

    # Optional: data subscription hooks
    def subscribe_market_data(self, symbol: str) -> Any:
        """Subscribe to live market data for `symbol`. Implementation-specific

        Return a subscription id or handler object that can be used to
        unsubscribe later.
        """

    def unsubscribe_market_data(self, subscription: Any) -> None:
        """Cancel a previous market data subscription."""
