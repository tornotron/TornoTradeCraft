"""Data Transfer Objects for IBKR events.

TickDTO fields:
- req_id: int         # market-data request id
- tick_type: int      # IB tick type code (see IB docs)
- price: Optional[float]
- size: Optional[int]
- symbol: Optional[str]  # symbol associated with the subscription (enriched by the broker)

Handlers registered for 'tick' and 'tick_size' events will receive a
`TickDTO` instance with the fields above.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class OrderStatusDTO:
    order_id: int
    status: str
    filled: float
    remaining: float
    avg_fill_price: float
    perm_id: int
    parent_id: int
    last_fill_price: float
    client_id: int
    why_held: str


@dataclass
class ExecutionDTO:
    req_id: int
    contract: Dict[str, Any]
    execution: Dict[str, Any]


@dataclass
class PositionDTO:
    account: str
    symbol: Optional[str]
    position: float
    avg_cost: float


@dataclass
class TickDTO:
    req_id: int
    tick_type: int
    price: Optional[float] = None
    size: Optional[int] = None
    symbol: Optional[str] = None
