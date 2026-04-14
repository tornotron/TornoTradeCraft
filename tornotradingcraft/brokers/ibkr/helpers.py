"""Helper builders and event payload mapping for the IBKR broker.

Functions here are used by `broker_ibkr.IBKRBroker` to build `Contract` and
`Order` objects and to map raw ibapi payloads into DTOs.
"""
from typing import Any, Dict

from tornotradingcraft.brokers.broker import Broker

from .api import Contract, Order
from .dtos import OrderStatusDTO, ExecutionDTO, PositionDTO, TickDTO


def build_contract(symbol: str, sec_type: str = "STK", currency: str = "USD", exchange: str = "SMART") -> Any:
    c = Contract()
    c.symbol = symbol
    c.secType = sec_type
    c.currency = currency
    c.exchange = exchange
    return c


def build_order(side: str, qty: float, order_type: str = "MKT", price: float = None, tif: str = "DAY") -> Any:
    ord = Order()
    ord.action = "BUY" if str(side).lower() in ("buy", "b") else "SELL"
    ord.totalQuantity = qty
    ord.orderType = order_type
    ord.tif = tif
    if price is not None and order_type in ("LMT", "LMT+"):
        # ibapi Order uses `lmtPrice` for limit orders
        ord.lmtPrice = price
    return ord


def map_event_payload(broker: Broker, event_name: str, payload: Dict[str, Any]):
    """Map raw callback payloads to DTOs.

    Returns typed dataclass instances for common events:
    - OrderStatusDTO for 'order_status'
    - ExecutionDTO for 'execution'
    - PositionDTO for 'position'
    - TickDTO for 'tick' and 'tick_size' (includes `symbol` field)

    The function accesses `broker._mkt_subscriptions` to enrich tick events
    with the subscribed contract's symbol.
    """
    try:
        if event_name == "order_status":
            return OrderStatusDTO(
                order_id=int(payload.get("order_id")),
                status=str(payload.get("status")),
                filled=float(payload.get("filled") or 0.0),
                remaining=float(payload.get("remaining") or 0.0),
                avg_fill_price=float(payload.get("avg_fill_price") or 0.0),
                perm_id=int(payload.get("perm_id") or 0),
                parent_id=int(payload.get("parent_id") or 0),
                last_fill_price=float(payload.get("last_fill_price") or 0.0),
                client_id=int(payload.get("client_id") or 0),
                why_held=str(payload.get("why_held") or ""),
            )

        if event_name == "execution":
            contract = payload.get("contract")
            exec_obj = payload.get("execution")
            return ExecutionDTO(
                req_id=int(payload.get("req_id") or 0),
                contract={
                    "symbol": getattr(contract, "symbol", None),
                    "secType": getattr(contract, "secType", None),
                    "exchange": getattr(contract, "exchange", None),
                },
                execution={
                    "execId": getattr(exec_obj, "execId", None),
                    "orderId": getattr(exec_obj, "orderId", None),
                    "shares": getattr(exec_obj, "shares", None),
                    "price": getattr(exec_obj, "price", None),
                },
            )

        if event_name == "position":
            contract = payload.get("contract")
            return PositionDTO(
                account=payload.get("account"),
                symbol=getattr(contract, "symbol", None),
                position=float(payload.get("position") or 0.0),
                avg_cost=float(payload.get("avg_cost") or 0.0),
            )

        if event_name in ("tick", "tick_size"):
            req_id = int(payload.get("req_id") or 0)
            contract = broker._mkt_subscriptions.get(req_id)
            symbol = getattr(contract, "symbol", None) if contract is not None else None
            # Defensive conversions: price may be present on 'tick' events
            # and size may be present on 'tick_size' events. Normalize types
            # to float/int or None so handlers receive predictable values.
            price_val = payload.get("price")
            try:
                price = None if price_val is None else float(price_val)
            except Exception:
                price = None

            size_val = payload.get("size")
            try:
                size = None if size_val is None else int(size_val)
            except Exception:
                size = None

            try:
                tick_type = int(payload.get("tick_type") or 0)
            except Exception:
                tick_type = 0

            dto = TickDTO(
                req_id=req_id,
                tick_type=tick_type,
                price=price,
                size=size,
                symbol=symbol,
            )
            return dto
    except Exception:
        return payload
    return payload
