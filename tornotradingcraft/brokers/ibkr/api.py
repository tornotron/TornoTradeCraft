"""IB API shim and lightweight EWrapper/EClient subclass.

This module isolates the runtime import of `ibapi` so the rest of the
package can import safely (we export EClient/EWrapper/Contract/Order and
an `_import_err` sentinel). The concrete `_IBApi` class proxies callbacks
to a gateway object (kept as a generic Any to avoid import cycles).
"""
from typing import Any
from tornotradingcraft.utils.exception_utils import swallow_exceptions

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    from ibapi.order import Order
    _import_err = None
except Exception as exc:  # pragma: no cover - import-time guard
    EClient = None
    EWrapper = None
    Contract = None
    Order = None
    _import_err = exc


class _IBApi(EWrapper, EClient):
    """Thin subclass combining EWrapper and EClient.

    The instance keeps a reference to a generic gateway (Any) and forwards
    many callbacks to it. The gateway is expected to implement methods used
    in the original module (e.g. `_on_next_valid_id`, `_emit_event`).
    """

    def __init__(self, gateway: Any):
        # Avoid importing gateway type here to prevent circular imports.
        if EClient is None or EWrapper is None:
            raise ImportError("ibapi is required but not available: %s" % _import_err)
        # EClient expects the EWrapper instance as its parameter. Since this
        # class mixes EClient and EWrapper, pass self as the wrapper.
        EClient.__init__(self, self)
        EWrapper.__init__(self)
        self._gateway = gateway

    @swallow_exceptions()
    def nextValidId(self, orderId: int) -> None:
        self._gateway._on_next_valid_id(orderId)
        # swallow exceptions coming from event handlers so we don't crash the API thread
        try:
            self._gateway._emit_event("next_valid_id", {"order_id": orderId})
        except Exception:
            # best-effort emit (defensive: emit implementations may still raise)
            return

    def error(self, reqId, errorCode, errorString):
        self._gateway._last_error = (reqId, errorCode, errorString)

    @swallow_exceptions()
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        self._gateway._emit_event("order_status", {
            "order_id": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avg_fill_price": avgFillPrice,
            "perm_id": permId,
            "parent_id": parentId,
            "last_fill_price": lastFillPrice,
            "client_id": clientId,
            "why_held": whyHeld,
        })

    @swallow_exceptions()
    def openOrder(self, orderId, contract, order, orderState):
        self._gateway._emit_event("open_order", {
            "order_id": orderId,
            "contract": contract,
            "order": order,
            "order_state": orderState,
        })

    @swallow_exceptions()
    def execDetails(self, reqId, contract, execution):
        self._gateway._emit_event("execution", {
            "req_id": reqId,
            "contract": contract,
            "execution": execution,
        })

    @swallow_exceptions()
    def execDetailsEnd(self, reqId):
        self._gateway._emit_event("execution_end", {"req_id": reqId})

    @swallow_exceptions()
    def position(self, account, contract, position, avgCost):
        self._gateway._emit_event("position", {
            "account": account,
            "contract": contract,
            "position": position,
            "avg_cost": avgCost,
        })

    @swallow_exceptions()
    def positionEnd(self):
        self._gateway._emit_event("position_end", {})

    @swallow_exceptions()
    def accountSummary(self, reqId, account, tag, value, currency):
        self._gateway._emit_event("account_summary", {
            "req_id": reqId,
            "account": account,
            "tag": tag,
            "value": value,
            "currency": currency,
        })

    @swallow_exceptions()
    def accountSummaryEnd(self, reqId):
        self._gateway._emit_event("account_summary_end", {"req_id": reqId})

    @swallow_exceptions()
    def tickPrice(self, reqId, tickType, price, attrib):
        self._gateway._emit_event("tick", {
            "req_id": reqId,
            "tick_type": tickType,
            "price": price,
            "attrib": attrib,
        })

    @swallow_exceptions()
    def tickSize(self, reqId, tickType, size):
        self._gateway._emit_event("tick_size", {
            "req_id": reqId,
            "tick_type": tickType,
            "size": size,
        })
