"""Interactive Brokers (IBKR) broker adapter.

This module implements a thin wrapper around the official `ibapi` package
providing a subset of the `Broker` interface needed by the framework.

Notes:
- IBKR uses an event-driven API and requires TWS / IB Gateway to be running
  (you mentioned it's running on port 7497). The implementation below starts
  the client in a background thread and waits for a readiness event (next
  valid order id) before allowing operations.
- This wrapper aims to be minimal and safe to import even if `ibapi` is
  not installed. If `ibapi` is missing, trying to instantiate IBKRBroker will
  raise ImportError.
"""

import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, Optional, Callable

from tornotradingcraft.brokers.broker import Broker, BrokerError

# split helpers and api into smaller modules
from .api import EClient, EWrapper, _import_err, _IBApi
from .helpers import build_contract, build_order, map_event_payload
from tornotradingcraft.utils import swallow_exceptions, expect_exception, safe_call



class IBKRBroker(Broker):
    """Interactive Brokers adapter implementing the `Broker` interface.

    Example usage:
        b = IBKRBroker(host='127.0.0.1', port=7497, client_id=42)
        b.connect()
        oid = b.place_order({...})
        b.disconnect()
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1, connect_timeout: float = 10.0):
        if EClient is None or EWrapper is None:
            raise ImportError("ibapi is required for IBKRBroker: %s" % _import_err)

        self.host = host
        self.port = port
        self.client_id = client_id
        self._connect_timeout = connect_timeout

        self._api: Optional[_IBApi] = None
        self._thread: Optional[threading.Thread] = None
        self._ready_event = threading.Event()
        self._next_order_id: Optional[int] = None
        self._last_error = None
        self._connected = False
        # event handlers: event_name -> list of callables(payload)
        self._event_handlers = {}
        # market data req_id -> contract mapping (for symbol lookups)
        self._mkt_subscriptions = {}
        # executor for handler invocation to limit thread usage
        self._executor = ThreadPoolExecutor(max_workers=8)
        # event queue and worker thread to serialize event dispatch and apply backpressure
        self._event_queue = queue.Queue(maxsize=1000)
        self._event_worker_stop = threading.Event()
        self._event_worker_thread = threading.Thread(target=self._event_worker, daemon=True)
        self._event_worker_thread.start()
        # event used when actively requesting a fresh next order id
        self._next_id_event = None

    # internal callbacks
    def _on_next_valid_id(self, order_id: int) -> None:
        self._next_order_id = order_id
        self._ready_event.set()
        self._connected = True
        # if someone is waiting for a fresh next order id, signal them
        if self._next_id_event is not None:
            try:
                self._next_id_event.set()
            finally:
                # clear so future callbacks don't erroneously signal
                self._next_id_event = None

    # Event registration API
    def register_event_handler(self, event_name: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a handler for broker events.

        Handlers receive a single dict-like payload. Common events:
        - 'next_valid_id'
        - 'order_status'
        - 'open_order'
        - 'execution'
        - 'execution_end'
        - 'position'
        - 'position_end'
        - 'account_summary'
        - 'account_summary_end'
        """
        self._event_handlers.setdefault(event_name, []).append(handler)

    def unregister_event_handler(self, event_name: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        handlers = self._event_handlers.get(event_name)
        if not handlers:
            return
        try:
            handlers.remove(handler)
        except ValueError:
            pass

    def _emit_event(self, event_name: str, payload: Dict[str, Any]) -> None:
        """Emit an event by enqueueing it; the background worker will
        dispatch to registered handlers using the executor. This avoids
        creating threads on the IB API thread and gives us a single place to
        control backpressure.
        """
        mapped = map_event_payload(self, event_name, payload)
        try:
            self._event_queue.put_nowait((event_name, mapped))
        except queue.Full:
            # drop the event if the queue is full to avoid blocking the API
            # thread; in production consider metrics or a bounded retry.
            pass

    def _event_worker(self) -> None:
        """Background worker that pulls events from the queue and submits
        handler calls to the executor.
        """
        while not self._event_worker_stop.is_set():
            try:
                event_name, mapped = self._event_queue.get(timeout=0.5)
            except Exception:
                continue

            handlers = list(self._event_handlers.get(event_name, []))
            for h in handlers:
                # best-effort submit; swallow scheduling exceptions
                safe_call(self._executor.submit, h, mapped)
            self._event_queue.task_done()

        # mapping delegated to helpers.map_event_payload

    def connect(self, *args, **kwargs) -> None:
        # Start the IB API client and wait for nextValidId as readiness signal
        if self._connected:
            return

        self._api = _IBApi(self)
        self._api.connect(self.host, self.port, self.client_id)

        def run_loop():
            try:
                self._api.run()
            except Exception:
                # run can throw if connection lost; mark disconnected
                self._connected = False

        self._thread = threading.Thread(target=run_loop, daemon=True)
        self._thread.start()

        # wait for nextValidId callback
        if not self._ready_event.wait(timeout=self._connect_timeout):
            # cleanup (best-effort)
            safe_call(getattr(self._api, "disconnect", lambda: None))
            raise BrokerError("Timed out waiting for IBKR API readiness")

    def disconnect(self) -> None:
        if self._api is None:
            return
        try:
            self._api.disconnect()
        finally:
            self._connected = False
            self._ready_event.clear()
            # stop the event worker and shutdown executor (best-effort)
            safe_call(self._event_worker_stop.set)
            safe_call(self._event_worker_thread.join, timeout=1.0)
            # stop accepting new handler tasks; do not wait for running ones
            safe_call(self._executor.shutdown, wait=False)
            # clear market data subscriptions map
            safe_call(self._mkt_subscriptions.clear)

    def is_connected(self) -> bool:
        return bool(self._connected)


    @expect_exception(BrokerError, "Failed to place order: {exc}")
    def place_order(self, order_payload: Dict[str, Any]) -> str:
        if not self.is_connected():
            raise BrokerError("IBKR broker not connected")

        symbol = order_payload.get("symbol")
        if not symbol:
            raise BrokerError("order_payload missing 'symbol'")

        qty = order_payload.get("qty") or order_payload.get("quantity")
        if qty is None:
            raise BrokerError("order_payload missing 'qty' or 'quantity'")

        side = order_payload.get("side", "buy")
        order_type = order_payload.get("type", "MKT")
        price = order_payload.get("price")

        contract = build_contract(symbol)
        order = build_order(side, qty, order_type, price)

        # Request a fresh next order id from the gateway to avoid id reuse.
        # This issues an reqIds(1) call and waits briefly for the nextValidId
        # callback to arrive which updates self._next_order_id.
        try:
            self._request_next_order_id(timeout=2.0)
        except Exception as exc:
            # preserve the original, more specific error message here
            raise BrokerError(f"Failed to obtain next order id: {exc}")

        order_id = self._next_order_id
        if order_id is None:
            raise BrokerError("No valid order id available after requesting one")

        # placeOrder is synchronous call into the API; results and fills come via callbacks
        # placeOrder is synchronous call into the API; results and fills come via callbacks
        self._api.placeOrder(order_id, contract, order)

        # do NOT manually increment _next_order_id here; the gateway will
        # provide the authoritative nextValidId via callback. Callers that need
        # to place many orders rapidly may request ids explicitly.
        return str(order_id)

    def _request_next_order_id(self, timeout: float = 2.0) -> None:
        """Request a fresh nextValidId from the gateway and wait up to
        `timeout` seconds for the callback. This uses reqIds(1) which will
        trigger nextValidId on the EWrapper.
        """
        if self._api is None:
            raise BrokerError("IB API not initialized")

        # If we already have a next id, still request a fresh one to stay
        # synchronized with the gateway.
        ev = threading.Event()
        self._next_id_event = ev
        try:
            # ask the gateway for ids; will trigger nextValidId
            self._api.reqIds(1)
        except Exception as exc:
            self._next_id_event = None
            raise BrokerError(f"reqIds failed: {exc}")

        if not ev.wait(timeout=timeout):
            # timed out; clear and raise
            self._next_id_event = None
            raise BrokerError("Timed out waiting for nextValidId from gateway")

    @expect_exception(BrokerError, "Failed to cancel order {order_id}: {exc}")
    def cancel_order(self, order_id: str) -> None:
        if not self.is_connected():
            raise BrokerError("IBKR broker not connected")
        self._api.cancelOrder(int(order_id))

    def get_positions(self) -> Iterable[Dict[str, Any]]:
        # requestPositions is callback-based; use the persistent event system
        # to collect results synchronously for callers that prefer a blocking
        # API.
        result = []
        event = threading.Event()

        @swallow_exceptions()
        def _on_position(payload: Dict[str, Any]) -> None:
                contract = payload.get("contract")
                result.append({
                    "account": payload.get("account"),
                    "symbol": getattr(contract, "symbol", None),
                    "position": payload.get("position"),
                    "avg_cost": payload.get("avg_cost"),
                })

        def _on_position_end(_payload: Dict[str, Any]) -> None:
            event.set()

        # register temporary handlers
        self.register_event_handler("position", _on_position)
        self.register_event_handler("position_end", _on_position_end)

        try:
            self._api.reqPositions()
            event.wait(timeout=5.0)
            self._api.cancelPositions()
        finally:
            # cleanup handlers
            self.unregister_event_handler("position", _on_position)
            self.unregister_event_handler("position_end", _on_position_end)

        return result

    def get_account_summary(self) -> Dict[str, Any]:
        # Minimal synchronous facade implemented on top of the event system.
        result: Dict[str, Any] = {}
        event = threading.Event()

        @swallow_exceptions()
        def _on_acc_summary(payload: Dict[str, Any]) -> None:
                account = payload.get("account")
                tag = payload.get("tag")
                value = payload.get("value")
                currency = payload.get("currency")
                result_key = f"{account}.{tag}"
                result[result_key] = {"value": value, "currency": currency}

        def _on_acc_summary_end(_payload: Dict[str, Any]) -> None:
            event.set()

        self.register_event_handler("account_summary", _on_acc_summary)
        self.register_event_handler("account_summary_end", _on_acc_summary_end)

        try:
            req_id = 9001
            self._api.reqAccountSummary(req_id, "All", "NetLiquidation,TotalCashValue")
            event.wait(timeout=5.0)
            self._api.cancelAccountSummary(req_id)
        finally:
            self.unregister_event_handler("account_summary", _on_acc_summary)
            self.unregister_event_handler("account_summary_end", _on_acc_summary_end)

        return result

    def subscribe_market_data(self, symbol: str) -> int:
        # Return a request id for the market data subscription.
        # For a production-ready solution, add management of handlers and
        # streaming callbacks.
        #
        # Tick events ('tick' and 'tick_size') are emitted as `TickDTO`
        # dataclass instances (fields: req_id, tick_type, price, size, symbol)
        # to handlers registered via `register_event_handler`.
        if not self.is_connected():
            raise BrokerError("IBKR broker not connected")
        contract = build_contract(symbol)
        req_id = int(time.time() * 1000) % (2 ** 31)
        # store mapping so tick events can be enriched with symbol
        self._mkt_subscriptions[req_id] = contract
        self._api.reqMktData(req_id, contract, "", False, False, [])
        return req_id

    def unsubscribe_market_data(self, subscription: Any) -> None:
        if not self.is_connected():
            return
        try:
            self._api.cancelMktData(int(subscription))
        finally:
            # remove mapping if present (best-effort)
            safe_call(self._mkt_subscriptions.pop, int(subscription), None)
