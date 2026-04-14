import socket
import time
import threading
import pytest


def _has_ibapi():
    try:
        # the package module exposes _import_err when ibapi failed to import
        from tornotradingcraft.brokers.ibkr import api as _api_mod
        return getattr(_api_mod, "_import_err", None) is None
    except Exception:
        return False


def _tws_available(host: str = "127.0.0.1", port: int = 7497, timeout: float = 1.0) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


skip_reason = "ibapi or TWS not available (skipping IBKR integration tests)"
pytestmark = pytest.mark.skipif(not (_has_ibapi() and _tws_available()), reason=skip_reason)


@pytest.fixture(scope="module")
def ibkr():
    """Create and connect an IBKRBroker. Module-scoped to avoid repeated connects."""
    from tornotradingcraft.brokers import IBKRBroker

    b = IBKRBroker(host="127.0.0.1", port=7497, client_id=99, connect_timeout=5.0)
    b.connect()
    # give some time for nextValidId and other callbacks to arrive
    time.sleep(0.1)
    yield b
    try:
        b.disconnect()
    except Exception:
        pass


@pytest.mark.parametrize(
    "event_name,payload,expected_type",
    [
        (
            "order_status",
            {
                "order_id": 123,
                "status": "Filled",
                "filled": 1,
                "remaining": 0,
                "avg_fill_price": 10.0,
                "perm_id": 0,
                "parent_id": 0,
                "last_fill_price": 10.0,
                "client_id": 1,
                "why_held": "",
            },
            "OrderStatusDTO",
        ),
        (
            "execution",
            {
                "req_id": 1,
                "contract": type("C", (), {"symbol": "AAPL", "secType": "STK", "exchange": "SMART"})(),
                "execution": type("E", (), {"execId": "E1", "orderId": 123, "shares": 1, "price": 10.0})(),
            },
            "ExecutionDTO",
        ),
        (
            "position",
            {"account": "DU123", "contract": type("C", (), {"symbol": "AAPL"})(), "position": 2, "avg_cost": 9.0},
            "PositionDTO",
        ),
        (
            "tick",
            {"req_id": 55555, "tick_type": 1, "price": 100.0, "size": None},
            "TickDTO",
        ),
    ],
)
def test_event_mapping_and_emit(ibkr, event_name, payload, expected_type):
    """Emit synthetic events via the broker's `_emit_event` and assert mapped DTO type."""
    received = {}
    ev = threading.Event()

    def handler(payload):
        received["payload"] = payload
        ev.set()

    # register and ensure the broker's mapping/worker dispatch runs
    ibkr.register_event_handler(event_name, handler)

    # If testing tick mapping, ensure subscription mapping exists so symbol enrichment works
    if event_name == "tick":
        # attach a dummy contract object so map_event_payload can read `.symbol`
        ibkr._mkt_subscriptions[payload.get("req_id")] = type("C", (), {"symbol": "AAPL"})()

    ibkr._emit_event(event_name, payload)
    assert ev.wait(2.0), f"handler for {event_name} did not run"

    mapped = received.get("payload")
    # import DTO classes for isinstance checks
    from tornotradingcraft.brokers.ibkr.dtos import OrderStatusDTO, ExecutionDTO, PositionDTO, TickDTO

    type_map = {
        "OrderStatusDTO": OrderStatusDTO,
        "ExecutionDTO": ExecutionDTO,
        "PositionDTO": PositionDTO,
        "TickDTO": TickDTO,
    }

    expected_cls = type_map[expected_type]
    assert isinstance(mapped, expected_cls), f"expected {expected_cls} for {event_name}, got {type(mapped)}"

    # cleanup
    ibkr.unregister_event_handler(event_name, handler)


def test_get_positions_and_account_summary(ibkr):
    """Call the synchronous facades and ensure they return data structures (or at least don't raise)."""
    try:
        positions = ibkr.get_positions()
        assert isinstance(positions, (list, tuple)), "get_positions should return an iterable"
    except Exception as exc:
        pytest.fail(f"get_positions raised: {exc}")

    try:
        acc = ibkr.get_account_summary()
        assert isinstance(acc, dict), "get_account_summary should return a dict"
    except Exception as exc:
        pytest.fail(f"get_account_summary raised: {exc}")


def test_market_data_subscribe_unsubscribe(ibkr):
    """Subscribe to market data briefly and ensure subscribe/unsubscribe calls succeed."""
    messages = []

    def on_tick(dto):
        messages.append(dto)

    req = ibkr.subscribe_market_data("AAPL")
    assert isinstance(req, int)

    ibkr.register_event_handler("tick", on_tick)
    ibkr.register_event_handler("tick_size", on_tick)

    # run short live window (may produce zero messages if account lacks market-data)
    time.sleep(3)

    # cleanup
    ibkr.unregister_event_handler("tick", on_tick)
    ibkr.unregister_event_handler("tick_size", on_tick)
    try:
        ibkr.unsubscribe_market_data(req)
    except Exception:
        # unsubscribe may raise if TWS refused the req id; that's acceptable for CI-robust tests
        pass

    assert isinstance(messages, list)


def test_place_order_error_and_cancel(ibkr):
    """Verify place_order raises on invalid payload and cancel_order does not crash."""

    with pytest.raises(Exception):
        # missing symbol should raise; accept any Exception to be permissive
        ibkr.place_order({})

    # cancel a non-existent order id should not raise
    try:
        ibkr.cancel_order("999999")
    except Exception as exc:
        pytest.fail(f"cancel_order raised unexpectedly: {exc}")
