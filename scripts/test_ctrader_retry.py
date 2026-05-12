"""Test for cTrader transient-init-error retry (2026-05-07 fix).

Production saw intermittent "Order send failed: __init__() should return
None, not 'NoneType'" errors during cTrader order submission. Likely
Python 3.13+ / protobuf compatibility race. Fix: single retry on this
specific error pattern.

This test exercises the pattern-match gate via
CTraderBridge._is_transient_init_error. Pattern matching is the only
decision point; the rest of the retry-loop is mechanical follow-through
once the gate fires.

Run from repo root:
    python scripts/test_ctrader_retry.py
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────
# Test 1 — gate fires on the target wording
# ─────────────────────────────────────────────────────────────────────

def test_1_gate_matches_target_wording():
    print("\n[1] _is_transient_init_error matches target error wording")
    from takumi_trader.core.ctrader_worker import CTraderBridge
    fn = CTraderBridge._is_transient_init_error

    # The exact wording from production
    e = TypeError("__init__() should return None, not 'NoneType'")
    if not fn(e):
        _fail(f"should match exact production wording, didn't: {e!r}")

    # Reasonable variants the pattern should also catch
    variants = [
        TypeError("ProtoOATradeSide.__init__() should return None, not 'NoneType'"),
        TypeError("Something else: __init__() should return None, not 'NoneType' here"),
    ]
    for v in variants:
        if not fn(v):
            _fail(f"should match wording variant, didn't: {v!r}")
    _ok(f"matches exact wording + 2 variants")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — gate doesn't fire on unrelated errors (false-positive guard)
# ─────────────────────────────────────────────────────────────────────

def test_2_gate_does_not_match_unrelated_errors():
    print("\n[2] _is_transient_init_error rejects unrelated errors")
    from takumi_trader.core.ctrader_worker import CTraderBridge
    fn = CTraderBridge._is_transient_init_error

    cases = [
        TypeError("'>' not supported between instances of 'float' and 'NoneType'"),
        ValueError("'BUY' is not a valid enum value"),
        ConnectionError("cTrader server unreachable"),
        RuntimeError("Cannot enter np.errstate twice."),
        TypeError("__init__() takes 2 positional arguments but 3 were given"),
        # Has 'NoneType' but not the return-None phrase
        TypeError("expected int, got NoneType"),
        # Has 'should return None' but not 'NoneType'
        TypeError("__init__() should return None, not 'int'"),
    ]
    for e in cases:
        if fn(e):
            _fail(f"should NOT match {e!r} — would cause unnecessary retries")
    _ok(f"correctly rejects {len(cases)} unrelated error patterns")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — retry-loop integration (mocked, no Twisted/protobuf needed)
# ─────────────────────────────────────────────────────────────────────

def test_3_retry_recovers_on_second_attempt():
    """Patch the inner imports to raise the target error on first call,
    then succeed on the second. Verify:
      * retry happens
      * a WARNING log is emitted (not an error popup)
      * no _emit_error fires
    """
    print("\n[3] retry flow: first attempt fails with target error -> retry succeeds")
    import types, logging

    # Mock protobuf module — replace ProtoOATradeSide.Value with a function
    # that raises on first call, succeeds on second
    call_count = {"n": 0}

    def _value(name):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise TypeError("__init__() should return None, not 'NoneType'")
        return 1 if name == "BUY" else 2

    fake_proto_msg = types.SimpleNamespace(
        ProtoOATradeSide=types.SimpleNamespace(Value=_value),
    )
    fake_messages = types.SimpleNamespace(OpenApiModelMessages_pb2=fake_proto_msg)
    fake_ctrader = types.SimpleNamespace(messages=fake_messages)

    fake_reactor = types.SimpleNamespace(
        callFromThread=lambda *a, **kw: None,  # no-op
    )
    fake_internet = types.SimpleNamespace(reactor=fake_reactor)
    fake_twisted = types.SimpleNamespace(internet=fake_internet)

    sys.modules["twisted"] = fake_twisted
    sys.modules["twisted.internet"] = fake_internet
    sys.modules["ctrader_open_api"] = types.SimpleNamespace(messages=fake_messages)
    sys.modules["ctrader_open_api.messages"] = fake_messages
    sys.modules["ctrader_open_api.messages.OpenApiModelMessages_pb2"] = fake_proto_msg

    # Capture log warnings
    captured: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, r):
            captured.append(r)

    target_logger = logging.getLogger("takumi_trader.core.ctrader_worker")
    target_logger.setLevel(logging.INFO)
    h = _Cap()
    h.setLevel(logging.INFO)
    target_logger.addHandler(h)
    try:
        # Construct a barely-functional CTraderBridge
        from PyQt6.QtWidgets import QApplication
        _app = QApplication.instance() or QApplication(sys.argv)
        from takumi_trader.core.ctrader_worker import CTraderBridge

        bridge = CTraderBridge()
        bridge._is_connected = True
        bridge._symbol_map = {"EURUSD": 1}
        # Capture error emissions
        emitted: list[tuple[str, str]] = []
        # _emit_error is a method that emits a Qt signal; replace it with a sink
        bridge._emit_error = lambda pair, err: emitted.append((pair, err))

        # Call open_order — should retry after the first attempt fails
        bridge.open_order(
            pair="EURUSD", direction="BUY", volume_lots=0.01,
            sl_price=0.0, tp_price=0.0, sl_pips=20.0, tp_pips=40.0,
        )

        # Verify call_count is 2 (retry happened)
        if call_count["n"] != 2:
            _fail(f"expected 2 attempts, got {call_count['n']}")

        # Verify no _emit_error fired (because retry succeeded)
        if emitted:
            _fail(f"expected no error emissions, got: {emitted}")

        # Verify a WARNING was logged about the recovery
        warns = [r for r in captured if r.levelno >= logging.WARNING]
        if not any("recovered on retry" in r.getMessage() for r in warns):
            msgs = [r.getMessage() for r in captured]
            _fail(f"expected 'recovered on retry' warning, got: {msgs}")
        _ok("retry recovered cleanly; warning logged; no error emission")
    finally:
        target_logger.removeHandler(h)


# ─────────────────────────────────────────────────────────────────────
# Test 4 — both attempts fail -> error emission with traceback
# ─────────────────────────────────────────────────────────────────────

def test_4_both_attempts_fail_emits_error():
    print("\n[4] retry flow: both attempts fail -> error emitted to operator")
    import types, logging, sys as _sys

    # Always-fail mock
    def _value(name):
        raise TypeError("__init__() should return None, not 'NoneType'")

    fake_proto_msg = types.SimpleNamespace(
        ProtoOATradeSide=types.SimpleNamespace(Value=_value),
    )
    fake_messages = types.SimpleNamespace(OpenApiModelMessages_pb2=fake_proto_msg)
    fake_reactor = types.SimpleNamespace(callFromThread=lambda *a, **kw: None)
    fake_internet = types.SimpleNamespace(reactor=fake_reactor)

    _sys.modules["twisted.internet"] = fake_internet
    _sys.modules["ctrader_open_api.messages.OpenApiModelMessages_pb2"] = fake_proto_msg

    captured: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, r):
            captured.append(r)

    target_logger = logging.getLogger("takumi_trader.core.ctrader_worker")
    target_logger.setLevel(logging.INFO)
    h = _Cap()
    h.setLevel(logging.INFO)
    target_logger.addHandler(h)
    try:
        from PyQt6.QtWidgets import QApplication
        _app = QApplication.instance() or QApplication(sys.argv)
        from takumi_trader.core.ctrader_worker import CTraderBridge

        bridge = CTraderBridge()
        bridge._is_connected = True
        bridge._symbol_map = {"EURUSD": 1}
        emitted: list[tuple[str, str]] = []
        bridge._emit_error = lambda pair, err: emitted.append((pair, err))

        bridge.open_order(
            pair="EURUSD", direction="BUY", volume_lots=0.01,
            sl_price=0.0, tp_price=0.0, sl_pips=20.0, tp_pips=40.0,
        )

        # Should have emitted exactly one error
        if len(emitted) != 1:
            _fail(f"expected 1 error emission, got {len(emitted)}: {emitted}")
        if "__init__() should return None" not in emitted[0][1]:
            _fail(f"error message should include the original wording: {emitted[0][1]}")

        # Should have logged at ERROR level with traceback (exc_info=True)
        errs = [r for r in captured if r.levelno >= logging.ERROR]
        if not errs:
            _fail("expected at least one ERROR log entry")
        # exc_info=True attaches the exception to the record
        if not any(r.exc_info for r in errs):
            _fail("expected exc_info to be attached on the final-failure log")
        _ok("both attempts failed -> single error emission + traceback log")
    finally:
        target_logger.removeHandler(h)


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("cTrader transient-init-error retry test")
    print("=" * 64)
    test_1_gate_matches_target_wording()
    test_2_gate_does_not_match_unrelated_errors()
    test_3_retry_recovers_on_second_attempt()
    test_4_both_attempts_fail_emits_error()
    print("\n" + "=" * 64)
    print("ALL CTRADER RETRY TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
