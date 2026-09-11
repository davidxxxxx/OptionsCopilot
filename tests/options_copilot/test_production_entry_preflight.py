from __future__ import annotations

from datetime import datetime, timezone

import pytest

from options_copilot.production_runtime import ProductionPipelineInputs


NOW = datetime(2026, 8, 6, 13, 20, tzinfo=timezone.utc)


class _Nav:
    content_hash = "b" * 64
    contract_hash = "c" * 64

    def snapshot(self, *, asof: datetime) -> "_Nav":
        assert asof == NOW
        return self


class _Pacing:
    ready = True
    capability_hash = "a" * 64

    def __init__(self) -> None:
        self.calls: list[str] = []

    def decision(self, request_class: str) -> object:
        self.calls.append(request_class)
        raise AssertionError("entry preflight must stop before market-data pacing")


class _Gateway:
    def __init__(
        self,
        *,
        positions: object,
        working_orders: object = (),
        instructions: object = (),
    ) -> None:
        self.position_value = positions
        self.working_order_value = working_orders
        self.instruction_value = instructions
        self.calls: list[str] = []
        self.chain_calls = 0

    def positions(self) -> object:
        self.calls.append("positions")
        return self.position_value

    def working_orders(self) -> object:
        self.calls.append("working_orders")
        return self.working_order_value

    def unsubmitted_instructions(self) -> object:
        self.calls.append("unsubmitted_instructions")
        return self.instruction_value

    def __getattr__(self, name: str):
        if name not in {
            "scan_underlyings",
            "underlying_quotes",
            "option_expirations",
            "qualify_option_contracts",
            "option_contract_definitions",
            "option_quote_batch",
        }:
            raise AttributeError(name)

        def forbidden(*_args: object, **_kwargs: object) -> object:
            self.chain_calls += 1
            raise AssertionError(f"{name} must not run after failed account preflight")

        return forbidden


def _run(gateway: _Gateway) -> dict[str, object]:
    inputs = ProductionPipelineInputs(
        gateway,  # type: ignore[arg-type]
        _Pacing(),  # type: ignore[arg-type]
        object(),
        core_symbols=("GLD", "SPY"),
        clock=lambda: NOW,
    )
    return dict(inputs.run(scan_run_id="entry-preflight", slot_at=NOW))


@pytest.mark.parametrize(
    ("symbol", "security_type"),
    (
        ("GLD", "OPT"),
        ("SPY", "OPT"),
        ("QQQ", "BAG"),
        ("IWM", "COMBO"),
    ),
)
def test_any_nonzero_derivative_position_stops_before_every_other_broker_read(
    symbol: str,
    security_type: str,
) -> None:
    gateway = _Gateway(
        positions=(
            {
                "contract_id": 101,
                "symbol": symbol,
                "security_type": security_type,
                "quantity": "1",
            },
        ),
        working_orders=({"order_id": 1},),
        instructions=({"instruction_id": "one"},),
    )

    payload = _run(gateway)

    assert payload["status"] == "POSITION_MANAGEMENT_ONLY"
    assert payload["decision"] == "NO_TRADE"
    assert payload["reasons"] == ("POSITION_MANAGEMENT_ONLY",)
    assert gateway.calls == ["positions"]
    assert gateway.chain_calls == 0


@pytest.mark.parametrize(
    "positions",
    (
        None,
        "unknown",
        ({"contract_id": 101, "security_type": "OPT", "quantity": 0},),
        (
            {
                "contract_id": 101,
                "symbol": "SPY",
                "security_type": "OPT",
                "quantity": "unknown",
            },
        ),
    ),
)
def test_unknown_or_malformed_positions_stop_before_chain(positions: object) -> None:
    gateway = _Gateway(positions=positions)

    payload = _run(gateway)

    assert payload["reasons"] == ("POSITIONS_UNKNOWN_OR_INVALID",)
    assert gateway.calls == ["positions"]
    assert gateway.chain_calls == 0


@pytest.mark.parametrize(
    ("working_orders", "reason"),
    (
        (None, "WORKING_ORDERS_UNKNOWN"),
        (({"order_id": 1},), "WORKING_ORDERS_PRESENT"),
    ),
)
def test_working_order_state_stops_before_instruction_and_chain(
    working_orders: object,
    reason: str,
) -> None:
    gateway = _Gateway(positions=(), working_orders=working_orders)

    payload = _run(gateway)

    assert payload["reasons"] == (reason,)
    assert gateway.calls == ["positions", "working_orders"]
    assert gateway.chain_calls == 0


@pytest.mark.parametrize(
    ("instructions", "reason"),
    (
        (None, "UNSUBMITTED_INSTRUCTIONS_UNKNOWN"),
        (({"instruction_id": "one"},), "UNSUBMITTED_INSTRUCTIONS_PRESENT"),
    ),
)
def test_instruction_state_stops_before_chain(
    instructions: object,
    reason: str,
) -> None:
    gateway = _Gateway(positions=(), instructions=instructions)

    payload = _run(gateway)

    assert payload["reasons"] == (reason,)
    assert gateway.calls == [
        "positions",
        "working_orders",
        "unsubmitted_instructions",
    ]
    assert gateway.chain_calls == 0
