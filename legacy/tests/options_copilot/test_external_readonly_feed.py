from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from options_copilot.gateway.external_readonly_feed import (
    EXTERNAL_READONLY_FEED_SCHEMA,
    EXTERNAL_READONLY_FEED_VERSION,
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    ExternalFeedValidationError,
    ExternalReadonlyFeedPublisher,
    ExternalReadonlyFeedReader,
)
from options_copilot.gateway.broker_snapshot import (
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
)
from options_copilot.gateway.ibkr_readonly import OptionContractRef, QuoteBatchStatus
from options_copilot.storage.canonical import canonical_hash, canonical_json


UTC = timezone.utc
REQUESTED_AT = datetime(2026, 8, 6, 13, 20, 0, tzinfo=UTC)
COMPLETED_AT = datetime(2026, 8, 6, 13, 20, 1, tzinfo=UTC)
WRITTEN_AT = datetime(2026, 8, 6, 13, 20, 2, tzinfo=UTC)


def _identity(contract_id: int, strike: str) -> dict[str, object]:
    local_symbol = f"SPY   260821C{int(float(strike) * 1000):08d}"
    return {
        "conId": contract_id,
        "localSymbol": local_symbol,
        "tradingClass": "SPY",
        "multiplier": 100,
        "exchange": "SMART",
        "expiry": "2026-08-21",
        "strike": strike,
        "right": "C",
    }


def _payload() -> dict[str, object]:
    first = _identity(101, "650")
    second = _identity(102, "655")
    return {
        "purpose": OPEN_REPRICE_PURPOSE,
        "batch_id": "external-batch-20260806-132000",
        "requested_at": REQUESTED_AT,
        "completed_at": COMPLETED_AT,
        "account": {
            "asof": COMPLETED_AT,
            "currency": "USD",
            "net_liquidation": "10000.00",
            "equity_with_loan_value": "10000.00",
            "available_funds": "9000.00",
            "buying_power": "18000.00",
            "initial_margin": "500.00",
            "maintenance_margin": "400.00",
            "excess_liquidity": "8600.00",
            "day_trades_remaining": 3,
            "connected": True,
        },
        "nav": {
            "asof": COMPLETED_AT,
            "currency": "USD",
            "strategy_nav": "10000.00",
            "source": "IBKR_EXTERNAL_READONLY",
        },
        "positions": [
            {
                "asof": COMPLETED_AT,
                "contract_id": 101,
                "symbol": "SPY",
                "local_symbol": first["localSymbol"],
                "security_type": "OPT",
                "currency": "USD",
                "exchange": "SMART",
                "quantity": "1",
                "average_cost": "250.00",
                "market_price": "2.60",
                "market_value": "260.00",
                "unrealized_pnl": "10.00",
                "realized_pnl": "0.00",
                "identity": first,
            }
        ],
        "working_orders": [],
        "unsubmitted_instructions": [],
        "secdefs": [
            {
                "batch_id": "external-batch-20260806-132000",
                "requested_at": REQUESTED_AT,
                "observed_at": COMPLETED_AT,
                "completed_at": COMPLETED_AT,
                "identity": first,
                "security_type": "OPT",
                "currency": "USD",
                "standard_contract": True,
                "adjusted": False,
                "source": "IBKR_EXTERNAL_READONLY",
            },
            {
                "batch_id": "external-batch-20260806-132000",
                "requested_at": REQUESTED_AT,
                "observed_at": COMPLETED_AT,
                "completed_at": COMPLETED_AT,
                "identity": second,
                "security_type": "OPT",
                "currency": "USD",
                "standard_contract": True,
                "adjusted": False,
                "source": "IBKR_EXTERNAL_READONLY",
            },
        ],
        "quotes": [
            _quote(first, "request-101", "2.50", "2.60"),
            _quote(second, "request-102", "1.40", "1.50"),
        ],
    }


def _quote(
    identity: dict[str, object], request_id: str, bid: str, ask: str
) -> dict[str, object]:
    return {
        "batch_id": "external-batch-20260806-132000",
        "request_id": request_id,
        "requested_at": REQUESTED_AT,
        "observed_at": COMPLETED_AT,
        "completed_at": COMPLETED_AT,
        "identity": identity,
        "source": "IBKR_EXTERNAL_READONLY",
        "bid": bid,
        "ask": ask,
        "last": bid,
        "close": bid,
        "volume": 120,
        "open_interest": 500,
        "implied_volatility": "0.22",
        "delta": "0.45",
        "gamma": "0.03",
        "theta": "-0.08",
        "vega": "0.10",
        "market_data_type": 1,
    }


def _publish(path: Path, payload: dict[str, object] | None = None):
    publisher = ExternalReadonlyFeedPublisher(path, clock=lambda: WRITTEN_AT)
    return publisher.publish(_payload() if payload is None else payload)


def _reader(path: Path, *, now: datetime | None = None):
    return ExternalReadonlyFeedReader(
        path,
        clock=lambda: now or (WRITTEN_AT + timedelta(seconds=1)),
    )


def _rewrite_with_valid_hash(path: Path, mutator) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    mutator(document)
    unsigned = dict(document)
    unsigned.pop("content_hash", None)
    document["content_hash"] = canonical_hash(unsigned)
    path.write_text(canonical_json(document) + "\n", encoding="utf-8")


def test_publish_and_read_canonical_fresh_batch_projects_read_only_adapter(
    tmp_path: Path,
) -> None:
    path = tmp_path / "external-ibkr.json"

    published = _publish(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    unsigned = dict(raw)
    content_hash = unsigned.pop("content_hash")

    assert raw["schema"] == EXTERNAL_READONLY_FEED_SCHEMA
    assert raw["version"] == EXTERNAL_READONLY_FEED_VERSION
    assert raw["written_at"] == WRITTEN_AT.isoformat(timespec="microseconds")
    assert content_hash == canonical_hash(unsigned)
    assert path.read_text(encoding="utf-8") == canonical_json(raw) + "\n"

    batch = _reader(path).read()
    assert batch == published
    assert batch.verify_hash() is True
    assert batch.purpose == OPEN_REPRICE_PURPOSE
    assert batch.batch_id == "external-batch-20260806-132000"
    assert batch.account_snapshot().net_liquidation.as_tuple().exponent == -2
    assert batch.strategy_nav()["strategy_nav"] == "10000.00"
    assert len(batch.positions()) == 1
    assert batch.working_orders() == ()
    assert batch.unsubmitted_instructions() == ()

    contracts = (
        OptionContractRef(
            contract_id=101,
            contract_id_ex="101",
            symbol="SPY",
            local_symbol=str(_identity(101, "650")["localSymbol"]),
            expiration=datetime(2026, 8, 21, tzinfo=UTC).date(),
            strike=650,
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
        OptionContractRef(
            contract_id=102,
            contract_id_ex="102",
            symbol="SPY",
            local_symbol=str(_identity(102, "655")["localSymbol"]),
            expiration=datetime(2026, 8, 21, tzinfo=UTC).date(),
            strike=655,
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
    )
    assert [row.contract_id for row in batch.option_contract_definitions(contracts)] == [
        101,
        102,
    ]
    quote_batch = batch.option_quote_batch(contracts)
    assert quote_batch.status is QuoteBatchStatus.COMPLETE
    assert [row.contract_id for row in quote_batch.quotes] == [101, 102]
    atomic = BrokerSnapshotBuilder(
        batch,
        clock=lambda: WRITTEN_AT + timedelta(seconds=1),
    ).build(contracts)
    assert atomic.status is BrokerSnapshotStatus.COMPLETE
    assert atomic.verify_hash() is True

    public_names = {
        name
        for cls in (ExternalReadonlyFeedPublisher, ExternalReadonlyFeedReader)
        for name in dir(cls)
        if not name.startswith("_")
    }
    assert not public_names.intersection(
        {"create_order", "place_order", "submit_order", "cancel_order", "modify_order"}
    )


def test_premarket_account_batch_is_fresh_and_has_no_fake_option_quotes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "external-ibkr-premarket.json"
    payload = _payload()
    payload["purpose"] = PREMARKET_ACCOUNT_PURPOSE
    payload["secdefs"] = []
    payload["quotes"] = []

    published = _publish(path, payload)
    batch = _reader(path).read()

    assert batch == published
    assert batch.purpose == PREMARKET_ACCOUNT_PURPOSE
    assert batch.working_orders() == ()
    assert batch.unsubmitted_instructions() == ()
    assert batch.secdef_rows == ()
    assert batch.quote_rows == ()


def test_premarket_account_batch_rejects_quote_evidence(tmp_path: Path) -> None:
    path = tmp_path / "external-ibkr-premarket.json"
    payload = _payload()
    payload["purpose"] = PREMARKET_ACCOUNT_PURPOSE

    with pytest.raises(ExternalFeedValidationError, match="cannot contain"):
        _publish(path, payload)


def test_publisher_uses_same_directory_temp_file_and_os_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "external-ibkr.json"
    _publish(path)
    before = path.read_bytes()
    calls: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def capture_replace(source, destination) -> None:
        calls.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr(
        "options_copilot.gateway.external_readonly_feed.os.replace",
        capture_replace,
    )
    payload = _payload()
    payload["batch_id"] = "external-batch-20260806-132001"
    for collection in (payload["secdefs"], payload["quotes"]):
        for row in collection:
            row["batch_id"] = payload["batch_id"]
    _publish(path, payload)

    assert len(calls) == 1
    temporary, destination = calls[0]
    assert temporary.parent == path.parent
    assert destination == path
    assert temporary != path
    assert not temporary.exists()
    assert path.read_bytes() != before


@pytest.mark.parametrize(
    ("mutator", "match"),
    [
        (
            lambda document: document.__setitem__("schema", "unsupported.schema"),
            "schema",
        ),
        (lambda document: document.__setitem__("version", 2), "version"),
        (lambda document: document.pop("nav"), "top-level fields"),
        (
            lambda document: document.__setitem__("working_orders", None),
            "working_orders must be a known array",
        ),
        (
            lambda document: document.__setitem__("unsubmitted_instructions", None),
            "unsubmitted_instructions must be a known array",
        ),
        (
            lambda document: document["quotes"][1]["identity"].pop("right"),
            "identity fields",
        ),
        (
            lambda document: document["secdefs"][1].__setitem__(
                "identity", document["secdefs"][0]["identity"]
            ),
            "duplicate contract",
        ),
        (
            lambda document: document["quotes"][1].__setitem__(
                "batch_id", "other-batch"
            ),
            "mixed batch",
        ),
        (
            lambda document: document["quotes"][1].__setitem__(
                "observed_at", "2026-08-06T13:20:00.500000+00:00"
            ),
            "mixed timestamp",
        ),
        (
            lambda document: document["quotes"][1].__setitem__(
                "open_interest", None
            ),
            "open_interest",
        ),
    ],
)
def test_reader_rejects_partial_unknown_duplicate_or_mixed_batches(
    tmp_path: Path, mutator, match: str
) -> None:
    path = tmp_path / "external-ibkr.json"
    _publish(path)
    _rewrite_with_valid_hash(path, mutator)

    with pytest.raises(ExternalFeedValidationError, match=match):
        _reader(path).read()


def test_reader_rejects_tampering_before_semantic_projection(tmp_path: Path) -> None:
    path = tmp_path / "external-ibkr.json"
    _publish(path)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["account"]["net_liquidation"] = "999999.00"
    path.write_text(canonical_json(document) + "\n", encoding="utf-8")

    with pytest.raises(ExternalFeedValidationError, match="content hash"):
        _reader(path).read()


def test_reader_rejects_batch_older_than_five_seconds(tmp_path: Path) -> None:
    path = tmp_path / "external-ibkr.json"
    _publish(path)

    with pytest.raises(ExternalFeedValidationError, match="older than five seconds"):
        _reader(path, now=COMPLETED_AT + timedelta(seconds=5, microseconds=1)).read()


def test_publisher_rejects_unknown_or_partial_payload_without_replacing_last_good(
    tmp_path: Path,
) -> None:
    path = tmp_path / "external-ibkr.json"
    _publish(path)
    before = path.read_bytes()
    invalid = _payload()
    invalid["account"] = None

    with pytest.raises(ExternalFeedValidationError, match="account"):
        _publish(path, invalid)

    assert path.read_bytes() == before
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))
