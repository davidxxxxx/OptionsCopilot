"""Production cache consumption remains observable and non-authoritative."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

import options_copilot.production_runtime as production
from options_copilot.feature_source_resolution import FeatureSourceResolver
from options_copilot.gateway import (
    BatchedOptionQuote,
    BrokerSnapshotBuilder,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
    UnderlyingIvHistory,
    UnderlyingIvHistoryPoint,
)
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.evidence import EvidenceStore
from options_copilot.storage.feature_sources import FeatureSourceObservationStore


NOW = datetime(2026, 9, 8, 15, 0, tzinfo=timezone.utc)
EXPIRY = date(2026, 9, 25)


def _binding(**request):
    return FeatureSourceResolver(SimpleNamespace(read=lambda **kwargs: ())).resolve(**request)


def _history(symbol, *, end_at):
    basis = {
        "symbol": symbol, "contract_id": 756733, "security_type": "STK",
        "request_exchange": "SMART", "currency": "USD",
    }
    value = UnderlyingIvHistory(
        symbol=symbol, contract_id=756733, request_exchange="SMART", currency="USD",
        observed_at=NOW, end_at=end_at, duration="30 D", bar_size="1 day",
        what_to_show="OPTION_IMPLIED_VOLATILITY", use_rth=True,
        points=tuple(
            UnderlyingIvHistoryPoint(NOW.date() - timedelta(days=offset), Decimal("0.20"))
            for offset in range(15, 0, -1)
        ),
        basis_hash=canonical_hash(basis), content_hash="",
    )
    return replace(value, content_hash=canonical_hash(value.hash_payload()))


@pytest.fixture
def lane(tmp_path):
    stores = []

    def build(resolver=None, *, expirations=(EXPIRY, EXPIRY)):
        calls = []
        contracts = tuple(
            OptionContractRef(
                contract_id=101 + index, contract_id_ex=f"{101 + index}@SMART",
                symbol="SPY", local_symbol=f"SPY-{index}-C", expiration=expiry,
                strike=Decimal(100 + index * 5), right="C", exchange="SMART",
                trading_class="SPY", multiplier=100,
            )
            for index, expiry in enumerate(expirations)
        )

        class Source:
            def underlying_iv_history(self, symbol, *, end_at):
                calls.append("history")
                return _history(symbol, end_at=end_at)

            def account_snapshot(self):
                return {"currency": "USD", "net_liquidation": Decimal("2500")}

            def positions(self):
                return ()

            def working_orders(self):
                return ()

            def unsubmitted_instructions(self):
                return ()

            def option_contract_definitions(self, requested):
                return tuple(
                    OptionSecDefSnapshot(
                        item.contract_id, item.local_symbol, item.trading_class,
                        item.multiplier, item.exchange, item.expiration,
                        item.strike, item.right, "OPT", "USD", True, False, "IBKR",
                    )
                    for item in requested
                )

            def option_quote_batch(self, requested):
                calls.append("quotes")
                quotes = tuple(
                    BatchedOptionQuote(
                        contract_id=item.contract_id, batch_id="binding-batch",
                        request_id=f"request-{item.contract_id}",
                        requested_at=NOW - timedelta(seconds=2),
                        observed_at=NOW - timedelta(seconds=1), completed_at=NOW,
                        source="IBKR", bid=Decimal("1"), ask=Decimal("1.10"),
                        exchange_time=NOW - timedelta(seconds=1),
                        implied_volatility=Decimal("0.20"), volume=100,
                        open_interest=1000, delta=Decimal("0.50"),
                        gamma=Decimal("0.02"), theta=Decimal("-0.08"),
                        vega=Decimal("0.11"), market_data_type=1,
                    )
                    for item in requested
                )
                return OptionQuoteBatch(
                    "binding-batch", QuoteBatchStatus.COMPLETE,
                    NOW - timedelta(seconds=2), NOW, "IBKR", quotes,
                )

        source = Source()
        store = EvidenceStore(tmp_path / f"evidence-{len(stores)}.sqlite3")
        stores.append(store)
        acquisition = production.ProductionBrokerEvidenceAcquisition(
            BrokerSnapshotBuilder(source, clock=lambda: NOW),
            SimpleNamespace(resolve_contracts=lambda **kwargs: contracts),
            store, SimpleNamespace(ready=True),
            SimpleNamespace(guard_current=lambda nav, *, callback: callback()),
            execution_cost_contract={},
            policy_resolver=SimpleNamespace(policy_contract_document=lambda _: {}),
            feature_source_resolver=resolver, clock=lambda: NOW,
        )
        nav = SimpleNamespace(
            content_hash="a" * 64, authority_hash="b" * 64, ledger_head_hash="c" * 64,
        )
        acquisition._strategy_nav_for_snapshot = lambda snapshot: (nav, ())
        return acquisition, source, store, calls

    yield build
    for store in stores:
        store.close()


def _acquire(acquisition):
    return acquisition.acquire(scan_run_id="scan-binding", universe={}, context={})


def test_actual_acquire_resolves_underlying_once_per_expiry_before_quotes(lane):
    observed = []
    calls = []

    def resolve(**request):
        calls.append("resolve")
        observed.append(request)
        return _binding(**request)

    acquisition, _, store, events = lane(
        SimpleNamespace(resolve=resolve),
        expirations=(EXPIRY, EXPIRY, date(2026, 10, 2)),
    )
    calls = events
    result = _acquire(acquisition)

    assert [(row["symbol"], row["con_id"], row["expiration"], row["cutoff"]) for row in observed] == [
        ("SPY", 756733, EXPIRY, NOW), ("SPY", 756733, date(2026, 10, 2), NOW),
    ]
    assert calls[:4] == ["history", "resolve", "resolve", "quotes"]
    report = result["feature_source_bindings"]
    assert report["scan_run_id"] == "scan-binding"
    assert report["cutoff"] == NOW.isoformat()
    assert len(report["bindings"]) == 2
    assert report["decision_authority"] == "OBSERVATION_ONLY"
    assert report["model_input_complete"] is False
    assert report["production_eligible"] is False
    assert result["atm_iv"] == Decimal("0.20")
    assert "market_score" not in result and "volatility_score" not in result
    persisted = store.query(kinds=("BROKER_SNAPSHOT",), limit=1)[0].record.payload
    assert persisted["feature_source_bindings"]["bindings"][0]["content_hash"] == report["bindings"][0]["content_hash"]
    assert store.verify_integrity() is True


@pytest.mark.parametrize("failure", ["snapshot", "reconstruction", "history", "nav", "nav_guard", "nav_changed"])
def test_bindings_survive_every_post_resolution_early_return(lane, monkeypatch, failure):
    acquisition, _, _, _ = lane(SimpleNamespace(resolve=lambda **kwargs: _binding(**kwargs)))
    if failure == "snapshot":
        monkeypatch.setattr(acquisition.broker_snapshot_builder, "build", lambda _: SimpleNamespace(reason_codes=("BBO_UNAVAILABLE",)))
    elif failure == "reconstruction":
        monkeypatch.setattr(production, "_secdefs_from_snapshot", lambda _: ())
    elif failure == "history":
        monkeypatch.setattr(production, "_validate_underlying_iv_history", lambda *args, **kwargs: ("IV_HISTORY_TEST_INVALID",))
    elif failure == "nav":
        acquisition._strategy_nav_for_snapshot = lambda _: (None, ("NAV_UNAVAILABLE",))
    elif failure == "nav_guard":
        acquisition.strategy_nav_source = object()
    else:
        acquisition.strategy_nav_source = SimpleNamespace(guard_current=lambda *args, **kwargs: None)
    result = _acquire(acquisition)
    assert result["reasons"]
    assert len(result["feature_source_bindings"]["bindings"]) == 1
    assert acquisition.feature_source_bindings() == result["feature_source_bindings"]


@pytest.mark.parametrize("bad", [None, [], {"order_allowed": True}, "secret-provider-response"])
def test_invalid_resolver_result_is_bounded_and_never_grants_authority(lane, bad):
    acquisition, _, _, _ = lane(SimpleNamespace(resolve=lambda **kwargs: bad))
    result = _acquire(acquisition)
    report = result["feature_source_bindings"]
    assert report["bindings"] == ()
    assert "FEATURE_SOURCE_BINDING_INVALID" in report["reason_codes"]
    assert report["model_input_complete"] is False
    assert "secret-provider-response" not in repr(report)
    assert result["reasons"] == ()


def test_resolver_exception_is_redacted_and_read_model_is_detached(lane):
    def fail(**kwargs):
        raise RuntimeError("credential=do-not-publish")

    acquisition, _, _, calls = lane(SimpleNamespace(resolve=fail))
    result = _acquire(acquisition)
    assert "FEATURE_SOURCE_RESOLVER_FAILED" in result["feature_source_bindings"]["reason_codes"]
    assert "do-not-publish" not in repr(result)
    before = tuple(calls)
    snapshot = acquisition.feature_source_bindings()
    snapshot["reason_codes"] = ("changed",)
    assert acquisition.feature_source_bindings()["reason_codes"] != ("changed",)
    assert tuple(calls) == before


def test_missing_resolver_is_explicit_even_before_acquisition(lane):
    acquisition, _, _, _ = lane()
    report = acquisition.feature_source_bindings()
    assert report["status"] == "WIRED_NOT_RUN"
    assert report["reason_codes"] == ("FEATURE_SOURCE_RESOLVER_UNWIRED",)
    assert _acquire(acquisition)["feature_source_bindings"]["status"] == "WIRED_NOT_RUN"


@pytest.mark.parametrize("mutation", [
    {"model_input_complete": True},
    {"production_eligible": True},
    {"market_score": "60"},
    {"volatility_score": "20"},
    {"decision_authority": "PRODUCTION"},
    {"con_id": True},
    {"symbol": "QQQ"},
    {"order_allowed": True},
    {"reason_codes": []},
])
def test_rehashed_resolver_claims_cannot_change_model_authority(lane, mutation):
    def resolve(**request):
        result = _binding(**request)
        result.update(mutation)
        result.pop("content_hash")
        result["content_hash"] = canonical_hash(result)
        return result

    acquisition, _, _, _ = lane(SimpleNamespace(resolve=resolve))
    report = _acquire(acquisition)["feature_source_bindings"]
    assert report["bindings"] == ()
    assert "FEATURE_SOURCE_BINDING_INVALID" in report["reason_codes"]
    assert report["model_input_complete"] is False
    assert report["production_eligible"] is False


@pytest.mark.parametrize("resolver", [object(), SimpleNamespace(resolve=1)])
def test_invalid_resolver_type_is_observation_only_failure(lane, resolver):
    acquisition, _, _, _ = lane(resolver)
    result = _acquire(acquisition)
    assert "FEATURE_SOURCE_RESOLVER_INVALID" in result["feature_source_bindings"]["reason_codes"]
    assert result["reasons"] == ()


def test_binding_read_model_is_deeply_detached_and_does_not_resolve(lane):
    seen = []

    def resolve(**kwargs):
        seen.append(kwargs)
        return _binding(**kwargs)

    acquisition, _, _, _ = lane(SimpleNamespace(resolve=resolve))
    _acquire(acquisition)
    view = acquisition.feature_source_bindings()
    view["bindings"][0]["sources"]["injected"] = {"order_allowed": True}
    assert "injected" not in acquisition.feature_source_bindings()["bindings"][0]["sources"]
    assert len(seen) == 1


def test_new_scan_early_failure_does_not_reuse_previous_scan_binding(lane):
    acquisition, _, _, _ = lane(SimpleNamespace(resolve=lambda **kwargs: _binding(**kwargs)))
    _acquire(acquisition)
    acquisition.pacing.ready = False
    result = acquisition.acquire(scan_run_id="scan-next", universe={}, context={})
    report = result["feature_source_bindings"]
    assert report["scan_run_id"] == "scan-next"
    assert report["cutoff"] is None
    assert report["bindings"] == ()
    assert report["status"] == "WIRED_NOT_RUN"


def test_invalid_history_identity_never_reaches_cache_resolver(lane):
    def resolve(**kwargs):
        pytest.fail("invalid broker identity must not address source cache")

    acquisition, source, _, _ = lane(SimpleNamespace(resolve=resolve))
    def invalid_history(symbol, *, end_at):
        history = replace(
            _history(symbol, end_at=end_at), contract_id=True,
            basis_hash=canonical_hash({
                "symbol": symbol, "contract_id": True, "security_type": "STK",
                "request_exchange": "SMART", "currency": "USD",
            }),
        )
        return replace(history, content_hash=canonical_hash(history.hash_payload()))

    source.underlying_iv_history = invalid_history
    result = _acquire(acquisition)
    assert result["feature_source_bindings"]["bindings"] == ()
    assert "FEATURE_SOURCE_HISTORY_IDENTITY_INVALID" in result["feature_source_bindings"]["reason_codes"]


@pytest.mark.parametrize("quote_failure", [False, True])
def test_reopened_observation_ledger_reaches_actual_acquire_and_keeps_exact_refs(
    lane, tmp_path, monkeypatch, quote_failure,
):
    from tests.options_copilot.test_feature_sources_diagnostic import _source

    path = tmp_path / "persisted-source-observations.sqlite3"
    source_cutoff = NOW - timedelta(seconds=10)
    expected = {}
    with FeatureSourceObservationStore(path, clock=lambda: NOW - timedelta(seconds=1)) as original:
        for kind in ("PRICE_HISTORY", "IV_HISTORY", "CURRENT_IV"):
            source = _source(kind, "SPY", source_cutoff)
            source["requested_at"] = source_cutoff.isoformat()
            source["available_at"] = (NOW - timedelta(seconds=2)).isoformat()
            if kind == "CURRENT_IV":
                source["received_at"] = (NOW - timedelta(seconds=3)).isoformat()
            source.pop("content_hash")
            source["content_hash"] = canonical_hash(source)
            expected[kind] = original.append(
                source, operation_id="diagnostic-persisted-binding", cutoff=source_cutoff,
            )

    with FeatureSourceObservationStore(path, clock=lambda: NOW) as reopened:
        acquisition, broker_source, evidence_store, _ = lane(FeatureSourceResolver(reopened))
        if quote_failure:
            monkeypatch.setattr(
                broker_source, "option_quote_batch",
                lambda _: OptionQuoteBatch(
                    "binding-batch", QuoteBatchStatus.TIMEOUT,
                    NOW - timedelta(seconds=2), NOW, "IBKR", (),
                ),
            )
        result = _acquire(acquisition)
        report = result["feature_source_bindings"]
        assert len(report["bindings"]) == 1
        binding = report["bindings"][0]
        assert binding["con_id"] == 756733
        assert binding["expiration"] == EXPIRY.isoformat()
        assert set(binding["sources"]) == set(expected)
        for kind, reference in binding["sources"].items():
            for key in ("observation_id", "sequence", "row_hash", "source_hash", "request_hash", "basis_hash", "first_seen_at"):
                assert reference[key] == expected[kind][key]
            assert reference["available_at"] == (NOW - timedelta(seconds=2)).isoformat()
            assert reference["source_cutoff_at"] == source_cutoff.isoformat()
        assert binding["model_input_complete"] is False
        assert binding["production_eligible"] is False
        assert "IV_BASIS_UNRESOLVED" in binding["reason_codes"]
        assert acquisition.feature_source_bindings() == report
        durable = evidence_store.query(kinds=("BROKER_SNAPSHOT",), limit=1)
        if quote_failure:
            assert result["reasons"]
            assert durable == ()
        else:
            assert result["reasons"] == ()
            persisted = durable[0].record.payload["feature_source_bindings"]["bindings"][0]
            assert persisted["content_hash"] == binding["content_hash"]
            for kind in expected:
                assert persisted["sources"][kind]["row_hash"] == expected[kind]["row_hash"]
                assert persisted["sources"][kind]["source_hash"] == expected[kind]["source_hash"]
        assert reopened.status()["observation_count"] == 3
