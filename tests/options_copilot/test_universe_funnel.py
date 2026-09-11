from __future__ import annotations

from datetime import datetime, timezone

from options_copilot.operations.capabilities import MarketDataPacingCapability, PACING_REQUEST_CLASSES
from options_copilot.scanner.pacing import RequestBudgetByClass
from options_copilot.scanner.universe import UniverseFunnel


def _pacing():
    now = datetime(2026, 8, 4, tzinfo=timezone.utc)
    capability = MarketDataPacingCapability.create(
        version="P0", observed_at=now, source="broker_disclosed", signer="human",
        request_classes={name: {"max_concurrency": 1, "request_window": 1, "max_requests": 10, "cooldown": 1} for name in PACING_REQUEST_CLASSES},
    )
    return RequestBudgetByClass(capability, now=now, expected_version="P0")


def test_open_gld_is_management_only_and_zero_entry_finalists():
    result = UniverseFunnel(_pacing()).run(
        positions=({"symbol": "GLD", "position": 1},), core_etfs=({"symbol": "SPY", "score": 2},),
        event_pool=(), scanner=(), coarse_contracts=({"symbol": "SPY", "dte": 14},),
    )
    assert result.mode == "POSITION_MANAGEMENT_ONLY"
    assert result.finalists == ()


def test_dte_boundaries_and_top_ten_action_limit():
    pacing = _pacing()
    result = UniverseFunnel(pacing).run(
        positions=(), core_etfs=(), event_pool=(), scanner=(),
        coarse_contracts=tuple({"symbol": "SPY", "dte": dte, "score": dte} for dte in (6, 7, 13, 14, 35, 36, 20, 21)),
        signed_dte_exception=True,
    )
    assert len(result.finalists) == 6
    assert any(item.reason_code == "DTE_BELOW_PERMANENT_FLOOR" for item in result.evidence)
    assert any(item.reason_code == "DTE_ABOVE_NORMAL_MAXIMUM" for item in result.evidence)
    assert pacing.usage()["secdef"] == {"used": 0, "limit": 10}
