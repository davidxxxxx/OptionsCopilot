from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from urllib.error import HTTPError

import pytest

from options_copilot.operations.cloud_jin10_research import (
    CloudJin10ResearchError,
    EVIDENCE_FILENAME,
    NETWORK_SECRET_NAME,
    SUMMARY_FILENAME,
    NetworkEnvironmentSecretReader,
    _run_cloud_jin10_research,
    run_cloud_jin10_research,
)
import options_copilot.operations.cloud_jin10_research as cloud_research_module
from options_copilot.providers.jin10_mcp import JIN10_MCP_URL


NOW = datetime(2026, 10, 6, 6, 15, tzinfo=timezone.utc)


class _Headers(dict):
    def get(self, key, default=None):
        wanted = str(key).lower()
        for name, value in self.items():
            if str(name).lower() == wanted:
                return value
        return default


class _Response:
    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 200,
        content_type: str = "application/json",
        session_id: str | None = None,
    ) -> None:
        self.status = status
        self.headers = _Headers({
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "Content-Encoding": "identity",
            **({"Mcp-Session-Id": session_id} if session_id else {}),
        })
        self._body = BytesIO(body)

    def geturl(self) -> str:
        return JIN10_MCP_URL

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def readline(self, size: int = -1) -> bytes:
        return self._body.readline(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _FlashOpener:
    def __init__(self) -> None:
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        assert request.full_url == JIN10_MCP_URL
        if request.get_method() == "DELETE":
            return _Response(status=204)
        payload = json.loads(request.data.decode("utf-8"))
        method = payload["method"]
        if method == "initialize":
            result = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                },
            }
            return _Response(json.dumps(result).encode(), session_id="cloud-test-session")
        if method == "notifications/initialized":
            return _Response(status=202)
        if method == "tools/list":
            result = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "tools": [{
                        "name": "list_flash",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"cursor": {"type": "string"}},
                            "additionalProperties": False,
                        },
                    }]
                },
            }
            return _Response(json.dumps(result).encode())
        assert method == "tools/call"
        assert payload["params"] == {"name": "list_flash", "arguments": {}}
        data = {
            "status": 200,
            "message": "success",
            "data": {
                "items": [{
                    "content": "公开快讯测试记录",
                    "time": "2026-10-06T14:14:00+08:00",
                    "url": "https://flash.jin10.com/detail/test-record",
                }],
                "has_more": True,
                "next_cursor": "must-not-be-followed",
            },
        }
        result = {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": {
                "content": [{"type": "text", "text": json.dumps(data)}],
                "isError": False,
            },
        }
        return _Response(json.dumps(result).encode())


def test_network_reader_never_aliases_local_or_dpapi_secret_name() -> None:
    reader = NetworkEnvironmentSecretReader({NETWORK_SECRET_NAME: "network-placeholder"})

    assert reader.get(NETWORK_SECRET_NAME) == "network-placeholder"
    assert reader.get("JIN10_MCP_TOKEN") is None


def test_one_shot_cloud_chain_reopens_and_verifies_store(tmp_path: Path) -> None:
    opener = _FlashOpener()
    output = tmp_path / "one-shot"

    result = _run_cloud_jin10_research(
        output,
        environment={NETWORK_SECRET_NAME: "network-placeholder"},
        opener=opener,
        clock=lambda: NOW,
        symbols=("SPY", "QQQ"),
        transport_mode="TEST_INJECTED",
    )

    assert result["status"] == "TEST_PASS"
    assert result["transport"] == {
        "mode": "TEST_INJECTED",
        "live_default_dependencies": False,
        "test_injection": True,
    }
    assert result["session_cleanup"] == {
        "session_allocated": True,
        "delete_attempted": True,
        "result": "CONFIRMED",
    }
    assert result["coverage"] == {
        "status": "BOUNDED_FIRST_PAGE_ONLY",
        "complete_24h": False,
        "next_cursor_followed": False,
    }
    assert result["authority"] == {
        "decision_authority": "SUPPORTING_ONLY",
        "production_eligible": False,
        "account_accessed": False,
        "ibkr_accessed": False,
        "order_creation_allowed": False,
        "order_submission_allowed": False,
    }
    store = result["store"]
    assert store["record_count"] == 1
    assert store["reopened"] is True
    assert store["integrity"] == "PASS"
    record = store["records"][0]
    assert record["published_at"] == "2026-10-06T06:14:00+00:00"
    assert record["received_at"] == NOW.isoformat()
    assert record["decision_authority"] == "SUPPORTING_ONLY"
    assert len(record["content_hash"]) == 64
    assert len(record["row_hash"]) == 64
    assert (output / EVIDENCE_FILENAME).is_file()
    assert (output / "jin10_cloud_news_evidence.news-analysis.sqlite3").is_file()
    assert (output / "weekly_briefs.sqlite3").is_file()
    assert json.loads((output / SUMMARY_FILENAME).read_text())["canonical_sha256"] == result[
        "canonical_sha256"
    ]
    methods = [request.get_method() for request, _timeout in opener.requests]
    assert methods == ["POST", "POST", "POST", "POST", "DELETE"]
    calls = [
        json.loads(request.data.decode("utf-8"))
        for request, _timeout in opener.requests
        if request.get_method() == "POST"
    ]
    assert [call["params"].get("name") for call in calls if call["method"] == "tools/call"] == [
        "list_flash"
    ]


def test_cloud_chain_surfaces_authentication_failure_without_secret(tmp_path: Path) -> None:
    secret = "network-placeholder-must-not-leak"

    class Opener:
        calls = 0

        @classmethod
        def open(cls, request, *, timeout):
            cls.calls += 1
            raise HTTPError(request.full_url, 401, secret, {}, None)

    with pytest.raises(CloudJin10ResearchError) as captured:
        _run_cloud_jin10_research(
            tmp_path / "auth-failure",
            environment={NETWORK_SECRET_NAME: secret},
            opener=Opener(),
            clock=lambda: NOW,
            symbols=("SPY",),
            transport_mode="TEST_INJECTED",
        )

    assert captured.value.reason == "AUTHENTICATION_FAILED"
    assert secret not in str(captured.value)
    assert Opener.calls == 1


def test_cloud_chain_fails_when_session_cleanup_is_not_confirmed(tmp_path: Path) -> None:
    class CleanupFailureOpener(_FlashOpener):
        def open(self, request, *, timeout):
            if request.get_method() == "DELETE":
                self.requests.append((request, timeout))
                return _Response(status=500)
            return super().open(request, timeout=timeout)

    with pytest.raises(CloudJin10ResearchError) as captured:
        _run_cloud_jin10_research(
            tmp_path / "cleanup-failure",
            environment={NETWORK_SECRET_NAME: "network-placeholder"},
            opener=CleanupFailureOpener(),
            clock=lambda: NOW,
            symbols=("SPY",),
            transport_mode="TEST_INJECTED",
        )

    assert captured.value.reason == "SESSION_CLEANUP_FAILED"


def test_live_entrypoint_is_not_dependency_injectable(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    def fake_run(output_dir, **kwargs):
        captured.update(kwargs)
        return {"status": "PASS"}

    monkeypatch.setattr(cloud_research_module, "_run_cloud_jin10_research", fake_run)

    assert run_cloud_jin10_research(tmp_path / "unused") == {"status": "PASS"}
    assert captured == {
        "environment": None,
        "opener": None,
        "clock": None,
        "symbols": cloud_research_module.DEFAULT_NEWS_CORE_SYMBOLS,
        "transport_mode": "LIVE_DEFAULT",
    }


def test_end_to_end_deadline_interrupts_before_live_work_can_continue(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import time

    def slow_run(*_args, **_kwargs):
        time.sleep(0.2)
        raise AssertionError("deadline did not interrupt live work")

    monkeypatch.setattr(cloud_research_module, "_run_cloud_jin10_research", slow_run)
    monkeypatch.setattr(cloud_research_module, "TOTAL_TIMEOUT_SECONDS", 0.02)

    with pytest.raises(CloudJin10ResearchError) as captured:
        run_cloud_jin10_research(tmp_path / "deadline")

    assert captured.value.reason == "TOTAL_TIMEOUT"


def test_end_to_end_deadline_cannot_be_swallowed_by_exception_boundaries(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import time

    continued = False

    def swallowing_run(*_args, **_kwargs):
        nonlocal continued
        try:
            time.sleep(0.2)
        except Exception:
            continued = True
        return {"status": "PASS_AFTER_SWALLOWED_TIMEOUT"}

    monkeypatch.setattr(
        cloud_research_module,
        "_run_cloud_jin10_research",
        swallowing_run,
    )
    monkeypatch.setattr(cloud_research_module, "TOTAL_TIMEOUT_SECONDS", 0.02)

    with pytest.raises(CloudJin10ResearchError) as captured:
        run_cloud_jin10_research(tmp_path / "deadline-not-swallowed")

    assert captured.value.reason == "TOTAL_TIMEOUT"
    assert continued is False


def test_live_transport_mode_rejects_dependency_injection(tmp_path: Path) -> None:
    with pytest.raises(CloudJin10ResearchError) as captured:
        _run_cloud_jin10_research(
            tmp_path / "forbidden",
            environment={NETWORK_SECRET_NAME: "fixture"},
            transport_mode="LIVE_DEFAULT",
        )

    assert captured.value.reason == "LIVE_DEPENDENCY_INJECTION_FORBIDDEN"
