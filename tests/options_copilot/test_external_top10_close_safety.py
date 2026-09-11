from __future__ import annotations

from options_copilot.runtime import ExternalTop10Composition


class _Loop:
    def __init__(self) -> None:
        self.can_close = False
        self.close_calls = 0
        self.start_calls = 0

    def start(self) -> None:
        self.start_calls += 1

    def close(self) -> bool:
        self.close_calls += 1
        return self.can_close

    def health(self) -> dict[str, object]:
        return {"status": "READY"}


class _Store:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_external_top10_close_keeps_store_open_until_worker_exits() -> None:
    loop = _Loop()
    store = _Store()
    composition = ExternalTop10Composition(
        producer=object(),  # type: ignore[arg-type]
        scheduler_service=object(),  # type: ignore[arg-type]
        scheduler_loop=loop,  # type: ignore[arg-type]
        calendar_provider=object(),  # type: ignore[arg-type]
        bundle_guard=object(),  # type: ignore[arg-type]
        scan_store=store,  # type: ignore[arg-type]
    )

    assert composition.close() is False
    assert store.close_calls == 0
    assert composition._closed is False

    loop.can_close = True
    assert composition.close() is True
    assert store.close_calls == 1
    assert composition._closed is True
    assert composition.close() is True
    assert store.close_calls == 1
