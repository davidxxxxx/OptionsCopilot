from __future__ import annotations

import json
from pathlib import Path

from options_copilot.operations.authority_audit import audit_repository, main


def _package(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "options_copilot"
    root.mkdir()
    (root / "__init__.py").write_text("", encoding="utf-8")
    for relative, source in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    return root


def test_review_only_protocol_and_false_result_fields_are_allowed(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "review.py": """
from typing import Protocol

class ReviewInstructionCreator(Protocol):
    def create_review_instruction(self, *, review_only: bool): ...

def validate_result(result):
    expected = {
        "review_only": True,
        "order_submitted": False,
        "transmitted_to_broker": False,
    }
    assert not result["order_submitted"]
    assert result["transmitted_to_broker"] is False
    return expected
""",
        },
    )

    report = audit_repository(root)

    assert report.ok
    assert report.violations == ()
    assert "options_copilot.review.validate_result" in report.call_graph


def test_direct_import_alias_attribute_call_and_constant_getattr_fail(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "danger.py": """
from external_broker import place_order as dispatch

WRITE_METHOD = "transmitOrder"

def imported_alias(order):
    return dispatch(order)

def direct_attribute(client, order):
    return client.placeOrder(order)

def constant_getattr(client, order):
    method = getattr(client, WRITE_METHOD)
    return method(order)
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    targets = {violation.target for violation in report.violations}
    assert "external_broker.place_order" in targets
    assert "*.placeOrder" in targets
    assert "*.transmitOrder" in targets
    assert {
        "options_copilot.danger.imported_alias",
        "options_copilot.danger.direct_attribute",
        "options_copilot.danger.constant_getattr",
    }.issubset(set(report.unsafe_callables))


def test_module_and_local_callable_aliases_propagate_authority(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "aliases.py": """
import broker_api as broker

dispatch = broker.placeOrder

def module_alias(order):
    return dispatch(order)

def local_alias(order):
    send = dispatch
    return send(order)
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    assert "options_copilot.aliases.module_alias" in report.unsafe_callables
    assert "options_copilot.aliases.local_alias" in report.unsafe_callables
    assert any(violation.kind == "callable_alias" for violation in report.violations)


def test_unused_function_local_import_and_alias_still_acquire_authority(
    tmp_path: Path,
) -> None:
    root = _package(
        tmp_path,
        {
            "local_import.py": """
import json
safe_alias = json.dumps

def acquire_without_calling():
    from broker_api import transmit_order as write
    retained = write
    return None
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    assert "options_copilot.local_import.acquire_without_calling" in report.unsafe_callables
    kinds = {violation.kind for violation in report.violations}
    assert "forbidden_import" in kinds
    assert "callable_alias" in kinds


def test_protocol_surface_and_inherited_unsafe_method_fail(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "contracts.py": """
from typing import Protocol as Contract

class WritableBroker(Contract):
    def submit_order(self, order): ...

class BaseAdapter:
    def dispatch(self, client, order):
        return client.placeOrder(order)

class ChildAdapter(BaseAdapter):
    pass
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    assert any(violation.kind == "protocol_surface" for violation in report.violations)
    inherited = [
        violation
        for violation in report.violations
        if violation.kind == "inherited_authority"
    ]
    assert inherited
    assert inherited[0].symbol == "options_copilot.contracts.ChildAdapter"
    assert "options_copilot.contracts.ChildAdapter.dispatch" in report.unsafe_callables


def test_three_wrapper_levels_and_cross_module_alias_are_traced(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "inner.py": """
def broker_write(client, order):
    return client.placeOrder(order)
""",
            "wrappers.py": """
from .inner import broker_write as start

def level_one(client, order):
    return start(client, order)

def level_two(client, order):
    return level_one(client, order)

def level_three(client, order):
    return level_two(client, order)
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    assert {
        "options_copilot.inner.broker_write",
        "options_copilot.wrappers.level_one",
        "options_copilot.wrappers.level_two",
        "options_copilot.wrappers.level_three",
    }.issubset(set(report.unsafe_callables))
    propagated = [
        violation
        for violation in report.violations
        if violation.symbol == "options_copilot.wrappers.level_three"
        and violation.kind == "transitive_call"
    ]
    assert propagated
    assert propagated[0].trace[-1] == "*.placeOrder"


def test_future_nested_adapter_is_scanned_without_a_file_allowlist(tmp_path: Path) -> None:
    root = _package(
        tmp_path,
        {
            "bridge/creator_adapter.py": """
def create_review_instruction(payload):
    return {"review_only": True, "order_submitted": False}

def hidden_write(client, order):
    return getattr(client, "submit_order")(order)
""",
        },
    )

    report = audit_repository(root)

    assert not report.ok
    assert any(
        violation.path == "bridge/creator_adapter.py"
        for violation in report.violations
    )
    assert report.files_scanned == 2


def test_repository_root_scans_only_production_python_trees(tmp_path: Path) -> None:
    package = tmp_path / "options_copilot"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "safe.py").write_text(
        "def inspect_snapshot():\n    return None\n",
        encoding="utf-8",
    )
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "verify.py").write_text(
        "def verify_read_only():\n    return True\n",
        encoding="utf-8",
    )
    for ignored in (".venv", "migration_backups", "tests"):
        directory = tmp_path / ignored
        directory.mkdir()
        (directory / "danger.py").write_text(
            "def submit_order():\n    return None\n",
            encoding="utf-8",
        )

    report = audit_repository(tmp_path)

    assert report.ok
    assert report.files_scanned == 3
    assert report.unsafe_callables == ()
    assert all(
        ".venv" not in path
        and "migration_backups" not in path
        and ".tests." not in path
        for path in report.call_graph
    )

    (scripts / "danger.py").write_text(
        "def transmit_order():\n    return None\n",
        encoding="utf-8",
    )
    unsafe_report = audit_repository(tmp_path)

    assert not unsafe_report.ok
    assert any(
        violation.path == "scripts/danger.py"
        for violation in unsafe_report.violations
    )


def test_json_cli_contract_is_stable_for_a_clean_tree(
    tmp_path: Path, capsys
) -> None:
    root = _package(tmp_path, {"safe.py": "def inspect_snapshot():\n    return None\n"})

    exit_code = main(["--root", str(root), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["ok"] is True
    assert payload["root"] == root.resolve().as_posix()
    assert payload["files_scanned"] == 2
    assert payload["violations"] == []
    assert list(payload) == [
        "schema_version",
        "ok",
        "root",
        "files_scanned",
        "callables_scanned",
        "unsafe_callables",
        "call_graph",
        "violations",
    ]
