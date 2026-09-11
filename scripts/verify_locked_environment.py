"""Verify exact hashed locks and installed distribution equality."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path


__all__ = [
    "InventoryMismatchError",
    "LockContractError",
    "assert_inventory_equal",
    "assert_lock_pair",
    "installed_inventory",
    "main",
    "normalize_project_name",
    "parse_lock_file",
    "parse_lock_text",
]


APPROVED_BOOTSTRAP_NAMES = frozenset({"pip", "setuptools"})
_HASH = r"--hash=sha256:[0-9a-fA-F]{64}"
_WINDOWS_MARKER = r'''(?:\s*;\s*sys_platform\s*==\s*["']win32["'])?'''
_PINNED_REQUIREMENT = re.compile(
    rf"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)"
    rf"==(?P<version>[^\s;\\*]+){_WINDOWS_MARKER}(?:\s+{_HASH})+$",
)
_NAME_NORMALIZATION = re.compile(r"[-_.]+")


class LockContractError(ValueError):
    """The selected lock or verifier allowance is not fail-closed."""


class InventoryMismatchError(RuntimeError):
    """The current interpreter inventory differs from the selected lock."""


def normalize_project_name(value: str) -> str:
    """Return the PEP 503 normalized distribution project name."""

    name = value.strip()
    if not name:
        raise LockContractError("distribution project name must not be blank")
    return _NAME_NORMALIZATION.sub("-", name).lower()


def _logical_entries(text: str, *, source: str) -> tuple[str, ...]:
    entries: list[str] = []
    current: list[str] = []
    for number, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            if current:
                raise LockContractError(
                    f"{source}:{number}: comment or blank line interrupted a requirement",
                )
            continue
        continued = stripped.endswith("\\")
        token = stripped[:-1].strip() if continued else stripped
        if not token:
            raise LockContractError(f"{source}:{number}: empty continuation")
        current.append(token)
        if continued:
            continue
        entries.append(" ".join(current))
        current = []
    if current:
        raise LockContractError(f"{source}: unterminated line continuation")
    return tuple(entries)


def parse_lock_text(text: str, *, source: str = "<lock>") -> dict[str, str]:
    """Parse a self-contained lock, rejecting every non-exact requirement."""

    if not isinstance(text, str):
        raise TypeError("lock text must be a string")
    inventory: dict[str, str] = {}
    entries = _logical_entries(text, source=source)
    if not entries:
        raise LockContractError(f"{source}: lock contains no requirements")
    for entry in entries:
        lowered = entry.lower()
        if lowered.startswith(("-e ", "--editable ", "-r ", "--requirement ", "-c ", "--constraint ")):
            raise LockContractError(f"{source}: resolver/editable directive is forbidden: {entry}")
        match = _PINNED_REQUIREMENT.fullmatch(entry)
        if match is None:
            raise LockContractError(
                f"{source}: requirement must be one exact == pin with SHA-256 hashes: {entry}",
            )
        name = normalize_project_name(match.group("name"))
        version = match.group("version")
        previous = inventory.get(name)
        if previous is not None:
            raise LockContractError(f"{source}: duplicate project pin: {name}")
        inventory[name] = version
    return dict(sorted(inventory.items()))


def parse_lock_file(path: Path | str) -> dict[str, str]:
    """Read one UTF-8 lock and return its exact normalized inventory."""

    lock_path = Path(path)
    try:
        text = lock_path.read_text(encoding="utf-8", errors="strict")
    except UnicodeError as exc:
        raise LockContractError(f"{lock_path}: lock is not valid UTF-8") from exc
    return parse_lock_text(text, source=str(lock_path))


def installed_inventory() -> dict[str, str]:
    """Read installed projects from the interpreter running this process."""

    inventory: dict[str, str] = {}
    locations: dict[str, str] = {}
    for distribution in importlib.metadata.distributions():
        raw_name = distribution.metadata.get("Name")
        if not raw_name:
            raise LockContractError("installed distribution is missing its Name metadata")
        name = normalize_project_name(raw_name)
        version = distribution.version
        try:
            location = str(Path(distribution.locate_file("")).resolve())
        except (OSError, RuntimeError, TypeError, ValueError):
            location = "<unresolved>"
        if name in inventory:
            raise InventoryMismatchError(
                "duplicate installed project metadata is forbidden: "
                f"{name}; first_version={inventory[name]}; "
                f"first_location={locations[name]}; "
                f"duplicate_version={version}; duplicate_location={location}",
            )
        inventory[name] = version
        locations[name] = location
    return dict(sorted(inventory.items()))


def _normalized_inventory(values: Mapping[str, str], *, label: str) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_name, raw_version in values.items():
        name = normalize_project_name(raw_name)
        version = str(raw_version).strip()
        if not version:
            raise LockContractError(f"{label} version must not be blank: {name}")
        if name in normalized:
            raise LockContractError(f"{label} contains duplicate normalized project: {name}")
        normalized[name] = version
    return normalized


def assert_inventory_equal(
    expected: Mapping[str, str],
    installed: Mapping[str, str],
    *,
    allowed_bootstrap: Iterable[str],
) -> None:
    """Fail unless installed projects exactly equal the lock after fixed allowances."""

    allowed = frozenset(normalize_project_name(name) for name in allowed_bootstrap)
    unsupported = sorted(allowed - APPROVED_BOOTSTRAP_NAMES)
    if unsupported:
        raise LockContractError(f"unsupported bootstrap allowance(s): {unsupported}")
    expected_normalized = _normalized_inventory(expected, label="lock")
    installed_normalized = _normalized_inventory(installed, label="installed inventory")
    audited = {
        name: version
        for name, version in installed_normalized.items()
        if name not in allowed
    }
    missing = sorted(set(expected_normalized) - set(audited))
    surplus = sorted(set(audited) - set(expected_normalized))
    version_mismatches = {
        name: {"expected": expected_normalized[name], "installed": audited[name]}
        for name in sorted(set(expected_normalized) & set(audited))
        if expected_normalized[name] != audited[name]
    }
    if missing or surplus or version_mismatches:
        details = {
            "missing": missing,
            "surplus": surplus,
            "version_mismatches": version_mismatches,
        }
        raise InventoryMismatchError(json.dumps(details, sort_keys=True))


def assert_lock_pair(
    production: Mapping[str, str],
    development: Mapping[str, str],
) -> None:
    """Fail unless production is an exact-version subset of development."""

    production_normalized = _normalized_inventory(
        production,
        label="production lock",
    )
    development_normalized = _normalized_inventory(
        development,
        label="development lock",
    )
    missing = sorted(set(production_normalized) - set(development_normalized))
    version_mismatches = {
        name: {
            "production": production_normalized[name],
            "development": development_normalized[name],
        }
        for name in sorted(set(production_normalized) & set(development_normalized))
        if production_normalized[name] != development_normalized[name]
    }
    if missing or version_mismatches:
        details = {
            "missing_from_development": missing,
            "version_mismatches": version_mismatches,
        }
        raise LockContractError(
            "development lock does not contain the exact production closure: "
            + json.dumps(details, sort_keys=True),
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate exact hash locks or compare one to this interpreter.",
    )
    parser.add_argument("--lock", type=Path)
    parser.add_argument("--production-lock", type=Path)
    parser.add_argument("--development-lock", type=Path)
    parser.add_argument("--allow-bootstrap", action="append", default=[])
    return parser


def run(argv: Sequence[str] | None = None) -> dict[str, object]:
    args = _build_parser().parse_args(argv)
    if sys.version_info < (3, 12):
        raise LockContractError("the audited interpreter must be CPython 3.12 or newer")
    if sys.implementation.name != "cpython":
        raise LockContractError("the audited interpreter must be CPython")
    pair_selected = args.production_lock is not None or args.development_lock is not None
    if args.lock is not None and pair_selected:
        raise LockContractError("select either one installed lock or one lock pair")
    if pair_selected:
        if args.production_lock is None or args.development_lock is None:
            raise LockContractError(
                "lock-pair validation requires production and development locks",
            )
        if args.allow_bootstrap:
            raise LockContractError(
                "bootstrap allowances apply only to installed-inventory validation",
            )
        production_path = args.production_lock.expanduser().resolve(strict=True)
        development_path = args.development_lock.expanduser().resolve(strict=True)
        if production_path == development_path:
            raise LockContractError("production and development locks must be different files")
        production = parse_lock_file(production_path)
        development = parse_lock_file(development_path)
        assert_lock_pair(production, development)
        return {
            "status": "LOCK_PAIR_VALID",
            "production_lock": str(production_path),
            "production_lock_sha256": hashlib.sha256(
                production_path.read_bytes(),
            ).hexdigest(),
            "production_locked": production,
            "development_lock": str(development_path),
            "development_lock_sha256": hashlib.sha256(
                development_path.read_bytes(),
            ).hexdigest(),
            "development_locked": development,
        }
    if args.lock is None:
        raise LockContractError("one installed lock or one lock pair is required")
    lock_path = args.lock.expanduser().resolve(strict=True)
    expected = parse_lock_file(lock_path)
    installed = installed_inventory()
    assert_inventory_equal(
        expected,
        installed,
        allowed_bootstrap=args.allow_bootstrap,
    )
    allowed = sorted(normalize_project_name(name) for name in args.allow_bootstrap)
    return {
        "status": "EXACT",
        "interpreter": str(Path(sys.executable).resolve()),
        "python_version": ".".join(str(part) for part in sys.version_info[:3]),
        "lock": str(lock_path),
        "lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
        "locked": expected,
        "installed": installed,
        "allowed_bootstrap": allowed,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        payload = run(argv)
    except (OSError, LockContractError, InventoryMismatchError, TypeError) as exc:
        print(
            json.dumps(
                {"status": "MISMATCH", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
