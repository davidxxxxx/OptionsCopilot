"""CLI for deterministic managed research assembly and strict import."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
from pathlib import Path
import sys

from options_copilot.news.managed_research_producer import (
    assemble_managed_research,
    atomic_write_research_envelope,
    load_managed_research_run,
)
from options_copilot.news.research_top10 import import_research_top10


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Assemble one hash-bound managed-plugin research run and import "
            "its supporting-only Top-10 envelope"
        )
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--store", required=True, type=Path)
    parser.add_argument("--envelope-out", type=Path)
    return parser


def _validate_distinct_paths(
    input_path: Path,
    store_path: Path,
    envelope_path: Path | None,
) -> None:
    requested = [input_path, store_path]
    if envelope_path is not None:
        requested.append(envelope_path)
    canonical: list[Path] = []
    for path in requested:
        absolute = path.absolute()
        if absolute.is_symlink():
            raise ValueError("managed research paths cannot be symlinks")
        canonical.append(absolute.resolve(strict=False))
    if len(set(canonical)) != len(canonical):
        raise ValueError("managed research paths must be distinct")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        _validate_distinct_paths(args.input, args.store, args.envelope_out)
        envelope = assemble_managed_research(
            load_managed_research_run(args.input),
            store_path=args.store,
        )
        result = import_research_top10(envelope, store_path=args.store)
    except Exception:
        # Input documents can contain account-derived research metadata.  Keep
        # stderr stable and never echo exception details or source fragments.
        sys.stderr.write("NO_TRADE: MANAGED_RESEARCH_PRODUCTION_FAILED\n")
        return 2
    if args.envelope_out is not None:
        try:
            atomic_write_research_envelope(args.envelope_out, envelope)
        except Exception:
            # The immutable SQLite import is already committed.  Report that
            # partial outcome explicitly so callers do not treat it as absent.
            sys.stderr.write(
                "NO_TRADE: MANAGED_RESEARCH_EXPORT_FAILED_AFTER_IMPORT\n"
            )
            return 3
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through python -m
    raise SystemExit(main())


__all__ = ["main"]
