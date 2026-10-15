"""Immutable, read-only similarity search for shadow-learning evidence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
import re
from types import MappingProxyType

from options_copilot.storage.canonical import canonical_hash


_VERSION_RE = re.compile(r"v[1-9][0-9]*(?:\.[0-9]+)*\Z")


def _decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be numeric")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field} must be numeric") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def _vector(value: Sequence[object], *, field: str) -> tuple[Decimal, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{field} must be a numeric sequence")
    output = tuple(_decimal(item, field=field) for item in value)
    if not output:
        raise ValueError(f"{field} cannot be empty")
    return output


@dataclass(frozen=True, slots=True)
class SimilarityMatch:
    evidence_id: str
    distance: Decimal


@dataclass(frozen=True, slots=True)
class SimilarityQueryResult:
    matches: tuple[SimilarityMatch, ...]
    index_version: str
    index_hash: str
    shadow_only: bool = True
    can_mutate_policy: bool = False
    can_mutate_risk: bool = False
    can_mutate_ranking: bool = False
    can_create_instruction: bool = False


class SimilarityIndex:
    """A frozen evidence index with no write or authority-bearing methods."""

    def __init__(
        self,
        evidence: Mapping[str, Sequence[object]],
        *,
        version: str,
    ) -> None:
        if not isinstance(version, str) or _VERSION_RE.fullmatch(version) is None:
            raise ValueError("similarity index version must be v1-style")
        if not isinstance(evidence, Mapping) or not evidence:
            raise ValueError("similarity evidence must be a nonempty mapping")
        vectors: dict[str, tuple[Decimal, ...]] = {}
        width: int | None = None
        for evidence_id, raw_vector in evidence.items():
            if not isinstance(evidence_id, str) or not evidence_id.strip():
                raise ValueError("evidence_id must be nonblank")
            vector = _vector(raw_vector, field=f"evidence[{evidence_id}]")
            width = len(vector) if width is None else width
            if len(vector) != width:
                raise ValueError("all similarity vectors must have the same width")
            vectors[evidence_id] = vector
        self._version = version
        self._width = width or 0
        self._evidence = MappingProxyType(dict(sorted(vectors.items())))
        self._index_hash = canonical_hash(
            {
                "schema": "options_copilot.learning.similarity_index.v1",
                "version": version,
                "evidence": self._evidence,
            }
        )

    @property
    def evidence(self) -> Mapping[str, tuple[Decimal, ...]]:
        return self._evidence

    @property
    def index_version(self) -> str:
        return self._version

    @property
    def index_hash(self) -> str:
        return self._index_hash

    def query(
        self,
        vector: Sequence[object],
        *,
        limit: int = 5,
    ) -> SimilarityQueryResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        needle = _vector(vector, field="query vector")
        if len(needle) != self._width:
            raise ValueError("query vector width does not match the index")
        matches = tuple(
            sorted(
                (
                    SimilarityMatch(evidence_id, _distance(needle, candidate))
                    for evidence_id, candidate in self._evidence.items()
                ),
                key=lambda item: (item.distance, item.evidence_id),
            )[:limit]
        )
        return SimilarityQueryResult(
            matches=matches,
            index_version=self._version,
            index_hash=self._index_hash,
        )


def _distance(left: tuple[Decimal, ...], right: tuple[Decimal, ...]) -> Decimal:
    with localcontext() as context:
        context.prec = 40
        squared = sum(((a - b) * (a - b) for a, b in zip(left, right)), Decimal("0"))
        return squared.sqrt()


__all__ = ["SimilarityIndex", "SimilarityMatch", "SimilarityQueryResult"]
