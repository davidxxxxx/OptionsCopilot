"""Pure identity validation for SEC Atom filer entries."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
import re
from urllib.parse import urlsplit


_ACCESSION = re.compile(
    r"urn:tag:sec\.gov,2008:accession-number="
    r"(?P<first>[0-9]{10})-(?P<year>[0-9]{2})-(?P<sequence>[0-9]{6})\Z"
)
_CIK = re.compile(r"[0-9]{10}\Z")
_DOCUMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z")
_ALLOWED_HOST = "www.sec.gov"


@dataclass(frozen=True, slots=True)
class SecFilerEntryIdentity:
    accession: str
    filer_cik: str
    url: str
    event_id: str


def sec_filer_entry_identity(
    accession: str,
    filer_cik: str,
    url: str,
) -> SecFilerEntryIdentity:
    """Validate and identify one exact accession-and-filer Atom entry."""

    accession_match = (
        _ACCESSION.fullmatch(accession)
        if isinstance(accession, str)
        else None
    )
    if accession_match is None:
        raise ValueError("SEC accession identity is invalid")
    if (
        not isinstance(filer_cik, str)
        or _CIK.fullmatch(filer_cik) is None
        or int(filer_cik) == 0
    ):
        raise ValueError("SEC filer CIK is invalid")
    if not isinstance(url, str) or url != url.strip():
        raise ValueError("SEC filer URL is invalid")
    parsed = urlsplit(url)
    path_parts = parsed.path.split("/")
    if (
        parsed.scheme.lower() != "https"
        or parsed.hostname != _ALLOWED_HOST
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or len(path_parts) != 7
        or path_parts[1:4] != ["Archives", "edgar", "data"]
        or path_parts[4] != str(int(filer_cik))
        or path_parts[5] != "".join(accession_match.groups())
        or _DOCUMENT.fullmatch(path_parts[6]) is None
        or path_parts[6] in {".", ".."}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("SEC filer URL is invalid")
    digest = hashlib.sha256(
        f"{accession}\x1f{filer_cik}".encode("utf-8")
    ).hexdigest()
    return SecFilerEntryIdentity(
        accession=accession,
        filer_cik=filer_cik,
        url=url,
        event_id=f"sec-current:{digest}",
    )


def sec_filer_group_identity(
    *,
    source: str,
    source_id: str,
    event_id: str,
    lineage_id: str | None,
    evidence_ids: Sequence[str],
    url: str,
    provider_story_id: str | None,
) -> str | None:
    """Recognize only a fully self-consistent new SEC filer event identity."""

    if source != "SEC" or provider_story_id is not None:
        return None
    try:
        path_parts = urlsplit(url).path.split("/")
        if len(path_parts) != 7 or not path_parts[4].isdigit():
            return None
        filer_cik = path_parts[4].zfill(10)
        identity = sec_filer_entry_identity(source_id, filer_cik, url)
    except (TypeError, ValueError):
        return None
    if (
        event_id != identity.event_id
        or lineage_id != identity.event_id
        or tuple(evidence_ids) != (identity.event_id,)
    ):
        return None
    return identity.event_id


__all__ = [
    "SecFilerEntryIdentity",
    "sec_filer_entry_identity",
    "sec_filer_group_identity",
]
