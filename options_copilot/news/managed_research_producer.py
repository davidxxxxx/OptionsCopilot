"""Deterministic managed-plugin research assembler with no broker authority.

The module accepts one already captured, hash-bound read-only run document.  It
does not own a broker client, creator transport, combo builder, approval path,
or order primitive.  Its only output is the existing supporting-only research
Top-10 v2 envelope.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import os
from pathlib import Path
import re
import tempfile
from zoneinfo import ZoneInfo

from options_copilot.news.research_top10 import (
    INDICATIVE_REPRICE,
    PREMARKET_RESEARCH,
    RESEARCH_TOP10_SCHEMA,
    RESEARCH_TOP10_SOURCE,
    RESEARCH_TOP10_VERSION,
    ResearchTop10ValidationError,
    read_research_top10,
    validate_research_top10_envelope,
)
from options_copilot.market.session_calendar import (
    CalendarStatus,
    UsOptionsSessionCalendar,
)
from options_copilot.storage.canonical import canonical_hash


MANAGED_RESEARCH_RUN_SCHEMA = "options_copilot.managed_research_run.v1"
MANAGED_RESEARCH_RUN_VERSION = 1
MAXIMUM_MANAGED_RUN_BYTES = 4 * 1024 * 1024

_NEW_YORK = ZoneInfo("America/New_York")
_CENT = Decimal("0.01")
_QUOTE_MAX_AGE = timedelta(seconds=5)
_QUOTE_MAX_SKEW = timedelta(seconds=2)
_POLICY_ASSUMED_MULTIPLIER = 100
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_BLOCKER = re.compile(r"[A-Z][A-Z0-9_:-]{2,127}\Z")
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9.-]{0,23}\Z")

_AUTHORITY = {
    "decision_authority": "SUPPORTING_ONLY",
    "approval_eligible": False,
    "instruction_creation_allowed": False,
    "order_allowed": False,
    "action_pool_eligible": False,
}
_TOP_FIELDS = {
    "schema",
    "version",
    "run_id",
    "batch_id",
    "phase",
    "trading_date",
    "observed_at",
    "source",
    "component_hashes",
    "account",
    "session",
    "secdefs",
    "quotes",
    "evidence",
    "candidates",
}
_COMPONENT_HASH_FIELDS = {
    "account",
    "session",
    "secdefs",
    "quotes",
    "evidence",
    "candidates",
}
_BINDING_FIELDS = {
    "account",
    "session",
    "secdefs",
    "quotes",
    "evidence",
}
_ACCOUNT_FIELDS = {
    "run_id",
    "batch_id",
    "observed_at",
    "currency",
    "strategy_nav_usd",
    "blockers",
}
_SESSION_FIELDS = {
    "run_id",
    "batch_id",
    "liquid_hours",
    "trading_hours",
    "timezone_id",
    "observed_at",
    "source",
    "blockers",
}
_SECDEF_FIELDS = {
    "run_id",
    "batch_id",
    "contract_id",
    "contract_id_ex",
    "underlying",
    "right",
    "strike",
    "expiration",
    "exchange",
    "trading_class",
    "local_symbol",
    "multiplier",
    "standard_or_adjusted",
    "evidence_hash",
    "blockers",
}
_QUOTE_FIELDS = {
    "run_id",
    "batch_id",
    "contract_id",
    "bid",
    "ask",
    "collected_at",
    "quote_asof",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
    "market_data_type",
    "evidence_hash",
    "blockers",
}
_EVIDENCE_FIELDS = {
    "run_id",
    "batch_id",
    "evidence_id",
    "evidence_hash",
}
_PREMARKET_CANDIDATE_FIELDS = {
    "research_id",
    "rank",
    "underlying",
    "strategy_type",
    "expiration",
    "quantity",
    "assumed_multiplier",
    "execution_cost_cap_usd",
    "expected_payoff_usd",
    "entry_condition",
    "invalidation_condition",
    "profit_target_condition",
    "stop_loss_condition",
    "research_summary",
    "evidence_ids",
    "legs",
    "bindings",
}
_REPRICE_SELECTOR_FIELDS = {"research_id", "rank", "bindings"}
_LEG_REFERENCE_FIELDS = {"contract_id", "side"}
_CANDIDATE_BLOCKERS = (
    "ASSUMED_MULTIPLIER_USED",
    "ENTRY_DEBIT_UNVERIFIED",
    "MAXIMUM_LOSS_UNVERIFIED",
    "AFTER_COST_EV_UNVERIFIED",
    "RISK_CAP_UNVERIFIED",
)
_OPTIONAL_SECDEF_BLOCKERS = {
    "local_symbol": "LOCAL_SYMBOL_UNAVAILABLE",
    "multiplier": "MULTIPLIER_UNAVAILABLE",
    "standard_or_adjusted": "STANDARD_ADJUSTED_UNVERIFIED",
}
_OPTIONAL_QUOTE_BLOCKERS = {
    "quote_asof": "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE",
    "implied_volatility": "IMPLIED_VOLATILITY_UNAVAILABLE",
    "delta": "DELTA_UNAVAILABLE",
    "gamma": "GAMMA_UNAVAILABLE",
    "theta": "THETA_UNAVAILABLE",
    "vega": "VEGA_UNAVAILABLE",
    "volume": "VOLUME_UNAVAILABLE",
    "open_interest": "OPEN_INTEREST_UNAVAILABLE",
    "market_data_type": "MARKET_DATA_TYPE_UNVERIFIED",
}
_REFRESHABLE_CANDIDATE_BLOCKERS = frozenset(
    {
        *_OPTIONAL_QUOTE_BLOCKERS.values(),
        "MARKET_DATA_NOT_LIVE",
        "QUOTE_CROSSED",
        "QUOTE_FUTURE",
        "QUOTE_LEG_SKEW_EXCEEDED",
        "QUOTE_STALE",
        "QUOTE_UNAVAILABLE",
    }
)


class ManagedResearchInputError(ValueError):
    """One managed research run is incomplete, mixed, or unbound."""


def load_managed_research_run(path: str | Path) -> Mapping[str, object]:
    """Load one bounded UTF-8 JSON object while rejecting duplicate keys."""

    source = Path(path)
    try:
        size = source.stat().st_size
        if size <= 0 or size > MAXIMUM_MANAGED_RUN_BYTES:
            raise ManagedResearchInputError("managed research input size is invalid")
        raw = source.read_bytes()
    except ManagedResearchInputError:
        raise
    except OSError as exc:
        raise ManagedResearchInputError("managed research input is unavailable") from exc
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_float=Decimal,
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManagedResearchInputError("managed research input JSON is invalid") from exc
    if not isinstance(value, Mapping):
        raise ManagedResearchInputError("managed research input must be an object")
    return value


def assemble_managed_research(
    value: object,
    *,
    store_path: str | Path | None = None,
) -> dict[str, object]:
    """Build a validated supporting-only research Top-10 v2 envelope."""

    run = _exact_mapping(value, _TOP_FIELDS, "managed research run")
    if run["schema"] != MANAGED_RESEARCH_RUN_SCHEMA:
        raise ManagedResearchInputError("managed research input schema is unsupported")
    version = run["version"]
    if isinstance(version, bool) or version != MANAGED_RESEARCH_RUN_VERSION:
        raise ManagedResearchInputError("managed research input version is unsupported")
    if run["source"] != RESEARCH_TOP10_SOURCE:
        raise ManagedResearchInputError("managed research source is unsupported")
    run_id = _identifier(run["run_id"], "run_id")
    batch_id = _identifier(run["batch_id"], "batch_id")
    phase = _choice(
        run["phase"],
        {PREMARKET_RESEARCH, INDICATIVE_REPRICE},
        "phase",
    )
    trading_date = _date(run["trading_date"], "trading_date")
    observed_at = _timestamp(run["observed_at"], "observed_at")
    _validate_slot(phase, observed_at, trading_date)

    account = _exact_mapping(run["account"], _ACCOUNT_FIELDS, "account")
    session = _exact_mapping(run["session"], _SESSION_FIELDS, "session")
    secdef_rows = _array(run["secdefs"], "secdefs")
    quote_rows = _array(run["quotes"], "quotes")
    evidence_rows = _array(run["evidence"], "evidence")
    candidate_rows = _array(run["candidates"], "candidates")
    components: dict[str, object] = {
        "account": account,
        "session": session,
        "secdefs": secdef_rows,
        "quotes": quote_rows,
        "evidence": evidence_rows,
        "candidates": candidate_rows,
    }
    declared_hashes = _component_hashes(run["component_hashes"], components)
    expected_bindings = {name: declared_hashes[name] for name in _BINDING_FIELDS}

    account_values = _account(
        account,
        run_id=run_id,
        batch_id=batch_id,
        trading_date=trading_date,
        observed_at=observed_at,
    )
    session_values = _session(
        session,
        run_id=run_id,
        batch_id=batch_id,
        trading_date=trading_date,
        observed_at=observed_at,
    )
    secdefs = _secdefs(
        secdef_rows,
        run_id=run_id,
        batch_id=batch_id,
        trading_date=trading_date,
    )
    quotes = _quotes(
        quote_rows,
        run_id=run_id,
        batch_id=batch_id,
        phase=phase,
        trading_date=trading_date,
        observed_at=observed_at,
    )
    if set(secdefs) != set(quotes):
        raise ManagedResearchInputError("secdef and quote contract sets differ")
    evidence = _evidence(
        evidence_rows,
        run_id=run_id,
        batch_id=batch_id,
    )

    if len(candidate_rows) != 10:
        raise ManagedResearchInputError("managed research requires exactly ten candidates")
    if phase == PREMARKET_RESEARCH:
        candidates = _premarket_candidates(
            candidate_rows,
            expected_bindings=expected_bindings,
            secdefs=secdefs,
            quotes=quotes,
            evidence=evidence,
            trading_date=trading_date,
            observed_at=observed_at,
            strategy_nav=account_values["strategy_nav"],
        )
        parent_content_hash = None
    else:
        candidates, parent_content_hash = _reprice_candidates(
            candidate_rows,
            expected_bindings=expected_bindings,
            secdefs=secdefs,
            quotes=quotes,
            evidence=evidence,
            trading_date=trading_date,
            observed_at=observed_at,
            strategy_nav=account_values["strategy_nav"],
            store_path=store_path,
        )

    envelope = {
        "schema": RESEARCH_TOP10_SCHEMA,
        "version": RESEARCH_TOP10_VERSION,
        "batch_id": batch_id,
        "phase": phase,
        "trading_date": trading_date.isoformat(),
        "observed_at": _timestamp_text(observed_at),
        "source": RESEARCH_TOP10_SOURCE,
        "strategy_nav_usd": _money_text(account_values["strategy_nav"]),
        "normal_risk_fraction": "0.10",
        "target_count": 10,
        "parent_content_hash": parent_content_hash,
        "session": session_values,
        "blockers": list(account_values["blockers"]),
        "candidates": candidates,
        **_AUTHORITY,
    }
    try:
        return validate_research_top10_envelope(envelope)
    except ResearchTop10ValidationError as exc:
        raise ManagedResearchInputError("assembled research envelope is invalid") from exc


def atomic_write_research_envelope(
    path: str | Path,
    envelope: Mapping[str, object],
) -> None:
    """Atomically publish one already validated research envelope JSON file."""

    try:
        validated = validate_research_top10_envelope(envelope)
    except ResearchTop10ValidationError as exc:
        raise ManagedResearchInputError("research envelope is invalid") from exc
    requested = Path(path).absolute()
    if requested.is_symlink():
        raise ManagedResearchInputError("research envelope output cannot be a symlink")
    destination = requested.resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    rendered = (
        json.dumps(
            validated,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        temporary = None
    except OSError as exc:
        raise ManagedResearchInputError("research envelope output failed") from exc
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _component_hashes(
    value: object,
    components: Mapping[str, object],
) -> dict[str, str]:
    row = _exact_mapping(value, _COMPONENT_HASH_FIELDS, "component_hashes")
    result: dict[str, str] = {}
    for name in sorted(_COMPONENT_HASH_FIELDS):
        declared = _digest(row[name], f"component_hashes.{name}")
        if canonical_hash(components[name]) != declared:
            raise ManagedResearchInputError(f"{name} component hash mismatch")
        result[name] = declared
    return result


def _account(
    value: Mapping[str, object],
    *,
    run_id: str,
    batch_id: str,
    trading_date: date,
    observed_at: datetime,
) -> dict[str, object]:
    _run_binding(value, run_id=run_id, batch_id=batch_id, name="account")
    timestamp = _bounded_timestamp(
        value["observed_at"],
        name="account.observed_at",
        trading_date=trading_date,
        upper_bound=observed_at,
    )
    if observed_at - timestamp > _QUOTE_MAX_AGE:
        raise ManagedResearchInputError("account.observed_at is stale")
    if value["currency"] != "USD":
        raise ManagedResearchInputError("account currency must be USD")
    return {
        "observed_at": timestamp,
        "strategy_nav": _positive_decimal(
            value["strategy_nav_usd"], "account.strategy_nav_usd"
        ),
        "blockers": _blockers(value["blockers"], "account.blockers"),
    }


def _session(
    value: Mapping[str, object],
    *,
    run_id: str,
    batch_id: str,
    trading_date: date,
    observed_at: datetime,
) -> dict[str, object]:
    _run_binding(value, run_id=run_id, batch_id=batch_id, name="session")
    blockers = _blockers(value["blockers"], "session.blockers")
    fields = (
        value["liquid_hours"],
        value["trading_hours"],
        value["timezone_id"],
        value["observed_at"],
        value["source"],
    )
    if all(item is None for item in fields):
        if "BROKER_SESSION_HOURS_UNAVAILABLE" not in blockers:
            raise ManagedResearchInputError(
                "missing session requires BROKER_SESSION_HOURS_UNAVAILABLE"
            )
        return {
            "liquid_hours": None,
            "trading_hours": None,
            "timezone_id": None,
            "observed_at": None,
            "source": None,
            "blockers": list(blockers),
        }
    if any(item is None for item in fields):
        raise ManagedResearchInputError("session fields cannot be partial")
    timestamp = _bounded_timestamp(
        value["observed_at"],
        name="session.observed_at",
        trading_date=trading_date,
        upper_bound=observed_at,
    )
    if observed_at - timestamp > _QUOTE_MAX_AGE:
        raise ManagedResearchInputError("session.observed_at is stale")
    liquid_hours = _text(value["liquid_hours"], "session.liquid_hours")
    trading_hours = _text(value["trading_hours"], "session.trading_hours")
    timezone_id = _text(value["timezone_id"], "session.timezone_id")
    source = _text(value["source"], "session.source")
    if timezone_id != "America/New_York":
        raise ManagedResearchInputError("session timezone is unsupported")
    if source != "IBKR_MANAGED_PLUGIN":
        raise ManagedResearchInputError("session source is unsupported")
    # The shared calendar normalizer recognizes the direct gateway provenance
    # name.  The exact managed-plugin source check above is the trust boundary;
    # this fixed adapter value only reuses its hours/date parser and is never
    # copied into the research envelope.
    normalized = UsOptionsSessionCalendar().normalize(
        liquid_hours=liquid_hours,
        trading_hours=trading_hours,
        timezone_id=timezone_id,
        observed_at=timestamp,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=observed_at,
    )
    if (
        normalized.status is not CalendarStatus.READY
        or not any(item.trading_date == trading_date for item in normalized.sessions)
    ):
        raise ManagedResearchInputError("session hours are invalid for the trading date")
    return {
        "liquid_hours": liquid_hours,
        "trading_hours": trading_hours,
        "timezone_id": timezone_id,
        "observed_at": _timestamp_text(timestamp),
        "source": source,
        "blockers": list(blockers),
    }


def _secdefs(
    values: Sequence[object],
    *,
    run_id: str,
    batch_id: str,
    trading_date: date,
) -> dict[int, dict[str, object]]:
    result: dict[int, dict[str, object]] = {}
    for index, raw in enumerate(values):
        name = f"secdefs[{index}]"
        row = _exact_mapping(raw, _SECDEF_FIELDS, name)
        _run_binding(row, run_id=run_id, batch_id=batch_id, name=name)
        blockers = _blockers(row["blockers"], f"{name}.blockers")
        contract_id = _positive_integer(row["contract_id"], f"{name}.contract_id")
        if contract_id in result:
            raise ManagedResearchInputError("duplicate secdef contract identity")
        contract_id_ex = _text(row["contract_id_ex"], f"{name}.contract_id_ex")
        if not contract_id_ex.startswith(f"{contract_id}@"):
            raise ManagedResearchInputError(f"{name}.contract_id_ex is not contract-bound")
        for field, blocker in _OPTIONAL_SECDEF_BLOCKERS.items():
            _optional_fact(row[field], blockers, blocker, f"{name}.{field}")
        standard = row["standard_or_adjusted"]
        if standard is not None and standard not in {"STANDARD", "ADJUSTED"}:
            raise ManagedResearchInputError(f"{name}.standard_or_adjusted is invalid")
        if standard == "ADJUSTED" and "ADJUSTED_CONTRACT_UNSUPPORTED" not in blockers:
            raise ManagedResearchInputError(f"{name} adjusted contract is not blocked")
        expiry = _date(row["expiration"], f"{name}.expiration")
        dte = (expiry - trading_date).days
        if not 14 <= dte <= 35:
            raise ManagedResearchInputError(f"{name} expiry is outside 14-35 DTE")
        result[contract_id] = {
            "contract_id": contract_id,
            "contract_id_ex": contract_id_ex,
            "underlying": _symbol(row["underlying"], f"{name}.underlying"),
            "right": _choice(row["right"], {"C", "P"}, f"{name}.right"),
            "strike": _decimal_text(_positive_decimal(row["strike"], f"{name}.strike")),
            "expiration": expiry.isoformat(),
            "exchange": _text(row["exchange"], f"{name}.exchange").upper(),
            "trading_class": _text(row["trading_class"], f"{name}.trading_class"),
            "local_symbol": _optional_text(row["local_symbol"], f"{name}.local_symbol"),
            "multiplier": _optional_positive_integer(
                row["multiplier"], f"{name}.multiplier"
            ),
            "standard_or_adjusted": standard,
            "identity_evidence_hash": _digest(
                row["evidence_hash"], f"{name}.evidence_hash"
            ),
            "blockers": list(blockers),
        }
    return result


def _quotes(
    values: Sequence[object],
    *,
    run_id: str,
    batch_id: str,
    phase: str,
    trading_date: date,
    observed_at: datetime,
) -> dict[int, dict[str, object]]:
    result: dict[int, dict[str, object]] = {}
    for index, raw in enumerate(values):
        name = f"quotes[{index}]"
        row = _exact_mapping(raw, _QUOTE_FIELDS, name)
        _run_binding(row, run_id=run_id, batch_id=batch_id, name=name)
        blockers = _blockers(row["blockers"], f"{name}.blockers")
        contract_id = _positive_integer(row["contract_id"], f"{name}.contract_id")
        if contract_id in result:
            raise ManagedResearchInputError("duplicate quote contract identity")
        bid = _optional_decimal(row["bid"], f"{name}.bid")
        ask = _optional_decimal(row["ask"], f"{name}.ask")
        if (bid is None) != (ask is None):
            raise ManagedResearchInputError(
                f"{name} bid and ask must both be numeric or both be null"
            )
        if bid is None:
            if phase != PREMARKET_RESEARCH:
                raise ManagedResearchInputError(
                    f"{name} INDICATIVE_REPRICE requires numeric bid and ask"
                )
            if "QUOTE_UNAVAILABLE" not in blockers:
                raise ManagedResearchInputError(
                    f"{name} null bid and ask require QUOTE_UNAVAILABLE"
                )
            if row["quote_asof"] is not None:
                raise ManagedResearchInputError(
                    f"{name} unquoted bid and ask require null quote_asof"
                )
        else:
            if "QUOTE_UNAVAILABLE" in blockers:
                raise ManagedResearchInputError(
                    f"{name} numeric bid and ask conflict with QUOTE_UNAVAILABLE"
                )
            if bid < 0 or ask is None or ask <= 0:
                raise ManagedResearchInputError(f"{name} quote is invalid")
            if bid > ask:
                raise ManagedResearchInputError(f"{name} quote is crossed")
        collected_at = _bounded_timestamp(
            row["collected_at"],
            name=f"{name}.collected_at",
            trading_date=trading_date,
            upper_bound=observed_at,
        )
        if observed_at - collected_at > _QUOTE_MAX_AGE:
            raise ManagedResearchInputError(f"{name}.collected_at is stale")
        quote_asof = row["quote_asof"]
        for field, blocker in _OPTIONAL_QUOTE_BLOCKERS.items():
            _optional_fact(row[field], blockers, blocker, f"{name}.{field}")
        if quote_asof is not None:
            quote_asof = _bounded_timestamp(
                quote_asof,
                name=f"{name}.quote_asof",
                trading_date=trading_date,
                upper_bound=collected_at,
            )
            if observed_at - quote_asof > _QUOTE_MAX_AGE:
                raise ManagedResearchInputError(f"{name}.quote_asof is stale")
        market_data_type = _optional_positive_integer(
            row["market_data_type"], f"{name}.market_data_type"
        )
        if market_data_type is not None:
            if market_data_type not in {1, 2, 3, 4}:
                raise ManagedResearchInputError(f"{name}.market_data_type is invalid")
            if market_data_type != 1 and "MARKET_DATA_NOT_LIVE" not in blockers:
                raise ManagedResearchInputError(f"{name} non-live data is not blocked")
        implied_volatility = _optional_decimal(
            row["implied_volatility"], f"{name}.implied_volatility"
        )
        if implied_volatility is not None and implied_volatility < 0:
            raise ManagedResearchInputError(f"{name}.implied_volatility is invalid")
        delta = _optional_decimal(row["delta"], f"{name}.delta")
        if delta is not None and not Decimal("-1") <= delta <= Decimal("1"):
            raise ManagedResearchInputError(f"{name}.delta is invalid")
        gamma = _optional_decimal(row["gamma"], f"{name}.gamma")
        vega = _optional_decimal(row["vega"], f"{name}.vega")
        if any(value is not None and value < 0 for value in (gamma, vega)):
            raise ManagedResearchInputError(f"{name} gamma or vega is invalid")
        theta = _optional_decimal(row["theta"], f"{name}.theta")
        result[contract_id] = {
            "bid": _optional_decimal_text(bid),
            "ask": _optional_decimal_text(ask),
            "collected_at": _timestamp_text(collected_at),
            "quote_asof": (
                None if quote_asof is None else _timestamp_text(quote_asof)
            ),
            "implied_volatility": _optional_decimal_text(implied_volatility),
            "delta": _optional_decimal_text(delta),
            "gamma": _optional_decimal_text(gamma),
            "theta": _optional_decimal_text(theta),
            "vega": _optional_decimal_text(vega),
            "volume": _optional_nonnegative_integer(
                row["volume"], f"{name}.volume"
            ),
            "open_interest": _optional_nonnegative_integer(
                row["open_interest"], f"{name}.open_interest"
            ),
            "market_data_type": market_data_type,
            "quote_evidence_hash": _digest(
                row["evidence_hash"], f"{name}.evidence_hash"
            ),
            "blockers": list(blockers),
        }
    return result


def _evidence(
    values: Sequence[object],
    *,
    run_id: str,
    batch_id: str,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for index, raw in enumerate(values):
        name = f"evidence[{index}]"
        row = _exact_mapping(raw, _EVIDENCE_FIELDS, name)
        _run_binding(row, run_id=run_id, batch_id=batch_id, name=name)
        evidence_id = _identifier(row["evidence_id"], f"{name}.evidence_id")
        if evidence_id in result:
            raise ManagedResearchInputError("duplicate evidence identity")
        result[evidence_id] = _digest(
            row["evidence_hash"], f"{name}.evidence_hash"
        )
    return result


def _premarket_candidates(
    values: Sequence[object],
    *,
    expected_bindings: Mapping[str, str],
    secdefs: Mapping[int, Mapping[str, object]],
    quotes: Mapping[int, Mapping[str, object]],
    evidence: Mapping[str, str],
    trading_date: date,
    observed_at: datetime,
    strategy_nav: Decimal,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    used_contracts: set[int] = set()
    used_evidence: set[str] = set()
    for index, raw in enumerate(values, start=1):
        name = f"candidates[{index - 1}]"
        row = _exact_mapping(raw, _PREMARKET_CANDIDATE_FIELDS, name)
        _candidate_binding(row["bindings"], expected_bindings, name)
        rank = _positive_integer(row["rank"], f"{name}.rank")
        if rank != index:
            raise ManagedResearchInputError("candidate ranks must be contiguous")
        underlying = _symbol(row["underlying"], f"{name}.underlying")
        strategy = _choice(
            str(row["strategy_type"]).upper(),
            {"BULL_CALL_VERTICAL", "BEAR_PUT_VERTICAL"},
            f"{name}.strategy_type",
        )
        expiration = _date(row["expiration"], f"{name}.expiration")
        dte = (expiration - trading_date).days
        if not 14 <= dte <= 35:
            raise ManagedResearchInputError(f"{name} expiry is outside 14-35 DTE")
        quantity = _positive_integer(row["quantity"], f"{name}.quantity")
        assumed_multiplier = _positive_integer(
            row["assumed_multiplier"], f"{name}.assumed_multiplier"
        )
        evidence_ids = _identifier_array(row["evidence_ids"], f"{name}.evidence_ids")
        if not evidence_ids or any(item not in evidence for item in evidence_ids):
            raise ManagedResearchInputError(f"{name} evidence is incomplete")
        used_evidence.update(evidence_ids)
        leg_rows = _candidate_legs(
            row["legs"],
            name=name,
            underlying=underlying,
            expiration=expiration,
            strategy=strategy,
            secdefs=secdefs,
            quotes=quotes,
            used_contracts=used_contracts,
        )
        _validate_leg_quote_skew(leg_rows, name)
        _validate_assumed_multiplier(leg_rows, assumed_multiplier, name)
        quotes_available = all(
            leg.get("bid") is not None and leg.get("ask") is not None
            for leg in leg_rows
        )
        raw_expected_payoff = row["expected_payoff_usd"]
        if quotes_available:
            if raw_expected_payoff is None:
                raise ManagedResearchInputError(
                    f"{name} quoted legs require numeric expected_payoff_usd"
                )
            expected_payoff = _exact_money(
                raw_expected_payoff, f"{name}.expected_payoff_usd"
            )
        else:
            if raw_expected_payoff is not None:
                raise ManagedResearchInputError(
                    f"{name} unquoted legs require null expected_payoff_usd"
                )
            expected_payoff = None
        candidate = _candidate_output(
            research_id=_identifier(row["research_id"], f"{name}.research_id"),
            rank=rank,
            underlying=underlying,
            strategy=strategy,
            expiration=expiration,
            dte=dte,
            quantity=quantity,
            assumed_multiplier=assumed_multiplier,
            execution_cost_cap=_money(
                row["execution_cost_cap_usd"], f"{name}.execution_cost_cap_usd"
            ),
            expected_payoff=expected_payoff,
            entry_condition=_text(row["entry_condition"], f"{name}.entry_condition"),
            invalidation_condition=_text(
                row["invalidation_condition"], f"{name}.invalidation_condition"
            ),
            profit_target_condition=_text(
                row["profit_target_condition"], f"{name}.profit_target_condition"
            ),
            stop_loss_condition=_text(
                row["stop_loss_condition"], f"{name}.stop_loss_condition"
            ),
            research_summary=_text(
                row["research_summary"], f"{name}.research_summary"
            ),
            evidence_ids=evidence_ids,
            evidence_hashes=tuple(evidence[item] for item in evidence_ids),
            legs=leg_rows,
            strategy_nav=strategy_nav,
            observed_at=observed_at,
        )
        candidates.append(candidate)
    if used_contracts != set(secdefs) or used_contracts != set(quotes):
        raise ManagedResearchInputError(
            "candidate legs do not exactly cover the run contracts"
        )
    if used_evidence != set(evidence):
        raise ManagedResearchInputError(
            "candidate evidence does not exactly cover the run evidence"
        )
    research_ids = [candidate["research_id"] for candidate in candidates]
    if len(set(research_ids)) != len(research_ids):
        raise ManagedResearchInputError("duplicate research_id")
    return candidates


def _reprice_candidates(
    values: Sequence[object],
    *,
    expected_bindings: Mapping[str, str],
    secdefs: Mapping[int, Mapping[str, object]],
    quotes: Mapping[int, Mapping[str, object]],
    evidence: Mapping[str, str],
    trading_date: date,
    observed_at: datetime,
    strategy_nav: Decimal,
    store_path: str | Path | None,
) -> tuple[list[dict[str, object]], str]:
    if store_path is None:
        raise ManagedResearchInputError("reprice parent store is required")
    parent_model = read_research_top10(store_path=store_path)
    if (
        parent_model.get("status") not in {"AVAILABLE", "DEGRADED"}
        or parent_model.get("trading_date") != trading_date.isoformat()
    ):
        raise ManagedResearchInputError("reprice parent is unavailable")
    parent_candidates = parent_model.get("premarket")
    if not isinstance(parent_candidates, list) or len(parent_candidates) != 10:
        raise ManagedResearchInputError("reprice parent Top-10 is unavailable")
    stages = parent_model.get("stages")
    if not isinstance(stages, list):
        raise ManagedResearchInputError("reprice parent lineage is unavailable")
    parent_stage = next(
        (
            item
            for item in stages
            if isinstance(item, Mapping) and item.get("phase") == PREMARKET_RESEARCH
        ),
        None,
    )
    if not isinstance(parent_stage, Mapping):
        raise ManagedResearchInputError("reprice parent lineage is unavailable")
    parent_hash = _digest(parent_stage.get("content_hash"), "parent content hash")
    if _money(parent_model.get("strategy_nav_usd"), "parent strategy_nav_usd") != strategy_nav:
        raise ManagedResearchInputError("reprice parent strategy NAV differs")

    selectors: list[tuple[str, int]] = []
    for index, raw in enumerate(values, start=1):
        name = f"candidates[{index - 1}]"
        row = _exact_mapping(raw, _REPRICE_SELECTOR_FIELDS, name)
        _candidate_binding(row["bindings"], expected_bindings, name)
        rank = _positive_integer(row["rank"], f"{name}.rank")
        if rank != index:
            raise ManagedResearchInputError("reprice ranks must remain contiguous")
        selectors.append(
            (_identifier(row["research_id"], f"{name}.research_id"), rank)
        )
    parent_selectors = [
        (str(candidate.get("research_id")), int(candidate.get("rank", 0)))
        for candidate in parent_candidates
        if isinstance(candidate, Mapping)
    ]
    if selectors != parent_selectors:
        raise ManagedResearchInputError("reprice selectors differ from parent Top-10")

    used_contracts: set[int] = set()
    used_evidence: set[str] = set()
    output: list[dict[str, object]] = []
    for index, raw_parent in enumerate(parent_candidates):
        if not isinstance(raw_parent, Mapping):
            raise ManagedResearchInputError("reprice parent candidate is invalid")
        parent = deepcopy(dict(raw_parent))
        parent_legs = parent.get("legs")
        if not isinstance(parent_legs, list) or len(parent_legs) != 2:
            raise ManagedResearchInputError("reprice parent legs are invalid")
        refreshed_legs: list[dict[str, object]] = []
        previous_leg_blockers: set[str] = set()
        for leg_index, raw_leg in enumerate(parent_legs):
            if not isinstance(raw_leg, Mapping):
                raise ManagedResearchInputError("reprice parent leg is invalid")
            previous_leg_blockers.update(
                _blockers(
                    raw_leg.get("blockers"),
                    f"parent[{index}].legs[{leg_index}].blockers",
                )
            )
            contract_id = _positive_integer(
                raw_leg.get("contract_id"),
                f"parent[{index}].legs[{leg_index}].contract_id",
            )
            if contract_id in used_contracts:
                raise ManagedResearchInputError("duplicate parent contract identity")
            used_contracts.add(contract_id)
            secdef = secdefs.get(contract_id)
            quote = quotes.get(contract_id)
            if secdef is None or quote is None:
                raise ManagedResearchInputError("reprice contract evidence is incomplete")
            _require_parent_secdef(raw_leg, secdef, index=index, leg_index=leg_index)
            refreshed_legs.append(
                _leg_output(secdef, quote, side=str(raw_leg.get("side")))
            )
        _validate_leg_quote_skew(refreshed_legs, f"parent[{index}]")
        evidence_ids = tuple(str(item) for item in parent.get("evidence_ids", ()))
        evidence_hashes = tuple(str(item) for item in parent.get("evidence_hashes", ()))
        if not evidence_ids or len(evidence_ids) != len(evidence_hashes):
            raise ManagedResearchInputError("reprice parent evidence is incomplete")
        if tuple(evidence.get(item) for item in evidence_ids) != evidence_hashes:
            raise ManagedResearchInputError("reprice evidence differs from parent")
        used_evidence.update(evidence_ids)
        execution_cost = _money(
            parent.get("execution_cost_cap_usd"), "parent execution_cost_cap_usd"
        )
        parent_verified = parent.get("risk_cap_verified") is True
        expected_payoff: Decimal | None
        if parent_verified:
            assumed_multiplier = _verified_leg_multiplier(refreshed_legs)
            previous_debit = _money(
                parent.get("entry_debit_usd"),
                "parent entry_debit_usd",
            )
            previous_after_cost = _money(
                parent.get("cost_after_ev_usd"),
                "parent cost_after_ev_usd",
            )
            expected_payoff = previous_debit + execution_cost + previous_after_cost
        else:
            assumed_multiplier = _positive_integer(
                parent.get("assumed_multiplier"), "parent assumed_multiplier"
            )
            _validate_assumed_multiplier(
                refreshed_legs,
                assumed_multiplier,
                f"parent[{index}]",
            )
            previous_debit_value = parent.get("indicative_entry_debit_usd")
            previous_after_cost_value = parent.get("indicative_cost_after_ev_usd")
            if previous_debit_value is None and previous_after_cost_value is None:
                expected_payoff = None
            elif previous_debit_value is None or previous_after_cost_value is None:
                raise ManagedResearchInputError(
                    "reprice parent expected payoff evidence is incomplete"
                )
            else:
                previous_debit = _money(
                    previous_debit_value,
                    "parent indicative_entry_debit_usd",
                )
                previous_after_cost = _money(
                    previous_after_cost_value,
                    "parent indicative_cost_after_ev_usd",
                )
                expected_payoff = (
                    previous_debit + execution_cost + previous_after_cost
                )
        economics = _indicative_economics(
            refreshed_legs,
            assumed_multiplier=assumed_multiplier,
            quantity=_positive_integer(parent.get("quantity"), "parent quantity"),
            execution_cost=execution_cost,
            expected_payoff=expected_payoff,
            strategy_nav=strategy_nav,
        )
        if parent_verified:
            economic_projection: dict[str, object] = {
                "entry_debit_usd": economics["debit"],
                "indicative_entry_debit_usd": None,
                "maximum_loss_usd": economics["maximum_loss"],
                "indicative_maximum_loss_usd": None,
                "cost_after_ev_usd": economics["after_cost_ev"],
                "indicative_cost_after_ev_usd": None,
                "risk_cap_verified": True,
            }
        else:
            economic_projection = {
                "entry_debit_usd": None,
                "indicative_entry_debit_usd": economics["debit"],
                "maximum_loss_usd": None,
                "indicative_maximum_loss_usd": economics["maximum_loss"],
                "cost_after_ev_usd": None,
                "indicative_cost_after_ev_usd": economics["after_cost_ev"],
                "risk_cap_verified": False,
            }
        persistent_blockers = list(
            _persistent_parent_blockers(
                parent.get("blockers"),
                previous_leg_blockers=previous_leg_blockers,
                name=f"parent[{index}].blockers",
            )
        )
        if expected_payoff is None:
            persistent_blockers.append("EXPECTED_PAYOFF_UNAVAILABLE")
        parent.update(
            {
                **economic_projection,
                "trade_status": "NO_TRADE",
                "legs": refreshed_legs,
                "blockers": _candidate_blockers(
                    refreshed_legs,
                    include_unverified=not parent_verified,
                    persistent=persistent_blockers,
                ),
                **_AUTHORITY,
            }
        )
        output.append(parent)
    if used_contracts != set(secdefs) or used_contracts != set(quotes):
        raise ManagedResearchInputError("reprice contracts differ from parent Top-10")
    if used_evidence != set(evidence):
        raise ManagedResearchInputError("reprice evidence set differs from parent")
    return output, parent_hash


def _candidate_legs(
    value: object,
    *,
    name: str,
    underlying: str,
    expiration: date,
    strategy: str,
    secdefs: Mapping[int, Mapping[str, object]],
    quotes: Mapping[int, Mapping[str, object]],
    used_contracts: set[int],
) -> list[dict[str, object]]:
    rows = _array(value, f"{name}.legs")
    if len(rows) != 2:
        raise ManagedResearchInputError(f"{name} must contain two legs")
    output: list[dict[str, object]] = []
    for index, raw in enumerate(rows):
        row = _exact_mapping(raw, _LEG_REFERENCE_FIELDS, f"{name}.legs[{index}]")
        contract_id = _positive_integer(
            row["contract_id"], f"{name}.legs[{index}].contract_id"
        )
        if contract_id in used_contracts:
            raise ManagedResearchInputError("contract identity reused across candidates")
        used_contracts.add(contract_id)
        secdef = secdefs.get(contract_id)
        quote = quotes.get(contract_id)
        if secdef is None or quote is None:
            raise ManagedResearchInputError(f"{name} leg evidence is incomplete")
        if secdef["underlying"] != underlying or secdef["expiration"] != expiration.isoformat():
            raise ManagedResearchInputError(f"{name} leg identity differs from candidate")
        side = _choice(row["side"], {"BUY", "SELL"}, f"{name}.legs[{index}].side")
        output.append(_leg_output(secdef, quote, side=side))
    sides = {leg["side"] for leg in output}
    if sides != {"BUY", "SELL"}:
        raise ManagedResearchInputError(f"{name} must contain one BUY and one SELL")
    buy = next(leg for leg in output if leg["side"] == "BUY")
    sell = next(leg for leg in output if leg["side"] == "SELL")
    expected_right = "C" if strategy == "BULL_CALL_VERTICAL" else "P"
    if buy["right"] != expected_right or sell["right"] != expected_right:
        raise ManagedResearchInputError(f"{name} option rights do not match strategy")
    buy_strike = _positive_decimal(buy["strike"], "buy strike")
    sell_strike = _positive_decimal(sell["strike"], "sell strike")
    if strategy == "BULL_CALL_VERTICAL" and not buy_strike < sell_strike:
        raise ManagedResearchInputError(f"{name} bull call strikes are invalid")
    if strategy == "BEAR_PUT_VERTICAL" and not buy_strike > sell_strike:
        raise ManagedResearchInputError(f"{name} bear put strikes are invalid")
    return output


def _validate_leg_quote_skew(
    legs: Sequence[Mapping[str, object]],
    name: str,
) -> None:
    collected = [
        _timestamp(leg.get("collected_at"), f"{name}.collected_at") for leg in legs
    ]
    if max(collected) - min(collected) > _QUOTE_MAX_SKEW:
        raise ManagedResearchInputError(f"{name} collected quote skew is excessive")
    quote_asof = [
        _timestamp(leg.get("quote_asof"), f"{name}.quote_asof")
        for leg in legs
        if leg.get("quote_asof") is not None
    ]
    if len(quote_asof) == len(legs) and max(quote_asof) - min(quote_asof) > _QUOTE_MAX_SKEW:
        raise ManagedResearchInputError(f"{name} exchange quote skew is excessive")


def _validate_assumed_multiplier(
    legs: Sequence[Mapping[str, object]],
    assumed_multiplier: int,
    name: str,
) -> None:
    multipliers = [leg.get("multiplier") for leg in legs]
    known = {int(value) for value in multipliers if value is not None}
    if len(known) > 1 or (known and assumed_multiplier not in known):
        raise ManagedResearchInputError(f"{name} assumed multiplier differs from secdefs")
    if any(value is None for value in multipliers):
        if assumed_multiplier != _POLICY_ASSUMED_MULTIPLIER:
            raise ManagedResearchInputError(
                f"{name} unknown multiplier requires the policy assumption"
            )


def _verified_leg_multiplier(legs: Sequence[Mapping[str, object]]) -> int:
    multipliers = [leg.get("multiplier") for leg in legs]
    if any(value is None for value in multipliers):
        raise ManagedResearchInputError("verified parent multiplier is unavailable")
    known = {int(value) for value in multipliers}
    if known != {_POLICY_ASSUMED_MULTIPLIER}:
        raise ManagedResearchInputError("verified parent multipliers are inconsistent")
    return _POLICY_ASSUMED_MULTIPLIER


def _candidate_blockers(
    legs: Sequence[Mapping[str, object]],
    *,
    include_unverified: bool,
    persistent: Sequence[str] = (),
) -> list[str]:
    blockers: list[str] = list(persistent)
    if include_unverified:
        blockers.extend(_CANDIDATE_BLOCKERS)
    for leg in legs:
        blockers.extend(str(item) for item in leg.get("blockers", ()))
    return list(dict.fromkeys(blockers))


def _persistent_parent_blockers(
    value: object,
    *,
    previous_leg_blockers: set[str],
    name: str,
) -> tuple[str, ...]:
    transient = {
        *_CANDIDATE_BLOCKERS,
        *_REFRESHABLE_CANDIDATE_BLOCKERS,
        *previous_leg_blockers,
    }
    return tuple(item for item in _blockers(value, name) if item not in transient)


def _candidate_output(
    *,
    research_id: str,
    rank: int,
    underlying: str,
    strategy: str,
    expiration: date,
    dte: int,
    quantity: int,
    assumed_multiplier: int,
    execution_cost_cap: Decimal,
    expected_payoff: Decimal | None,
    entry_condition: str,
    invalidation_condition: str,
    profit_target_condition: str,
    stop_loss_condition: str,
    research_summary: str,
    evidence_ids: Sequence[str],
    evidence_hashes: Sequence[str],
    legs: list[dict[str, object]],
    strategy_nav: Decimal,
    observed_at: datetime,
) -> dict[str, object]:
    for leg in legs:
        collected_at = _timestamp(leg["collected_at"], "leg.collected_at")
        if collected_at > observed_at:
            raise ManagedResearchInputError("leg collection exceeds candidate observation")
    quotes_available = all(
        leg.get("bid") is not None and leg.get("ask") is not None for leg in legs
    )
    economics = (
        _indicative_economics(
            legs,
            assumed_multiplier=assumed_multiplier,
            quantity=quantity,
            execution_cost=execution_cost_cap,
            expected_payoff=expected_payoff,
            strategy_nav=strategy_nav,
        )
        if quotes_available
        else None
    )
    return {
        "research_id": research_id,
        "rank": rank,
        "underlying": underlying,
        "strategy_type": strategy,
        "expiration": expiration.isoformat(),
        "dte": dte,
        "quantity": quantity,
        "entry_debit_usd": None,
        "indicative_entry_debit_usd": None if economics is None else economics["debit"],
        "execution_cost_cap_usd": _money_text(execution_cost_cap),
        "maximum_loss_usd": None,
        "indicative_maximum_loss_usd": (
            None if economics is None else economics["maximum_loss"]
        ),
        "cost_after_ev_usd": None,
        "indicative_cost_after_ev_usd": (
            None if economics is None else economics["after_cost_ev"]
        ),
        "assumed_multiplier": assumed_multiplier,
        "risk_cap_verified": False,
        "trade_status": "NO_TRADE",
        "entry_condition": entry_condition,
        "invalidation_condition": invalidation_condition,
        "profit_target_condition": profit_target_condition,
        "stop_loss_condition": stop_loss_condition,
        "research_summary": research_summary,
        "evidence_ids": list(evidence_ids),
        "evidence_hashes": list(evidence_hashes),
        "blockers": _candidate_blockers(
            legs,
            include_unverified=True,
            persistent=(
                ("EXPECTED_PAYOFF_UNAVAILABLE",)
                if expected_payoff is None
                else ()
            ),
        ),
        "legs": legs,
        **_AUTHORITY,
    }


def _indicative_economics(
    legs: Sequence[Mapping[str, object]],
    *,
    assumed_multiplier: int,
    quantity: int,
    execution_cost: Decimal,
    expected_payoff: Decimal | None,
    strategy_nav: Decimal,
) -> dict[str, str | None]:
    buy = next((leg for leg in legs if leg.get("side") == "BUY"), None)
    sell = next((leg for leg in legs if leg.get("side") == "SELL"), None)
    if buy is None or sell is None:
        raise ManagedResearchInputError("vertical sides are incomplete")
    debit = ((
        _positive_decimal(buy.get("ask"), "buy ask")
        - _nonnegative_decimal(sell.get("bid"), "sell bid")
    ) * Decimal(assumed_multiplier) * Decimal(quantity)).quantize(
        _CENT, rounding=ROUND_HALF_UP
    )
    if debit <= 0:
        raise ManagedResearchInputError("indicative debit must be positive")
    width = abs(
        _positive_decimal(buy.get("strike"), "buy strike")
        - _positive_decimal(sell.get("strike"), "sell strike")
    )
    maximum_payout = width * Decimal(assumed_multiplier) * Decimal(quantity)
    if debit > maximum_payout:
        raise ManagedResearchInputError("indicative debit exceeds vertical width")
    if expected_payoff is not None and expected_payoff > maximum_payout:
        raise ManagedResearchInputError(
            "expected payoff exceeds vertical maximum payout"
        )
    maximum_loss = debit + execution_cost
    if maximum_loss > strategy_nav * Decimal("0.10"):
        raise ManagedResearchInputError("indicative maximum loss exceeds the 10% cap")
    after_cost_ev = (
        None if expected_payoff is None else expected_payoff - debit - execution_cost
    )
    if after_cost_ev is not None and after_cost_ev <= 0:
        raise ManagedResearchInputError("indicative after-cost EV must be positive")
    return {
        "debit": _money_text(debit),
        "maximum_loss": _money_text(maximum_loss),
        "after_cost_ev": None if after_cost_ev is None else _money_text(after_cost_ev),
    }


def _leg_output(
    secdef: Mapping[str, object],
    quote: Mapping[str, object],
    *,
    side: str,
) -> dict[str, object]:
    blockers = list(
        dict.fromkeys(
            [
                *[str(item) for item in secdef.get("blockers", ())],
                *[str(item) for item in quote.get("blockers", ())],
            ]
        )
    )
    return {
        "contract_id": secdef["contract_id"],
        "contract_id_ex": secdef["contract_id_ex"],
        "side": side,
        "right": secdef["right"],
        "strike": secdef["strike"],
        "expiration": secdef["expiration"],
        "exchange": secdef["exchange"],
        "trading_class": secdef["trading_class"],
        "local_symbol": secdef["local_symbol"],
        "multiplier": secdef["multiplier"],
        "standard_or_adjusted": secdef["standard_or_adjusted"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "collected_at": quote["collected_at"],
        "quote_asof": quote["quote_asof"],
        "implied_volatility": quote["implied_volatility"],
        "delta": quote["delta"],
        "gamma": quote["gamma"],
        "theta": quote["theta"],
        "vega": quote["vega"],
        "volume": quote["volume"],
        "open_interest": quote["open_interest"],
        "market_data_type": quote["market_data_type"],
        "identity_evidence_hash": secdef["identity_evidence_hash"],
        "quote_evidence_hash": quote["quote_evidence_hash"],
        "blockers": blockers,
    }


def _require_parent_secdef(
    parent: Mapping[str, object],
    secdef: Mapping[str, object],
    *,
    index: int,
    leg_index: int,
) -> None:
    fields = (
        "contract_id",
        "contract_id_ex",
        "right",
        "strike",
        "expiration",
        "exchange",
        "trading_class",
        "local_symbol",
        "multiplier",
        "standard_or_adjusted",
        "identity_evidence_hash",
    )
    if any(parent.get(field) != secdef.get(field) for field in fields):
        raise ManagedResearchInputError(
            f"reprice parent identity differs at candidate {index} leg {leg_index}"
        )


def _candidate_binding(
    value: object,
    expected: Mapping[str, str],
    name: str,
) -> None:
    row = _exact_mapping(value, _BINDING_FIELDS, f"{name}.bindings")
    if any(
        _digest(row[field], f"{name}.bindings.{field}") != expected[field]
        for field in _BINDING_FIELDS
    ):
        raise ManagedResearchInputError(f"{name} component binding mismatch")


def _run_binding(
    value: Mapping[str, object],
    *,
    run_id: str,
    batch_id: str,
    name: str,
) -> None:
    if value.get("run_id") != run_id or value.get("batch_id") != batch_id:
        raise ManagedResearchInputError(f"{name} run or batch binding mismatch")


def _validate_slot(phase: str, observed_at: datetime, trading_date: date) -> None:
    observed_et = observed_at.astimezone(_NEW_YORK)
    if observed_et.date() != trading_date:
        raise ManagedResearchInputError("observed_at and trading_date differ")
    if phase == PREMARKET_RESEARCH:
        start, end = time(9, 20), time(9, 30)
    else:
        start, end = time(9, 35), time(9, 45)
    observed_time = observed_et.timetz().replace(tzinfo=None)
    if not start <= observed_time < end:
        raise ManagedResearchInputError("managed research phase is outside its slot window")


def _bounded_timestamp(
    value: object,
    *,
    name: str,
    trading_date: date,
    upper_bound: datetime,
) -> datetime:
    timestamp = _timestamp(value, name)
    if timestamp.astimezone(_NEW_YORK).date() != trading_date:
        raise ManagedResearchInputError(f"{name} is from another trading date")
    if timestamp > upper_bound:
        raise ManagedResearchInputError(f"{name} exceeds the run observation")
    return timestamp


def _optional_fact(
    value: object,
    blockers: Sequence[str],
    required_blocker: str,
    name: str,
) -> None:
    if value is None and required_blocker not in blockers:
        raise ManagedResearchInputError(f"{name} requires {required_blocker}")
    if value is not None and required_blocker in blockers:
        raise ManagedResearchInputError(f"{name} conflicts with {required_blocker}")


def _exact_mapping(value: object, fields: set[str], name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ManagedResearchInputError(f"{name} fields are incomplete")
    if not all(isinstance(key, str) for key in value):
        raise ManagedResearchInputError(f"{name} keys are invalid")
    return value


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ManagedResearchInputError(f"{name} must be an array")
    return value


def _identifier_array(value: object, name: str) -> tuple[str, ...]:
    rows = _array(value, name)
    result = tuple(_identifier(item, f"{name}[{index}]") for index, item in enumerate(rows))
    if len(set(result)) != len(result):
        raise ManagedResearchInputError(f"{name} contains duplicates")
    return result


def _identifier(value: object, name: str) -> str:
    text_value = _text(value, name)
    if _IDENTIFIER.fullmatch(text_value) is None:
        raise ManagedResearchInputError(f"{name} is invalid")
    return text_value


def _symbol(value: object, name: str) -> str:
    text_value = _text(value, name).upper()
    if _SYMBOL.fullmatch(text_value) is None:
        raise ManagedResearchInputError(f"{name} is invalid")
    return text_value


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ManagedResearchInputError(f"{name} is not a SHA-256 digest")
    return value


def _blockers(value: object, name: str) -> tuple[str, ...]:
    rows = _array(value, name)
    result: list[str] = []
    for index, item in enumerate(rows):
        if not isinstance(item, str) or _BLOCKER.fullmatch(item) is None:
            raise ManagedResearchInputError(f"{name}[{index}] is invalid")
        result.append(item)
    if len(set(result)) != len(result):
        raise ManagedResearchInputError(f"{name} contains duplicates")
    return tuple(result)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 4096:
        raise ManagedResearchInputError(f"{name} must be non-empty text")
    return value.strip()


def _optional_text(value: object, name: str) -> str | None:
    return None if value is None else _text(value, name)


def _choice(value: object, choices: set[str], name: str) -> str:
    if not isinstance(value, str) or value not in choices:
        raise ManagedResearchInputError(f"{name} is invalid")
    return value


def _date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise ManagedResearchInputError(f"{name} must be a date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ManagedResearchInputError(f"{name} must be a date") from exc


def _timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ManagedResearchInputError(f"{name} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ManagedResearchInputError(f"{name} must be a timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ManagedResearchInputError(f"{name} must be timezone-aware")
    return parsed


def _timestamp_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ManagedResearchInputError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ManagedResearchInputError(f"{name} must be a positive integer") from exc
    if result <= 0 or str(value).strip() not in {str(result), f"{result}.0"}:
        raise ManagedResearchInputError(f"{name} must be a positive integer")
    return result


def _optional_positive_integer(value: object, name: str) -> int | None:
    return None if value is None else _positive_integer(value, name)


def _optional_nonnegative_integer(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ManagedResearchInputError(f"{name} must be a nonnegative integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ManagedResearchInputError(f"{name} must be a nonnegative integer") from exc
    if result < 0 or str(value).strip() not in {str(result), f"{result}.0"}:
        raise ManagedResearchInputError(f"{name} must be a nonnegative integer")
    return result


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ManagedResearchInputError(f"{name} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ManagedResearchInputError(f"{name} must be numeric") from exc
    if not result.is_finite():
        raise ManagedResearchInputError(f"{name} must be finite")
    return result


def _optional_decimal(value: object, name: str) -> Decimal | None:
    return None if value is None else _decimal(value, name)


def _positive_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result <= 0:
        raise ManagedResearchInputError(f"{name} must be positive")
    return result


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ManagedResearchInputError(f"{name} must be nonnegative")
    return result


def _money(value: object, name: str) -> Decimal:
    return _nonnegative_decimal(value, name).quantize(_CENT, rounding=ROUND_HALF_UP)


def _exact_money(value: object, name: str) -> Decimal:
    result = _nonnegative_decimal(value, name)
    if result != result.quantize(_CENT):
        raise ManagedResearchInputError(f"{name} must use exact cents")
    return result


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value)


def _money_text(value: Decimal) -> str:
    return format(value.quantize(_CENT, rounding=ROUND_HALF_UP), ".2f")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ManagedResearchInputError(f"managed research input repeats key {key!r}")
        result[key] = value
    return result


__all__ = [
    "MANAGED_RESEARCH_RUN_SCHEMA",
    "MANAGED_RESEARCH_RUN_VERSION",
    "ManagedResearchInputError",
    "assemble_managed_research",
    "atomic_write_research_envelope",
    "load_managed_research_run",
]
