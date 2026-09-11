from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
from io import StringIO
import json
from pathlib import Path
import threading

import pytest

from options_copilot.learning.markers import (
    ApprovedHumanAuthorityKeyring,
    AuthorityValidationError,
    PromotionAuthority,
    RollbackAuthority,
)
from options_copilot.learning.governance_cli import build_parser, main as governance_cli_main
from options_copilot.governance.contracts import create_correction, load_contract
from options_copilot.learning.policy_authority import (
    AuthorityConflict,
    CurrentPolicyResolver,
    PolicyAuthorityError,
    PolicyAuthorityLedger,
    PolicyAuthorityTampered,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
FIXTURE_SECRET = b"non-production-p9-policy-fixture-key"


class FixtureSignatureVerifier:
    trust_domain = "TEST_ONLY"

    def verify(self, *, signer_key_id: str, signature_algorithm: str, message: bytes, signature: str) -> bool:
        if signer_key_id != "test-only:p9-policy-fixture" or signature_algorithm != "TEST_ONLY_SHA256":
            return False
        return signature == hashlib.sha256(FIXTURE_SECRET + message).hexdigest()


FIXTURE_VERIFIER = FixtureSignatureVerifier()
INITIAL = Path("options_copilot/governance/initial_champion_scenario_policy.v1.json")
INITIAL_SOURCE_HASH = hashlib.sha256(INITIAL.read_bytes()).hexdigest()
INITIAL_POLICY_HASH = "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
POLICY2_PAYLOAD = {"calibration": {}, "fixture": True}
H = {name: character * 64 for name, character in {
    "evaluation": "3", "dataset": "4", "independence": "5",
    "cost": "6", "risk": "7", "proposal": "8", "candidate": "9",
    "ranking": "a",
}.items()}
H["policy2"] = canonical_hash(POLICY2_PAYLOAD)


def _open_ledger(path: Path, *, initial_policy: Path = INITIAL) -> PolicyAuthorityLedger:
    return PolicyAuthorityLedger(
        path,
        initial_policy_path=initial_policy,
        signature_verifier=FIXTURE_VERIFIER,
        allow_test_authority=True,
    )


def _attest(body: dict[str, object]) -> dict[str, object]:
    body = dict(body)
    body["signer_key_id"] = "test-only:p9-policy-fixture"
    body["signature_algorithm"] = "TEST_ONLY_SHA256"
    body["governance_signature"] = hashlib.sha256(
        FIXTURE_SECRET + canonical_json(body).encode("utf-8")
    ).hexdigest()
    body["content_hash"] = canonical_hash(body)
    return body


def _promotion(sequence: int, previous: str, *, policy_hash: str = H["policy2"]) -> PromotionAuthority:
    return PromotionAuthority.from_dict(_attest({
        "schema": "options_copilot.learning.promotion_authority.v1", "phase": "P9",
        "decision": "PROMOTE_CHALLENGER", "sequence": sequence,
        "actor": "human:test-fixture", "signed_at": NOW.isoformat(),
        "prior_policy_version": "v1", "prior_policy_hash": INITIAL_POLICY_HASH,
        "current_policy_version": "v2", "current_policy_hash": policy_hash,
        "initial_policy_source_hash": INITIAL_SOURCE_HASH,
        "evaluation_report_hash": H["evaluation"], "reference_dataset_hash": H["dataset"],
        "independence_hash": H["independence"], "execution_cost_hash": H["cost"],
        "risk_contract_hash": H["risk"], "reason": "locked non-production fixture",
        "previous_authority_hash": previous,
    }), signature_verifier=FIXTURE_VERIFIER, allow_test_authority=True)


def _rollback(sequence: int, previous: str, *, prior_version: str = "v2", prior_hash: str = H["policy2"]) -> RollbackAuthority:
    return RollbackAuthority.from_dict(_attest({
        "schema": "options_copilot.learning.rollback_authority.v1", "phase": "P9",
        "decision": "ROLLBACK_POLICY", "sequence": sequence,
        "actor": "human:test-fixture", "signed_at": (NOW + timedelta(minutes=1)).isoformat(),
        "prior_policy_version": prior_version, "prior_policy_hash": prior_hash,
        "current_policy_version": "v1", "current_policy_hash": INITIAL_POLICY_HASH,
        "initial_policy_source_hash": INITIAL_SOURCE_HASH,
        "evaluation_report_hash": H["evaluation"], "reference_dataset_hash": H["dataset"],
        "independence_hash": H["independence"], "execution_cost_hash": H["cost"],
        "risk_contract_hash": H["risk"], "reason": "fixture rollback",
        "previous_authority_hash": previous,
    }), signature_verifier=FIXTURE_VERIFIER, allow_test_authority=True)


def _a_grade(sequence: int, previous: str, policy_marker: str) -> dict[str, object]:
    return _attest({
        "schema": "options_copilot.learning.a_grade_authority.v1", "phase": "P9",
        "decision": "APPROVE_A_GRADE", "version": "v1", "sequence": sequence,
        "append_only": True, "proposal_hash": H["proposal"], "candidate_hash": H["candidate"],
        "current_policy_version": "v2", "current_policy_hash": H["policy2"],
        "policy_authority_marker_hash": policy_marker,
        "execution_cost_version": "v1", "execution_cost_hash": H["cost"],
        "ranking_basis_hash": H["ranking"], "evaluation_report_hash": H["evaluation"],
        "reference_dataset_hash": H["dataset"], "independence_hash": H["independence"],
        "risk_contract_hash": H["risk"], "actor": "human:test-fixture",
        "signed_at": NOW.isoformat(), "expires_at": (NOW + timedelta(hours=1)).isoformat(),
        "previous_authority_hash": previous, "revoked": False, "rolled_back": False,
    })


def test_initial_policy_is_immutable_fallback_and_restart_replays_promotion(tmp_path: Path) -> None:
    before = INITIAL.read_bytes()
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as ledger:
        assert ledger.journal_mode == "wal"
        assert ledger.synchronous == "full"
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        assert fallback.current_policy_version == "v1"
        promotion = _promotion(1, fallback.policy_authority_marker_hash)
        ledger.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)
        promoted = CurrentPolicyResolver(ledger).resolve(now=NOW)
        assert promoted.current_policy_version == "v2"
        assert promoted.current_policy_hash == H["policy2"]
        assert promoted.policy_authority_marker_hash == promotion.content_hash

    with _open_ledger(db) as restarted:
        replayed = CurrentPolicyResolver(restarted).resolve(now=NOW)
        assert replayed.current_policy_version == "v2"
        assert replayed.policy_authority_marker_hash == promotion.content_hash
    assert INITIAL.read_bytes() == before


def _signed_policy_v2():
    prior = load_contract(INITIAL)
    return create_correction(
        prior,
        version="v2",
        effective_at=NOW - timedelta(minutes=2),
        provenance=prior.provenance,
        payload=prior.payload,
        actor=prior.actor,
        signed_at=NOW - timedelta(minutes=1),
    )


def test_policy_contract_document_returns_immutable_initial_signed_contract(
    tmp_path: Path,
) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        resolver = CurrentPolicyResolver(ledger)
        resolved = resolver.resolve(now=NOW)

        document = resolver.policy_contract_document(resolved)

        assert document == load_contract(INITIAL).to_dict()
        assert document["version"] == resolved.current_policy_version
        assert document["contract_hash"] == resolved.current_policy_hash
        assert "governance_signature" not in document


def test_policy_contract_document_returns_complete_signed_promoted_contract(
    tmp_path: Path,
) -> None:
    contract = _signed_policy_v2()
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        resolver = CurrentPolicyResolver(ledger)
        initial = resolver.resolve(now=NOW)
        promotion = _promotion(
            1,
            initial.policy_authority_marker_hash,
            policy_hash=contract.contract_hash,
        )
        ledger.append_promotion(
            promotion,
            policy_payload=contract.to_dict(),
        )
        promoted = resolver.resolve(now=NOW)

        document = resolver.policy_contract_document(promoted)

        assert document == contract.to_dict()
        assert document["version"] == promoted.current_policy_version
        assert document["contract_hash"] == promoted.current_policy_hash


def test_raw_promoted_payload_remains_resolvable_but_has_no_production_contract(
    tmp_path: Path,
) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        resolver = CurrentPolicyResolver(ledger)
        initial = resolver.resolve(now=NOW)
        promotion = _promotion(1, initial.policy_authority_marker_hash)
        ledger.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)

        promoted = resolver.resolve(now=NOW)

        assert promoted.payload["fixture"] is True
        assert resolver.policy_contract_document(promoted) is None


def test_policy_contract_document_rejects_a_concurrently_superseded_resolution(
    tmp_path: Path,
) -> None:
    database = tmp_path / "authority.sqlite3"
    contract = _signed_policy_v2()
    with _open_ledger(database) as reader:
        resolver = CurrentPolicyResolver(reader)
        stale = resolver.resolve(now=NOW)
        promotion = _promotion(
            1,
            stale.policy_authority_marker_hash,
            policy_hash=contract.contract_hash,
        )
        with _open_ledger(database) as writer:
            writer.append_promotion(
                promotion,
                policy_payload=contract.to_dict(),
            )

        assert resolver.policy_contract_document(stale) is None


def test_rollback_atomically_restores_initial_and_revokes_bound_a_grade(tmp_path: Path) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        promotion = _promotion(1, fallback.policy_authority_marker_hash)
        ledger.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)
        marker = _a_grade(2, promotion.content_hash, promotion.content_hash)
        ledger.append_a_grade(marker)
        assert ledger.is_a_grade_active(str(marker["content_hash"]), asof=NOW) is True
        rollback = _rollback(3, str(marker["content_hash"]))
        ledger.append_rollback(rollback)

        resolved = CurrentPolicyResolver(ledger).resolve(now=NOW + timedelta(minutes=1))
        assert resolved.current_policy_version == "v1"
        assert resolved.policy_authority_marker_hash == rollback.content_hash
        assert ledger.is_a_grade_active(str(marker["content_hash"])) is False


def test_current_a_grade_marker_is_only_the_verified_authority_head(
    tmp_path: Path,
) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        assert ledger.current_a_grade_marker() is None
        initial = CurrentPolicyResolver(ledger).resolve(now=NOW)
        promotion = _promotion(1, initial.policy_authority_marker_hash)
        ledger.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)
        assert ledger.current_a_grade_marker() is None

        marker = _a_grade(2, promotion.content_hash, promotion.content_hash)
        ledger.append_a_grade(marker)
        assert ledger.current_a_grade_marker() == marker

        ledger.append_rollback(_rollback(3, str(marker["content_hash"])))
        assert ledger.current_a_grade_marker() is None


def test_a_grade_active_read_is_one_serialized_snapshot_against_concurrent_rollback(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as reader:
        fallback = CurrentPolicyResolver(reader).resolve(now=NOW)
        promotion = _promotion(1, fallback.policy_authority_marker_hash)
        reader.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)
        marker = _a_grade(2, promotion.content_hash, promotion.content_hash)
        reader.append_a_grade(marker)
        rollback = _rollback(3, str(marker["content_hash"]))
        head_read = threading.Event()
        writer_started = threading.Event()
        writer_done = threading.Event()

        def after_head() -> None:
            head_read.set()
            assert writer_started.wait(timeout=2)
            assert writer_done.wait(timeout=0.05) is False

        def writer() -> None:
            assert head_read.wait(timeout=2)
            writer_started.set()
            with _open_ledger(db) as other:
                other.append_rollback(rollback)
            writer_done.set()

        reader._after_active_head_read = after_head
        thread = threading.Thread(target=writer)
        thread.start()
        assert reader.is_a_grade_active(str(marker["content_hash"]), asof=NOW) is True
        thread.join(timeout=3)
        assert writer_done.is_set()
        reader._after_active_head_read = lambda: None
        assert reader.is_a_grade_active(str(marker["content_hash"]), asof=NOW) is False


def test_current_policy_guard_blocks_second_connection_writer_and_rechecks_head(
    tmp_path: Path,
) -> None:
    db = tmp_path / "guarded-authority.sqlite3"
    left = _open_ledger(db)
    right = _open_ledger(db)
    entered, release = threading.Event(), threading.Event()
    try:
        resolver = CurrentPolicyResolver(left)
        resolved = resolver.resolve(now=NOW)
        promotion = _promotion(1, resolved.policy_authority_marker_hash)

        def guarded() -> object | None:
            return resolver.guard_current(
                resolved,
                callback=lambda: (
                    entered.set(),
                    release.wait(timeout=5),
                    "approved",
                )[-1],
            )

        with ThreadPoolExecutor(max_workers=2) as workers:
            guard_future = workers.submit(guarded)
            assert entered.wait(timeout=5)
            writer = workers.submit(
                right.append_promotion,
                promotion,
                policy_payload=POLICY2_PAYLOAD,
            )
            assert not writer.done()
            release.set()
            assert guard_future.result(timeout=5) == "approved"
            writer.result(timeout=5)

        callbacks: list[str] = []
        assert resolver.guard_current(
            resolved,
            callback=lambda: callbacks.append("called"),
        ) is None
        assert callbacks == []
    finally:
        left.close()
        right.close()


def test_tampered_chain_fails_closed_after_restart(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        ledger.append_promotion(
            _promotion(1, fallback.policy_authority_marker_hash),
            policy_payload=POLICY2_PAYLOAD,
        )
        ledger.connection.execute("UPDATE authority_events SET content_hash=? WHERE sequence=1", ("f" * 64,))
        ledger.connection.commit()

    with _open_ledger(db) as restarted:
        with pytest.raises(PolicyAuthorityTampered):
            CurrentPolicyResolver(restarted).resolve(now=NOW)


def test_payload_and_payload_hash_double_tamper_still_fails_authority_binding(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        ledger.append_promotion(
            _promotion(1, fallback.policy_authority_marker_hash),
            policy_payload=POLICY2_PAYLOAD,
        )
        attacker_payload = {"calibration": {}, "fixture": False, "attacker": True}
        ledger.connection.execute(
            "UPDATE authority_events SET policy_payload_json=?, policy_payload_hash=? WHERE sequence=1",
            (canonical_json(attacker_payload), canonical_hash(attacker_payload)),
        )
        ledger.connection.commit()

    with _open_ledger(db) as restarted:
        with pytest.raises(PolicyAuthorityTampered, match="does not match its authority"):
            restarted.verify_integrity()


def test_two_concurrent_promotions_from_one_head_have_one_winner(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as bootstrap:
        head = CurrentPolicyResolver(bootstrap).resolve(now=NOW).policy_authority_marker_hash
    outcomes: list[str] = []
    lock = threading.Lock()

    def worker(payload: dict[str, object]) -> None:
        policy_hash = canonical_hash(payload)
        try:
            with _open_ledger(db) as ledger:
                ledger.append_promotion(_promotion(1, head, policy_hash=policy_hash), policy_payload=payload)
            value = "ok"
        except AuthorityConflict:
            value = "conflict"
        with lock:
            outcomes.append(value)

    threads = [
        threading.Thread(target=worker, args=({"calibration": {}, "variant": character},))
        for character in ("2", "b")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["conflict", "ok"]


def test_crash_before_commit_leaves_no_partial_policy_change(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    with _open_ledger(db) as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        artifact = tmp_path / "promotion.json"
        ledger._before_commit = lambda: (_ for _ in ()).throw(RuntimeError("fixture crash"))
        with pytest.raises(RuntimeError, match="fixture crash"):
            ledger.append_promotion(
                _promotion(1, fallback.policy_authority_marker_hash),
                policy_payload=POLICY2_PAYLOAD,
                artifact_output=artifact,
            )
        ledger._before_commit = lambda: None
        assert CurrentPolicyResolver(ledger).resolve(now=NOW).current_policy_version == "v1"
        assert not artifact.exists()


def test_artifact_output_failure_rolls_back_ledger_head(tmp_path: Path) -> None:
    db = tmp_path / "authority.sqlite3"
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("fixture", encoding="utf-8")
    with _open_ledger(db) as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        with pytest.raises(OSError):
            ledger.append_promotion(
                _promotion(1, fallback.policy_authority_marker_hash),
                policy_payload=POLICY2_PAYLOAD,
                artifact_output=blocker / "promotion.json",
            )
        assert ledger.current_state().sequence == 0
        assert CurrentPolicyResolver(ledger).resolve(now=NOW).current_policy_version == "v1"


def test_wrong_prior_head_and_non_monotonic_sequence_are_rejected(tmp_path: Path) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        fallback = CurrentPolicyResolver(ledger).resolve(now=NOW)
        with pytest.raises(AuthorityConflict, match="previous hash"):
            ledger.append_promotion(_promotion(1, "f" * 64), policy_payload=POLICY2_PAYLOAD)
        with pytest.raises(AuthorityConflict, match="sequence"):
            ledger.append_promotion(_promotion(2, fallback.policy_authority_marker_hash), policy_payload=POLICY2_PAYLOAD)
        assert CurrentPolicyResolver(ledger).resolve(now=NOW).current_policy_version == "v1"


def test_initial_policy_source_mutation_fails_closed(tmp_path: Path) -> None:
    initial_copy = tmp_path / "initial.json"
    initial_copy.write_bytes(INITIAL.read_bytes())
    with _open_ledger(tmp_path / "authority.sqlite3", initial_policy=initial_copy) as ledger:
        initial_copy.write_bytes(initial_copy.read_bytes() + b"\n")
        with pytest.raises(PolicyAuthorityTampered, match="initial policy source changed"):
            CurrentPolicyResolver(ledger).resolve(now=NOW)


def test_resolver_rejects_future_authority_and_stale_initial_fallback(tmp_path: Path) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        resolver = CurrentPolicyResolver(
            ledger,
            maximum_initial_age=timedelta(days=30),
        )
        fallback = resolver.resolve(now=NOW)
        with pytest.raises(PolicyAuthorityError, match="freshness window expired"):
            resolver.resolve(now=fallback.effective_at + timedelta(days=31))
        promotion = _promotion(1, fallback.policy_authority_marker_hash)
        ledger.append_promotion(promotion, policy_payload=POLICY2_PAYLOAD)
        with pytest.raises(PolicyAuthorityError, match="signature is in the future"):
            resolver.resolve(now=NOW - timedelta(seconds=1))


def test_resolver_can_keep_unexpired_immutable_initial_policy_current(
    tmp_path: Path,
) -> None:
    with _open_ledger(tmp_path / "authority.sqlite3") as ledger:
        resolver = CurrentPolicyResolver(ledger, maximum_initial_age=None)
        fallback = resolver.resolve(now=NOW)

        later = resolver.resolve(
            now=fallback.effective_at + timedelta(days=365),
        )

        assert later == fallback


def test_cli_surface_has_only_human_sign_and_verify_verbs_and_no_yes_flag() -> None:
    parser = build_parser()
    subparsers = next(action for action in parser._actions if action.dest == "command")
    assert set(subparsers.choices) == {
        "sign-promotion", "sign-a-grade", "sign-rollback",
        "verify-promotion-decision", "verify-a-grade-decision", "verify-rollback-decision",
    }
    for command_parser in subparsers.choices.values():
        assert all(action.dest != "yes" for action in command_parser._actions)


class _FixtureTTY(StringIO):
    def isatty(self) -> bool:
        return True


def test_cli_rejects_pipe_and_stringio_without_creating_authority(tmp_path: Path) -> None:
    draft = tmp_path / "draft.json"
    draft.write_text(json.dumps({"actor": "human:test-fixture"}), encoding="utf-8")
    output = StringIO()
    code = governance_cli_main(
        [
            "sign-promotion", "--draft", str(draft),
            "--authority-db", str(tmp_path / "authority.sqlite3"),
            "--output", str(tmp_path / "authority.json"),
            "--policy-payload", str(tmp_path / "policy.json"), "--json",
        ],
        stdin=StringIO("anything\n"),
        stdout=output,
        stderr=StringIO(),
    )
    assert code == 4
    assert json.loads(output.getvalue())["error"]["code"] == "INTERACTIVE_TTY_REQUIRED"
    assert not (tmp_path / "authority.json").exists()


def test_policy_ledger_rejects_test_verifier_without_explicit_test_scope(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        AuthorityValidationError,
        match="approved production human verifier",
    ):
        PolicyAuthorityLedger(
            tmp_path / "production.sqlite3",
            signature_verifier=FIXTURE_VERIFIER,
        )


def test_verify_only_cli_fails_closed_without_pinned_production_keyring(
    tmp_path: Path,
) -> None:
    db = tmp_path / "authority.sqlite3"
    artifact = tmp_path / "promotion.json"
    production_shaped = {
        "schema": "options_copilot.learning.promotion_authority.v1",
        "phase": "P9",
        "decision": "PROMOTE_CHALLENGER",
        "sequence": 1,
        "actor": "human:unit-governance",
        "signed_at": NOW.isoformat(),
        "prior_policy_version": "v1",
        "prior_policy_hash": INITIAL_POLICY_HASH,
        "current_policy_version": "v2",
        "current_policy_hash": H["policy2"],
        "initial_policy_source_hash": INITIAL_SOURCE_HASH,
        "evaluation_report_hash": H["evaluation"],
        "reference_dataset_hash": H["dataset"],
        "independence_hash": H["independence"],
        "execution_cost_hash": H["cost"],
        "risk_contract_hash": H["risk"],
        "reason": "untrusted production-shaped fixture",
        "previous_authority_hash": canonical_hash(
            {
                "schema": "options_copilot.learning.policy_authority_genesis.v1",
                "phase": "P9",
                "initial_policy_version": "v1",
                "initial_policy_hash": INITIAL_POLICY_HASH,
                "initial_policy_source_hash": INITIAL_SOURCE_HASH,
            }
        ),
        "signer_key_id": "human-key:unit-governance",
        "signature_algorithm": "ED25519",
        "governance_signature": base64.b64encode(b"\x00" * 64).decode("ascii"),
    }
    production_shaped["content_hash"] = canonical_hash(production_shaped)
    artifact.write_text(json.dumps(production_shaped), encoding="utf-8")
    output = StringIO()

    code = governance_cli_main(
        [
            "verify-promotion-decision",
            "--path",
            str(artifact),
            "--authority-db",
            str(db),
            "--json",
        ],
        stdout=output,
        stderr=StringIO(),
    )

    assert code == 4
    assert json.loads(output.getvalue())["error"]["code"] == (
        "NO_TRUSTED_HUMAN_SIGNER"
    )
    assert not db.exists()


def test_public_keys_cannot_manufacture_a_production_verifier() -> None:
    assert not hasattr(ApprovedHumanAuthorityKeyring, "from_ed25519_public_keys")


def test_test_scope_cannot_promote_a_verifier_that_claims_production(
    tmp_path: Path,
) -> None:
    class ProductionClaimingFixtureVerifier(FixtureSignatureVerifier):
        trust_domain = "PRODUCTION_HUMAN"

    with pytest.raises(AuthorityValidationError, match="NO_TRUSTED_HUMAN_SIGNER"):
        PolicyAuthorityLedger(
            tmp_path / "production.sqlite3",
            signature_verifier=ProductionClaimingFixtureVerifier(),
            allow_test_authority=True,
        )


def test_real_tty_challenge_still_fails_without_trusted_signer_and_never_appends(tmp_path: Path) -> None:
    draft = tmp_path / "draft.json"
    draft.write_text(json.dumps({"actor": "human:test-fixture", "decision": "PROMOTE_CHALLENGER"}), encoding="utf-8")
    stdout = _FixtureTTY()
    db = tmp_path / "authority.sqlite3"
    code = governance_cli_main(
        [
            "sign-promotion", "--draft", str(draft),
            "--authority-db", str(db),
            "--output", str(tmp_path / "authority.json"),
            "--policy-payload", str(tmp_path / "policy.json"), "--json",
        ],
        stdin=_FixtureTTY(),
        stdout=stdout,
        stderr=StringIO(),
    )
    lines = stdout.getvalue().splitlines()
    assert code == 4
    assert any(line.startswith("CHALLENGE_HASH ") for line in lines)
    assert json.loads(lines[-1])["error"]["code"] == "NO_TRUSTED_HUMAN_SIGNER"
    with _open_ledger(db) as ledger:
        assert ledger.current_state().sequence == 0
    assert not (tmp_path / "authority.json").exists()
