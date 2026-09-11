# OptionsCopilot

本地优先、单用户、Windows 环境下的美股 / ETF 期权研究助手。系统通过永久只读的
IBKR 数据链路，结合新闻、事件、基本面和确定性风险计算，组织股票研究池、期权结构池、
候选排名与持仓观察。**系统不提交、修改或撤销券商订单。**

本仓库是 2026-09-12 整理的源码快照：包含应用源码、测试、锁定依赖、运维脚本和模块文档，
不包含账户快照、成交记录、凭据、个人 NAV 授权材料、数据库、日志、备份或本地开发历史。
这不是可直接投入实盘的完整交付；生产特征、历史输入和报价管理链仍有已知限制。

## 文档导航

| 文档 | 内容 |
| --- | --- |
| [功能清单](docs/FEATURES.md) | 按使用场景整理功能，并标明研究 / 条件可用 / 未完成边界 |
| [模块地图](docs/MODULES.md) | 模块职责、源码入口、数据流、API 和测试定位 |
| [当前状态与阻塞](docs/STATUS.md) | 已实现与未验收的区别、已知问题、完整流程验收条件 |
| [发布与隐私边界](docs/PUBLISHING.md) | 上传内容、排除内容、测试范围和源码快照的使用限制 |
| [测试说明](docs/TESTING.md) | 可分发源码的离线测试命令，以及需要本地私有材料的测试 |
| [操作说明](options_copilot/README.md) | 只读 GUI、可选数据源和兼容审批桥接的操作说明 |

功能整理不改变现有代码目录，也不改变交易权限。下面保留本地维护步骤；其中引用的
迁移基线、个人签名和本地账本并不随 GitHub 源码包分发。

## Safety contract

- Research and scanning are review-only. This repository has no authority to
  submit an order or replace the user's final IBKR confirmation.
- IBKR access is permanently `readonly=True`; no environment variable can
  disable that boundary.
- Normal risk is capped at 10% of current NAV, A-grade at 15%, and 20% is an
  absolute rejection line.
- Only one open defined-risk combination is allowed. Normal entries are 14-35
  DTE; 0-3 DTE and sub-7-DTE structures are prohibited.
- Stale, conflicting, incomplete, or unverifiable inputs fail closed to
  observation/`NO_TRADE`.

## Setup

The supported environment is PowerShell 7, CPython 3.12 or newer, and the
project-owned interpreter at `G:\OptionsCopilot\.venv\Scripts\python.exe`.
Use PowerShell 7 from `G:\OptionsCopilot`:

```powershell
Set-Location G:\OptionsCopilot
pwsh.exe -NoProfile -File .\scripts\setup_options_copilot_env.ps1 -Mode Verify
& .\.venv\Scripts\python.exe scripts\verify_locked_environment.py `
    --lock requirements-dev.lock --allow-bootstrap pip --allow-bootstrap setuptools
& .\.venv\Scripts\python.exe -m pip check
```

This normal setup and verification path consumes the committed locks and never runs a
resolver. The target was created fresh and installed only with
`--require-hashes --no-deps -r requirements-dev.lock`; the exact inventory
audit rejects every extra package and version drift. Only `pip` and
`setuptools` are allowed as interpreter-bootstrap distributions. `pip check`
is supplementary dependency-consistency evidence, not proof that the installed
inventory equals the lock.

`Mode Verify` also creates a separate, unique production verification
environment beneath `G:\OptionsCopilot\data\options_copilot\hermetic`, installs
it only with `--require-hashes --no-deps -r requirements.lock`, runs the same
exact inventory audit and `pip check`, and removes that invocation-owned scratch
environment afterward. It never installs production packages into `.venv`.

Lock regeneration is a distinct maintainer action. It requires the target
`.venv` to be absent and refuses to delete or replace a pre-existing target:

```powershell
Set-Location G:\OptionsCopilot
pwsh.exe -NoProfile -File .\scripts\setup_options_copilot_env.ps1 `
    -Mode RegenerateAndCreate
```

`RegenerateAndCreate` uses a separate ignored G-drive bootstrap environment,
installs only the requested resolver tool `pip-tools==7.6.0`, and runs the
equivalent of these attributed lock-generation operations from
`pyproject.toml`:

```text
piptools compile --resolver=backtracking --generate-hashes --strip-extras --output-file=requirements.lock pyproject.toml
piptools compile --resolver=backtracking --generate-hashes --strip-extras --extra=dev --output-file=requirements-dev.lock pyproject.toml
```

It then creates the fresh `G:\OptionsCopilot\.venv` and installs only the
self-contained `requirements-dev.lock`. The resolver bootstrap is never part of
the target or production verification inventory.

Before any `py`, venv, pip, or pip-tools subprocess, the setup script snapshots
the process-scoped `PIP_CACHE_DIR`, `PIP_NO_CACHE_DIR`, `TEMP`, and `TMP`, points
them to unique ignored locations beneath
`G:\OptionsCopilot\data\options_copilot\hermetic`, and uses `--no-cache-dir`.
Its `finally` path restores all caller values on success or failure and removes
only scratch directories created by that invocation. The final sanitized JSON
audit lists the target, bootstrap, production verification, cache, `TEMP`, and
`TMP` paths so their G-drive containment can be inspected.

The migrated provider file is
`data\options_copilot\api_keys.local.json`. It is intentionally ignored by Git
and remains plaintext local configuration. Never paste it into chat, logs,
screenshots, source control, or shell arguments. IBKR credentials never belong
in that file.

## Start

The launcher only binds loopback and refuses to replace an existing listener:

```powershell
pwsh.exe -NoProfile -File .\scripts\start_options_copilot.ps1 -Background -OpenBrowser
```

Default URL: `http://127.0.0.1:8891/`.

Do not start a second copy while the old `G:\quantumtrading` instance is still
using port 8891. Moving the live service to this folder requires a separate,
explicitly supervised cutover and broker/read-only reconciliation.

To inspect only the configured IBKR listener and Python dependency, without an
IB API login or handshake:

```powershell
& .\.venv\Scripts\python.exe .\scripts\probe_options_copilot_ib_gateway.py
```

Production launch performs no implicit install or network resolution. It uses
the already verified project environment; environment creation and dependency
maintenance remain explicit setup actions.

### One-time pacing policy authority

The five conservative IBKR market-data limits are approved once as a signed
`PacingPolicyAuthority`. Schema v2 uses
`LONG_LIVED_UNTIL_REVOKED`: it does not expire after 24 hours, the launcher
auto-discovers the single installed `capability.json` plus `approval.json` on
every start, and the request guard rechecks revocation before every read. The
legacy schema v1 remains compatible and retains its 24-hour expiry.

The human trust root and authority are each installed once from a real
interactive PowerShell terminal. The private-key envelope stays outside the
project and is protected by Windows DPAPI:

```powershell
& .\.venv\Scripts\python.exe -m options_copilot.operations.pacing_trust_bootstrap_cli `
    --private-key-output G:\OptionsCopilotSecrets\pacing-private.dpapi.json `
    --signer-key-id human-key:operator-pacing

& .\.venv\Scripts\python.exe -m options_copilot.operations.pacing_authority_cli `
    --capability <pacing-checkpoint>\capability.json `
    --private-key G:\OptionsCopilotSecrets\pacing-private.dpapi.json `
    --signer-key-id human-key:operator-pacing `
    --approval-output <pacing-checkpoint>\approval.json
```

No daily confirmation is required afterward. To stop all new IBKR market-data
reads immediately, install a signed revocation beside the authority; the
running request guard observes it without a restart:

```powershell
& .\.venv\Scripts\python.exe -m options_copilot.operations.pacing_policy_revocation_cli `
    --authority-dir <pacing-checkpoint> `
    --private-key G:\OptionsCopilotSecrets\pacing-private.dpapi.json `
    --signer-key-id human-key:operator-pacing `
    --reason "operator requested"
```

Both contracts are permanently `review_only=true` and
`direct_order_submission=false`; neither grants instruction or order authority.

## Standalone workspace checkpoint

This section is a local migration-maintenance procedure, not a fresh-clone
quick start. The referenced historical commits, plans, evidence, and private
authority files are deliberately absent from the source-only publication.

Plan 03 exposes capture and Plan 04 uses the default verification path below.
The evidence destination is ignored runtime evidence. Confirm it is ignored and
must not already exist before capture; if it exists, stop and report the
no-overwrite gate. Do not delete, replace, stage, or commit that evidence.

```powershell
Set-Location G:\OptionsCopilot
$baseline = "data/options_copilot/evidence/workspace/phase-01/pre_edit_baseline.json"
git check-ignore -q -- $baseline
if ($LASTEXITCODE -ne 0) { throw "Workspace checkpoint is not ignored." }
if (Test-Path -LiteralPath $baseline) {
    throw "Workspace checkpoint must not already exist; no-overwrite gate."
}
& .\.venv\Scripts\python.exe -m options_copilot.operations.workspace_guard `
    --capture `
    --phase 01 `
    --source-baseline-commit 7237246 `
    --execution-baseline-commit 7237246 `
    --baseline data/options_copilot/evidence/workspace/phase-01/pre_edit_baseline.json `
    --allowlists options_copilot/operations/standalone_phase_allowlists.json
```

After capture, verification is the default operation and uses the same phase,
ignored baseline, and standalone manifest:

```powershell
Set-Location G:\OptionsCopilot
& .\.venv\Scripts\python.exe -m options_copilot.operations.workspace_guard `
    --phase 01 `
    --baseline data/options_copilot/evidence/workspace/phase-01/pre_edit_baseline.json `
    --allowlists options_copilot/operations/standalone_phase_allowlists.json
git diff --name-only 7237246..HEAD
```

The captured HEAD and current/later HEAD are observation metadata only.
Commit `7237246` remains the immutable source and execution diff authority for
capture, verification, and `git diff --name-only 7237246..HEAD`; no captured or
later HEAD may advance that authority.

## Verification

Use the source-snapshot profile in [docs/TESTING.md](docs/TESTING.md) for a
fresh clone. An unrestricted full pytest run also includes local-only tests
that require deliberately excluded financial contracts and migration evidence.
Do not reconstruct or self-sign that authority to make those tests pass.

These environment and source checks do not contact IBKR or create instructions.
`Mode Verify` creates a fresh production verification environment;
its hash-locked pip install may access the configured package index:

```powershell
pwsh.exe -NoProfile -File .\scripts\setup_options_copilot_env.ps1 -Mode Verify
& .\.venv\Scripts\python.exe scripts\verify_locked_environment.py `
    --lock requirements-dev.lock --allow-bootstrap pip --allow-bootstrap setuptools
& .\.venv\Scripts\python.exe -m pip check
& .\.venv\Scripts\python.exe -m compileall -q options_copilot
node --check options_copilot/frontend/app.js
& .\.venv\Scripts\python.exe -m options_copilot.operations.authority_audit --json
git diff --check
```

Only in the original local development installation, with the genuine
authority/evidence corpus available, run the additional full-suite and
migration-baseline checks:

```powershell
& .\.venv\Scripts\python.exe -m pytest tests/options_copilot -q
& .\.venv\Scripts\python.exe -m options_copilot.operations.workspace_guard `
    --phase 01 `
    --baseline data/options_copilot/evidence/workspace/phase-01/pre_edit_baseline.json `
    --allowlists options_copilot/operations/standalone_phase_allowlists.json
```

These environment, source, and safety checks are necessary but do not
replace later acceptance against the live loopback service, current read-only
broker reconciliation, scheduled decision evidence, or visible browser GUI.

## Local state is not distributed

An operator's installation may contain these local-only files:

- `data/options_copilot/api_keys.local.json` (local-only, Git-ignored)
- `data/options_copilot/governance/policy_authority.sqlite3` (private local authority, Git-ignored)
- `data/options_copilot/evidence/` (private local evidence and baselines)

None of these files, or the approval, bridge, ranking, scan, news, and
runtime-observation databases, are uploaded. Local stores may be created when
the runtime is deliberately started; that does not recreate missing human
signatures or financial authority. See [publication boundaries](docs/PUBLISHING.md).
