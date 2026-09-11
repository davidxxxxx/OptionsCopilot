# Options Copilot 实现模式映射

本文件保留早期迁移时的模式参考，不是当前功能完成清单或新环境操作步骤。
下文的 `trade_copilot/`、历史研究文档及基线属于原工作区，不随源码快照分发。
当前模块入口和真实限制以 [模块地图](../docs/MODULES.md) 与 [状态说明](../docs/STATUS.md) 为准。

**来源：** `IMPLEMENTATION_RESEARCH.md` 与当前代码库
**用途：** 执行代理在新增文件前必须优先复用这些模式；本文件不授权修改被引用的 `trade_copilot/` 文件。

## 模块到现有模式的映射

| 拟实现能力 | 推荐文件 | 首选现有模式 | 应复用的具体契约 |
|---|---|---|---|
| 原子 broker snapshot | `gateway/broker_snapshot.py` | `gateway/ibkr_readonly.py` 的 `AccountSnapshot`、`PositionSnapshot`、`OptionQuoteSnapshot`；`bridge/coordinator.py` 的 `BrokerGateResult` | 使用 frozen dataclass、UTC aware time、Decimal；快照必须同时包含 account、positions、working orders、unsubmitted instructions、secdefs、quotes 与 completeness。 |
| Broker gate 持久 proof | 修改 `bridge/coordinator.py`、`bridge/store.py` | `_broker_gate_payload()`、`_verify_broker_gate()`、调用前 reservation | proof 必须 hash-bind proposal/snapshot/secdefs/quotes；在取得 SQLite 写锁后重读时钟；外部调用前落盘唯一 attempt。 |
| Evidence ledger | `storage/evidence.py` | `storage/ledger.py` 的 `PointInTime`、`DecisionLedger`、hash chain；`approval/store.py` 的 WAL/FULL 初始化 | 四时间 published/first_seen/ingested/observed；append-only、canonical hash、identity conflict、corruption detection；不在记录中保存 secret。 |
| 扫描 run/租约 | `scanner/scheduler.py` | `bridge/store.py` 单活跃 partial index/transaction；`trade_copilot/advisor/scheduler.py` 的 `AdviceSchedulePolicy`、`AdviceScheduleState`、`ScheduleDecision` | SQLite `BEGIN IMMEDIATE` + active lease；run_id 唯一；美东 session；同一 cadence 不重复；重启只补未完成且无外部副作用的 read-only run。 |
| Universe funnel | `scanner/universe.py` | `gateway/ibkr_readonly.py::scan_underlyings()` 与输入验证 | 分层不可跳级；持仓优先；每层保存 included/excluded reason、request budget、provider health；有界并发和缓存。 |
| Provider adapters | `providers/jin10.py`、`providers/official.py` | `providers/events.py` 的 `JsonTransport`、`ProviderUnavailable`、`NewsEvent`、`EarningsEvent`、`NewsAggregator` | 传输层可注入；固定超时；解析异常 fail closed；event_id/content hash 去重；provenance 与三时间；冲突不覆盖。 |
| DPAPI secrets | 修改 `security/cli.py`、`scripts/set_options_copilot_secret.ps1` | `security/dpapi.py::DPAPISecretStore` | 仅按允许的 secret name 读写；stdout/stderr 永不回显；旧 Jin10 token 不迁移。 |
| Strategy registry | `strategies/templates.py` | `domain/models.py` 的 `OptionLeg`/`OptionStrategy`；`risk/payoff.py` 的精算入口 | 模板输出完整 legs/ratios，不接受自由文本策略；每个模板声明适用场景、退出约束、是否涉及 assignment。 |
| Candidate generator | `strategies/generator.py` | `proposals.py` 的不信任输入重构与 `_canonical_proposal`；`risk/payoff.py` | 生成后仍走同一独立 validator；使用可执行 ask 买/bid 卖；所有费用/滑点进入 cost；max loss 由 payoff engine 计算。 |
| 波动/情景 | `analytics/volatility.py`、`analytics/scenarios.py` | `analytics/positioning.py` 纯函数 + frozen result；`ranking/engine.py` Decimal 排序 | 输入/输出不可变；Max Pain/walls/PCR/GEX 字段标记 supporting-only；保存情景概率、假设、反证和 calibration version。 |
| Top 3 与组合优先 | `ranking/portfolio.py` 或扩展 `ranking/engine.py` | `ranking/engine.py::RankingEngine` 的稳定 tie-break 和 `NO_TRADE` | 先硬过滤、后策略优先级、再 EV/流动性；Top 3 不得共享同一 candidate_id；无合格项必须结构化 NO_TRADE reason codes。 |
| Strategy NAV | 修改 `runtime.py`、`performance/campaign.py` | `TenKCampaign` 的现金流隔离；`risk/policy.py::RiskEngine` | 风险只使用 `strategy_nav_usd`；10K 进度不能改变 cap；数据缺失拒绝，不回退到账户 NLV。 |
| Position manager | `positions/manager.py` | `trade_copilot/state/position_state.py::PositionState` 只读归一化；`proposals.py` payoff 重算；`bridge/coordinator.py` authority gate | 比较 before/after 权威持仓；只允许完全平仓或数量、净短腿、max loss、margin 均不增加；不能通过 strategy label 自报“减险”。 |
| Outcome ledger | `learning/outcomes.py` | `storage/ledger.py` append-only；`learning/governance.py` promotion/rollback | decision_id 不变；horizon/MFE/MAE/成本后 PnL/quote quality 分开；晚到数据产生新版本，不覆写历史。 |
| Similarity | `learning/similarity.py` | `storage/canonical.py` canonical JSON/hash | 只读检索；返回 evidence IDs、距离和版本；不得修改风险或生产配置。 |
| External creator | `bridge/creator_adapter.py` | `bridge/coordinator.py::ReviewInstructionCreator` Protocol；测试中的 `RecordingCreator` | 方法只能 `create_review_instruction`；返回固定 schema；没有 submit/transmit/order method；一次 reservation 后任何不确定结果终态 UNKNOWN。 |
| GUI read model | 修改 `api/app.py`、`frontend/app.js` | 当前 `CopilotServices`/read-model payload 与轮询协议 | API 不暴露 secret/token；候选展示 evidence/quote age/max loss/exit plan；持仓管理和新开仓使用不同 badge/action；review link 仅 READY 状态可见。 |
| 运维 readiness | `operations/readiness.py`、专用脚本测试 | `scripts/start_options_copilot.ps1` 的精确 loopback listener 检查 | 当前项目只绑定 `127.0.0.1:8891`；不由本项目控制其他交易系统、远程转发或 Gateway 生命周期。 |

## SQLite 统一约定

- 使用独立数据库文件：evidence、scan runs、decisions/outcomes、approvals、bridge 不共表。
- 启动时验证 `journal_mode=WAL`、`synchronous=FULL`、`foreign_keys=ON`，不能只执行 PRAGMA 后忽略返回。
- 写路径统一显式 transaction；身份字段用受限 regex；金额用 Decimal 字符串；时间用 UTC ISO 8601。
- 所有跨进程唯一性依赖数据库 unique index/transaction，不依赖进程内锁。
- 迁移必须按 `user_version` 定向测试，不能对未知旧版本直接 `CREATE IF NOT EXISTS` 后升级版本号。

## 测试模式

- 纯业务逻辑：沿用 `test_domain_risk_ranking.py` 的 Decimal 边界与构造器测试。
- Broker/bridge：沿用 `test_bridge_coordinator.py` 的 `MutableClock`、`broker_snapshot()`、`RecordingCreator` 和并发测试。
- Runtime/API：沿用 `test_runtime.py` 的临时数据目录、原子 snapshot reload 与 fail-closed assertions。
- Provider：沿用 `test_event_providers.py` 的可注入 fake transport；增加秘密泄漏、冲突、429、timeout、坏 JSON。
- 进程验收：新增专门 integration marker；捕获启动前后 PID/端口/命令行，最后无论成功失败都只清理本测试创建的 8891 进程。

## 禁止的实现捷径

- 不从 `trade_copilot` 复制下单/command authority；只能读其状态与调度模式。
- 不让 LLM 输出直接进入 bridge；必须经过本地模板、payoff、risk、broker proof 和 GUI 审批。
- 不用 last/close/mid 替代执行级 bid/ask。
- 不用“已有持仓所以这是平仓”的文本标签绕过 position diff proof。
- 不让 scheduler 自动调用 creator；scheduler 只生成冻结候选，creator 仍要求未过期人工审批。

## PATTERN MAPPING COMPLETE
