# Options Copilot 实施研究

**日期：** 2026-08-03
**状态：** 规划输入，非上线声明
**边界：** 只覆盖 `options_copilot/`、`tests/options_copilot/` 与两个专用 PowerShell 脚本；不改变 Rithmic、期货、`paper_server` 或 `trade_copilot`。

## 1. Current State Matrix

“生产级”在本表仅表示本地契约与自动化测试较完整，不代表真实 IBKR、行情订阅或外部 Connector 已完成生产验收。

| 能力 | 当前状态 | 证据与结论 |
|---|---|---|
| 风险配置 | 本地契约较完整 | 5 秒报价、5 分钟审批、$5 重定价、10%/15%/20%、单组合、7 DTE 均有不可放宽校验。 |
| Payoff 与提案验证 | 本地契约较完整 | 会重建提案、独立精算最大亏损、拒绝裸卖/无限风险/未知亏损，并校验结构哈希与逐腿报价。 |
| GUI 与只读 API | 部分完成 | 能显示 Campaign、账户、GLD 持仓、候选、学习状态及审批状态；当前主要消费外部写入的 `runtime_snapshot.json`。 |
| IBKR 只读 gateway | 部分完成 | 已有账户、持仓、scanner、期权链、secdef 与报价 primitive，连接强制 read-only；尚未接入持续运行时，也没有真实 entitlement/pacing 验收。 |
| 新闻与财报 provider | 部分完成 | Finnhub、Alpha Vantage、去重与 DPAPI 边界存在；尚未接入扫描/排名；Jin10 provider 缺失。 |
| Max Pain / Walls / PCR / GEX | 算法完成、未接线 | 纯函数和 supporting-only 契约已测试，但没有运行时消费者。 |
| 候选排名 | 部分完成 | 能按成本后 EV 与风险过滤并返回 `NO_TRADE`；缺少策略模板生成、组合优先级、真正的 Top 3 和运行时接线。 |
| Strategy NAV | 偏差 | Campaign 能计算策略 NAV，但审批路径仍主要使用账户 NLV；现金流调整后的策略 NAV 尚未成为唯一风险基数。 |
| Champion / Challenger | 治理骨架完成 | 30 个独立样本只进入 Discovery、人工 promotion/rollback 已有持久状态机；缺少真实 challenger、影子预测、结果归因和运行时解锁。 |
| 审批与 bridge store | 本地契约较完整 | WAL/FULL、hash-bound、单活跃 handoff、单次外部 attempt、严格状态机、`UNKNOWN_OUTCOME` 禁止重试已测试。 |
| 外部 IBKR instruction creator | 缺失 | 仓库内没有真实 creator；当前依赖 Codex/Managed Connector 外部编排。 |
| 两小时扫描器 | 缺失 | 没有 scheduler、扫描 run ledger、补扫/去重、候选生成编排。现有 heartbeat 只是任务唤醒，不等于应用内扫描器。 |
| GLD 持仓管理 | 严重缺口 | GUI 能显示 GLD，但“存在任一期权仓位即拒绝”会同时阻断平仓/减险 instruction；当前只能观察，不能完成受控退出。 |
| 端到端测试 | 缺失 | 204 个专属测试覆盖本地逻辑，但没有真实 TWS/Gateway、实时链、provider、外部 creator、浏览器与市场时段 E2E。 |

### 当前可信数据流

```text
外部 Managed Connector / 人工快照
  -> ingest.py（拒绝 secret-like 字段）
  -> runtime_snapshot.json
  -> runtime/FastAPI/GUI
  -> proposals.py 重新构造并精算
  -> approvals.sqlite3（5 分钟、hash-bound、一次性）
  -> bridge claim -> authorize+reserve -> complete
  -> 外部 creator 创建 review-only instruction
  -> 用户在 IBKR 内最终确认或拒绝
```

### Day-1 Truth

今天真实可用的是：独立 8891 GUI、外部/人工快照展示、GLD 风险观察、外部候选的本地硬验证、短期审批记录和 fail-closed bridge 状态机。

### Non-Claims

当前不得宣称：自动扫描全市场、自动生成 Top 3、新闻/财报已经影响生产决策、GLD 可自动管理、实时自学习已运行、A 级已解锁、外部 IBKR instruction creator 已闭环，或 204 个模拟测试等同于真实券商验收。

## 2. External Constraints

### 2.1 IBKR 与行情权限

- “所有美股期权”不能设计为持续订阅全部合约。必须使用分层漏斗、请求预算、缓存、取消订阅和有界并发。
- IBKR scanner、secdef、期权链与逐腿报价是不同成本/限制的请求类别；实现时必须通过 capability probe 记录实际权限、延迟、错误码和速率，而不是把某个固定上限写死为永恒事实。
- 发现阶段可以使用延迟或较旧数据，但必须显式标记 `DISCOVERY_ONLY`；审批和 bridge 必须重新获取逐腿可执行 bid/ask，最老报价不得超过 5 秒。
- 每条 leg 必须绑定权威 conid、underlying、expiry、strike、right、multiplier=100、USD、标准合约；调整合约或字段不一致一律拒绝。
- 没有对应 OPRA/美股行情权限时，系统只能降级为研究或 `NO_TRADE`，不得用 last/close/mid 伪装成可执行报价。
- Read-only gateway 只允许幂等读取重试；任何外部 instruction 创建都遵守一次性 reservation，超时后只能 `UNKNOWN_OUTCOME` 并人工对账。

### 2.2 Universe Funnel

推荐漏斗：

1. **Level 0 — 持仓优先：** 当前期权持仓及其 underlying 永远先处理。
2. **Level 1 — 高流动 ETF 核心池：** SPY/QQQ/IWM 等配置化白名单，用于稳定基线与回放。
3. **Level 2 — 事件池：** 未来 14–35 天财报、重大公告、行业事件与异常新闻标的。
4. **Level 3 — 市场 scanner：** 价格、成交量、波动和期权可用性筛选 underlying，不直接拉全部链。
5. **Level 4 — 链粗筛：** 只取 7–35 DTE、有限 strike 窗口和满足 OI/volume/spread 的合约。
6. **Level 5 — 深度定价：** 只对少量 finalist 获取实时逐腿报价、Greeks、secdef，并生成策略组合。

每个扫描 run 必须持久化输入 universe、被过滤原因、请求预算、provider 状态和 `NO_TRADE` 原因，以避免幸存者偏差。

### 2.3 新闻、财报与事件

- 财报日期、8-K/10-Q/10-K、公司公告以 SEC EDGAR 与公司 IR 为事实锚点；聚合新闻只作为补充。
- Jin10、Finnhub、Alpha Vantage 等 provider 必须保存 provenance、published_at、first_seen_at、ingested_at、原始事件 ID 与内容 hash。
- 同一事件跨来源合并，但不能覆盖冲突；日期或盈利数值冲突时标记 `CONFLICTED`，禁止基于该事件创建可审批候选。
- 免费源可能延迟、限流、缺字段或改变许可条款，只能支撑研究和候选发现；执行级报价仍必须来自 IBKR。
- X/API、社交媒体或未经核实帖子不得成为核心依赖；最多作为低权重待核实线索。
- 聊天中暴露过的 Jin10 token 永久视为泄露：撤销后通过 DPAPI 脚本存新 token，日志、快照和错误信息都不得包含秘密。

### 2.4 市场时钟与点时一致性

- 所有记录使用 UTC，并额外保存交易日与美东 market session。
- 扫描调度按美东交易日判断；上海本地周六凌晨仍可能是美股周五盘中/盘后。
- earnings/news/quotes/positions/orders 必须携带各自观察时间，不能用一个顶层时间假装整包同步。
- Replay 只能使用在决策时已可见的数据，禁止回填后数据泄漏。

## 3. Recommended Architecture

### 3.1 运行边界

```text
Read-only Data Plane
  IBKR account/positions/orders/secdef/quotes
  Official events + supplemental news
          |
          v
Point-in-Time Evidence Store
  events / quotes / option chains / scan runs / provider health
          |
          v
Decision Plane
  universe funnel -> feature builder -> strategy templates
  -> scenario engine -> ranking -> risk gate -> Top 3 / NO_TRADE
          |
          v
Human Approval Plane
  GUI challenge -> 5-minute approval -> bridge reservation
  -> external review-instruction creator -> IBKR user confirmation
          |
          v
Learning Plane
  frozen decision -> forward outcomes -> attribution
  -> champion/challenger report -> human promotion/rollback
```

数据、决策、审批、外部 handoff 和学习必须使用不同持久库/表，并通过不可变 ID 关联，避免任何学习代码获得 broker authority。

### 3.2 推荐新增模块

| 模块 | 责任 |
|---|---|
| `gateway/broker_snapshot.py` | 原子化汇总账户、持仓、working orders、未提交 instruction、secdef/quote freshness；生成 broker gate proof。 |
| `providers/jin10.py` | DPAPI secret、超时/限流、事件 schema、去重与 provenance；不得把 token 写入请求日志。 |
| `providers/official.py` | SEC/公司 IR 财报与公告事实锚点；缓存与变更检测。 |
| `storage/evidence.py` | point-in-time 事件、quote、链与 provider health ledger。 |
| `scanner/universe.py` | 分层 universe、请求预算、持仓优先、事件池、scanner 池。 |
| `scanner/scheduler.py` | 美东 session、两小时 cadence、run id、租约、防重入、重启补扫和闭市 no-op。 |
| `strategies/templates.py` | 明确策略模板和适用条件；只生成有限风险结构。 |
| `strategies/generator.py` | 将 finalist 链组合成候选，独立计算 debit/credit、max loss/profit、breakeven 与 liquidity。 |
| `analytics/volatility.py` | IV rank/percentile、term structure、skew、expected move 与财报 IV crush 特征。 |
| `analytics/scenarios.py` | 事件/价格/波动情景与成本后 EV；保存假设与反证条件。 |
| `ranking/portfolio.py` | 组合优先、Top 3、多样性约束、NO_TRADE reason codes。 |
| `positions/manager.py` | 只允许识别现有组合、完全平仓或可证明减险；禁止增加净短腿、数量、max loss 或资本占用。 |
| `learning/outcomes.py` | 固定 horizon 的 forward outcome、MFE/MAE、成本后 PnL、校准与场景标签。 |
| `learning/similarity.py` | 只读相似场景检索；不得直接改变生产参数。 |
| `bridge/creator_adapter.py` | 唯一外部 creator adapter；只能创建 review-only instruction，没有 order-submit method。 |
| `operations/readiness.py` | entitlement、provider、时钟、端口、Tailscale、working order、instruction 与数据库一致性报告。 |

### 3.3 策略组合优先级

所有策略先通过有限风险、7 DTE、流动性、成本后 EV 与单组合门槛，再按场景选择：

1. **现有仓位完全平仓/减险**：最高优先级，不与新开仓竞争。
2. **Debit vertical**：有方向催化、IV 不极端、成本受限时的默认结构。
3. **Credit vertical**：仅在 IV 风险溢价充分、宽 spread/尾部风险/指派风险均计价后；仍须精确 max loss。
4. **Calendar/diagonal**：仅在期限结构与事件窗口明确、两期限均有可执行流动性时。
5. **Defined-risk butterfly/condor**：只有价格区间论点和 pin/墙信号与独立证据一致时。
6. **Long call/put**：有限风险但时间损耗高；只有组合无法以合理流动性构造且 EV 更优时作为例外。

永久排除裸卖、ratio 中未完全覆盖的额外短腿、无法权威识别的调整合约，以及任何 max loss 不能精确计算的结构。

### 3.4 入场与退出合同

每个候选必须冻结：论点、催化时间、预期持有期、最大亏损、目标区间、止损/失效条件、time stop、profit-take、事件后退出规则、允许的重定价范围和最大持仓日。

退出不能只用固定百分比；至少同时支持：

- Thesis invalidation（新闻/财报/价格结构被反证）；
- Risk stop（组合可执行平仓成本达到预定亏损）；
- Profit take（目标收益或剩余回报/风险不再划算）；
- Time stop（事件已过、theta 加速或进入禁止 DTE 区）；
- Liquidity/quote integrity stop（只允许显示人工处理建议，不能用坏报价自动构造 instruction）。

## 4. Validation Architecture

| 层级 | 必须验证的内容 | 代表性门槛 |
|---|---|---|
| 单元 | payoff、模板、EV、NAV、DTE、position delta、事件去重、session clock | Decimal 边界；裸卖/无限亏损/未知字段一律拒绝。 |
| 属性/模糊 | 任意 leg 组合与字段别名 | 不能生成未覆盖短腿；max loss 与 payoff envelope 一致。 |
| SQLite/并发 | scan lease、审批、bridge reservation、outcome ledger | 多进程只产生一个 active run/attempt；崩溃可恢复且不重试外部调用。 |
| Provider contract | 限流、超时、坏 JSON、冲突事件、秘密脱敏 | 所有失败转为 provider health + degraded/NO_TRADE；无 token 泄漏。 |
| IBKR paper/live-read | entitlement、scanner、链、secdef、报价、订单/持仓 reconciliation | 真实账户只读；无 order primitive；5 秒内逐腿 bid/ask；pacing 可观测。 |
| Replay | 点时一致、成本/滑点、财报事件、选择偏差 | 所有特征 first_seen 不晚于 decision_at；无未来数据。 |
| Walk-forward | calibration、EV、drawdown、分场景稳定性 | 训练/验证时间隔离；报告净佣金/滑点结果和样本量。 |
| Shadow | Champion 与 Challenger 同时预测但不改变生产 | 至少 30 个独立场景只进入 Discovery；人工签署 promotion。 |
| Bridge E2E | GUI 双确认、过期、重定价、一次 creator、UNKNOWN | Creator 最多一次；结果不确定后禁止自动重试；永不提交订单。 |
| 运维 | 8766/8788/8878 不变、8891 单实例、Tailscale 双入口 | 启停前后 PID/端口/远程 HTTPS 均有证据。 |

生产候选上线前至少需要三类真实验收：市场开盘的 IBKR 只读数据、真实 provider 网络契约、真实 review-instruction creator 沙箱/受控账户链路。单元测试数量不能替代这三类证据。

## 5. Threat Model

| 威胁 | 严重度 | 强制缓解 |
|---|---|---|
| 候选或 LLM 伪造 max loss | Critical | 永远由本地 payoff engine 重算；声明与重算不一致拒绝。 |
| 借 position management 增仓 | Critical | 比较执行前后权威持仓；只允许数量/净短腿/max loss/资本占用不增加，完全平仓优先。 |
| 重复创建 IBKR instruction | Critical | 调用前 durable reservation；同 approval 唯一 attempt；超时即 UNKNOWN，不重试。 |
| 坏/旧报价触发审批 | High | 审批和 bridge 两次独立 freshness；逐腿 bid/ask 与 secdef proof；超过 5 秒拒绝。 |
| 数据源冲突或新闻注入 | High | provenance、hash、官方事实锚点、冲突状态；网页文本不能改变系统指令或权限。 |
| API token 泄漏 | High | DPAPI、secret-like 字段拒绝、固定错误码、日志脱敏；旧 Jin10 token 撤销。 |
| 学习系统自动放宽风险 | Critical | 学习库与 authority 分离；promotion/A-grade 仅人工签署；风险常量不可由模型/环境变量放宽。 |
| Replay 未来数据泄漏 | High | published/first_seen/ingested/decision 四时间；点时查询；数据版本 hash。 |
| IBKR pacing 导致不完整链 | Medium | 请求预算、缓存、退避、partial 状态；不完整数据只能 NO_TRADE。 |
| Tailscale/GUI 误替换现有服务 | High | loopback 8891、精确 PID/命令行检查、保留 443->8878、远程双入口验收。 |
| 10K 目标诱发风险漂移 | High | Campaign 仅展示；风险独立硬编码；任何目标进度不能进入 risk-cap 计算。 |

## 6. Phase Dependencies

```text
P0 现状冻结与审计基线
  -> P1 策略 NAV + 原子 broker snapshot
  -> P2 GLD 只减险持仓管理
  -> P3 证据库 + provider 生产化
  -> P4 universe funnel + scheduler
  -> P5 策略模板 + 波动/情景 + Top 3
  -> P6 GUI 解释与持仓/候选工作流
  -> P7 外部 creator 受控 E2E
  -> P8 replay/walk-forward/outcomes
  -> P9 champion/challenger shadow 治理
  -> P10 生产 readiness、逐级放量与长期运营
```

关键依赖：

- P2 必须在任何新开仓扫描前完成，因为 GLD 当前占用唯一组合名额。
- P4 依赖 P1/P3，不能在 broker snapshot 或 provider evidence 不可信时生成候选。
- P5 依赖 P4 提供有界 finalist，避免对全链暴力组合。
- P7 依赖 P1/P2/P5 的权威 proof；不能先接 creator 再补安全门槛。
- P8/P9 只能消费冻结决策，不能反向获得 broker authority。
- P10 的 live-review 候选放量需要真实市场时段验收；无固定日历承诺替代质量门槛。

## 7. Known Unknowns

1. 当前 IBKR 账户的 OPRA/美股期权实时行情 entitlement、Greeks/OI 完整性及 snapshot/streaming 权限。
2. 采用 TWS、IB Gateway、Client Portal/Managed Connector 中哪一条作为长期只读数据面；外部 creator 的稳定 API 契约与 deep link host。
3. IBKR 实际 scanner/chain/secdef/quote pacing 行为、并发预算和断线恢复表现。
4. Jin10 MCP 的许可、延迟、字段稳定性与美股覆盖；必须用撤销后的新 token 做 provider contract test。
5. 免费源是否足以覆盖财报日历和新闻；不足部分是否接受低成本订阅。
6. GLD 当前组合的真实可执行平仓报价、合约是否为标准 multiplier=100，以及 IBKR 是否支持所需 combo review instruction。
7. 佣金、exchange fee、smart routing、滑点与指派/提前行权模型需要从真实账户配置校准。
8. 何谓“独立场景”的最终聚类规则；同一财报、同一 ticker、相邻扫描不能重复计数。
9. Champion 的初始规则版本、Challenger 的模型族和 promotion 统计阈值需在 AI-SPEC/eval 计划中锁定。
10. 账户现金流、存取款和非策略仓位如何从 10K Campaign strategy NAV 中剥离。

---

## RESEARCH COMPLETE

本研究足以进入实施规划；未知项应作为早期 capability probe 和人工决策门槛，而不是由实现者自行猜测。
