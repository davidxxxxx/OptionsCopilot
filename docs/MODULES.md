# 模块地图

本次按职责整理说明，不移动或重构现有源码。状态定义见 [功能清单](FEATURES.md)。

## 模块与源码入口

| 模块 | 职责与主要入口 | 代表性测试 | 当前边界 |
| --- | --- | --- | --- |
| 运行时与装配 | [runtime.py](../options_copilot/runtime.py)、[production_runtime.py](../options_copilot/production_runtime.py)、[config.py](../options_copilot/config.py)：组装服务、DIRECT / EXTERNAL 路径、线程和生命周期 | `test_runtime.py`、`test_production_runtime.py` | 启动成功不是来源与生产资格通过 |
| 券商与原子快照 | [gateway/](../options_copilot/gateway/)：`IBKRReadOnlyGateway`、账户控制状态、合约、逐腿报价、`broker_snapshot.py` | `test_ibkr_readonly_gateway.py`、`test_broker_snapshot.py`、`test_upstream_control_lifecycle.py` | 永久只读；上游失联与本地连接分别判定 |
| 历史与特征输入 | [history_source_runtime.py](../options_copilot/history_source_runtime.py)、[feature_source_resolution.py](../options_copilot/feature_source_resolution.py)、`gateway/native_history.py`、`storage/history_sources.py` | `test_scheduled_history_runtime.py`、`test_feature_source_resolution.py` | 有界历史片段和来源绑定不是完整模型输入 |
| 新闻与事件来源 | [providers/](../options_copilot/providers/)、[news/](../options_copilot/news/)、[news_runtime.py](../options_copilot/news_runtime.py) | `test_event_providers.py`、`test_news_runtime.py`、`test_provider_runtime_truth.py` | 配置、加载、来源成功和证据权威不能混为一谈 |
| 基本面与身份 | [fundamentals/](../options_copilot/fundamentals/)、[providers/sec_identity.py](../options_copilot/providers/sec_identity.py) | `test_fundamentals.py`、`test_sec_filer_identity_pit.py` | 来源和历史时点需要验证 |
| 股票研究池 | [equity_pool/](../options_copilot/equity_pool/)、[research_allocation.py](../options_copilot/research_allocation.py) | `test_equity_pool_core.py`、`test_equity_pool_integration.py` | 有界研究池，缺失因素明确降级 |
| 期权结构与策略 | [option_pool/](../options_copilot/option_pool/)、[strategies/](../options_copilot/strategies/)、[domain/](../options_copilot/domain/) | `test_option_structure_pool.py`、`test_strategy_generator.py` | 明确合约、逐腿证据和有限风险是前提 |
| 分析与模型预览 | [analytics/](../options_copilot/analytics/)：情景、波动率、EMA20、IV 百分位、benchmark、signed_features | `test_signed_features.py`、`test_ema20.py`、`test_iv_percentile.py`、`test_benchmark.py` | 预览可计算，生产特征入口仍关闭 |
| 决策与排名 | [decision/](../options_copilot/decision/)、[ranking/](../options_copilot/ranking/)、[execution_cost.py](../options_copilot/execution_cost.py) | `test_decision_pipeline.py`、`test_production_candidate_e2e.py`、`test_joint_ranking.py` | 费用后门槛及证据完整性决定是否有候选 |
| 风险、NAV 与持仓 | [risk/](../options_copilot/risk/)、[performance/](../options_copilot/performance/)、[positions/](../options_copilot/positions/) | `test_domain_risk_ranking.py` | 个人授权和真实持仓样例不分发；当前报价时间校验仍有已知问题 |
| 调度与盘后准备 | [scanner/](../options_copilot/scanner/)、[after_hours_indicative.py](../options_copilot/after_hours_indicative.py)、[market/](../options_copilot/market/) | `test_scan_scheduler.py`、`test_daily_operation_authority.py`、`test_after_hours_indicative.py` | 日历、时间槽和租约不可绕过；盘后数据仅供观察 |
| AI 与学习回放 | [llm/](../options_copilot/llm/)、[learning/](../options_copilot/learning/)、[learning_shadow.py](../options_copilot/learning_shadow.py)、[replay/](../options_copilot/replay/) | `test_news_deepseek.py`、`test_shadow_evaluation_runtime.py`、`test_replay.py` | 影子学习与解释不改变生产规则 |
| 治理、审批与桥接 | [governance/](../options_copilot/governance/)、[approval/](../options_copilot/approval/)、[bridge/](../options_copilot/bridge/) | `test_governance_contracts.py`、`test_approval_store.py`、`test_bridge_coordinator.py` | 真实人类授权和 creator transport 是独立边界 |
| 存储、安全与运维 | [storage/](../options_copilot/storage/)、[state/](../options_copilot/state/)、[security/](../options_copilot/security/)、[operations/](../options_copilot/operations/)、[scripts/](../scripts/) | `test_evidence_store.py`、`test_config_security.py`、`test_dependency_lock.py` | SQLite 哈希链、密钥保护、端口与环境检查；不分发运行数据 |
| GUI 与 API | [api/](../options_copilot/api/)、[frontend/](../options_copilot/frontend/) | `test_api_frontend.py`、`test_frontend_contract.py`、`test_frontend_health_refresh.py` | 浏览器只投影状态；缺失数据不补造、按钮不代表权限 |

## 数据流与权限分离

```text
IBKR 只读数据 ─┬─ 账户 / 合约 / 逐腿报价 ─┐
              └─ 原生历史 / 来源片段 ───┤
新闻 / 日历 / 基本面 ─ 支持性证据 ───────┤
                                       ↓
                 研究池 → 组合与特征 → 风险 / 成本门槛 → 排名 / 决策
                                       ↓                  ↓
                                 持仓观察 / 管理       本地 GUI
                                       ↓                  ↓
                                  学习与回放        人类审批边界
                                                          ↓
                                            外部复核指令（当前未就绪）
                                                          ↓
                                            用户在 IBKR 独立确认订单
```

`runtime.py` 是装配根；下层模块接收注入依赖。`decision/pipeline.py` 不拥有审批或
券商写入权限，`bridge/` 也不实现真实订单提交。所有路径中的失败可结束为 `NO_TRADE`。

## 常用只读 API

| 页面 / 用途 | API |
| --- | --- |
| 总体健康与控制快照 | `/api/health/summary`、`/api/bootstrap` |
| 持仓与管理状态 | `/api/positions`、`/api/management/current` |
| 新闻、日历、简报 | `/api/news`、`/api/calendar`、`/api/weekly-brief` |
| 来源与基本面 | `/api/configuration/providers`、`/api/source-evidence`、`/api/fundamentals` |
| 研究池与候选 | `/api/equity-pool/latest`、`/api/option-pool/latest`、`/api/research-top10`、`/api/candidates` |
| 调度与排名 | `/api/scans/latest`、`/api/scans/campaign`、`/api/rankings/latest` |
| 特征来源诊断 | `/api/diagnostics/feature-source-cache` |
| 学习与解释 | `/api/learning`、`/api/learning/records`、`/api/advisory` |

以上是数据读取入口，不是操作许可。正式接口定义以 [api/app.py](../options_copilot/api/app.py)
和本地运行时的 OpenAPI 为准；读取健康接口不会补签模型、补造数据或证明来源已恢复。
