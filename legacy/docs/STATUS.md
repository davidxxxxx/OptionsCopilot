# 当前状态与已知阻塞

状态复核：2026-10-02；可发布应用源码与 2026-09-12 快照一致，本次只更新发布文档。
本文不是此刻的账户、市场行情或收益报告；发布时不运行
真实券商采集、重启、签名、下单或模型晋升操作。

## 已实现但不等于实盘完成

仓库已有本地 GUI/API、只读券商适配、原子快照、新闻/事件与基本面来源、研究池、有限风险
组合、调度、排名、影子学习、回放和哈希化存储。功能对应关系见 [模块地图](MODULES.md)。
已有测试验证许多纯计算、安全门槛及模拟边界；不代表真实市场的整流程已经验收。

## 明确保留的阻塞

| 范围 | 源码中的实际限制 | 不能据此声称 |
| --- | --- | --- |
| 报价管理时间基准 | [broker_snapshot.py](../options_copilot/gateway/broker_snapshot.py) 按 `exchange_time` 计算 age/skew；[positions/manager.py](../options_copilot/positions/manager.py) 按 `observed_at` 复算，可能拒绝为 `QUOTE_TIME_MISMATCH` | 已取得某个价格就能通过持仓管理与可执行证据校验 |
| 上游恢复 | [production_runtime.py](../options_copilot/production_runtime.py) 的 supervisor 重连依据本地 `connected`；上游 LOST 与仍连着的本地 socket 需要独立恢复处理 | 单次重启成功或端口存在就证明持续可用 |
| 历史覆盖与持续采集 | [history_source_runtime.py](../options_copilot/history_source_runtime.py) 每轮最多三个标的，依赖收盘后 40 分钟的 `NEXT_SESSION_PREPARATION` 租约和有效日历 | 已有若干历史片段就覆盖所有候选、持仓与基准 |
| 历史口径 | [history_source_contracts.py](../options_copilot/history_source_contracts.py) 保留未验证的历史交易日覆盖、复权和方法学状态 | 足够的行数等于 60 / 252 个已完成且可比较的交易日 |
| 生产特征 | [analytics/signed_features.py](../options_copilot/analytics/signed_features.py) 的 `build_signed_market_features()` 仍直接拒绝；`build_market_feature_preview()` 仅供预览 | 确认 EMA20、IV 百分位或 benchmark 参数后生产模型即已启用 |
| 波动率来源 | [production_runtime.py](../options_copilot/production_runtime.py) 隔离旧的期权腿 IV 聚合；ATM 曲面及当前/历史 IV 的可比性尚未闭合 | 用若干腿的平均 IV 替代真实 ATM 曲面或原生 IV 比较口径 |
| 特征消费与排名 | [feature_source_resolution.py](../options_copilot/feature_source_resolution.py) 的绑定结果仍为观察性、`model_input_complete=false`、`production_eligible=false` | 来源哈希存在就等于可用于生产评分、费用后 EV 或审批 |
| 复核指令与订单 | [bridge/creator_transport.py](../options_copilot/bridge/creator_transport.py) 与治理门槛要求真实外部复核能力和授权；真实订单写入永久禁止 | 有桥接代码、actor 标签或自哈希就等于用户签名或提交订单能力 |

这些问题在本次文档/发布工作中**没有被修复**。源码包不会为了显示“已完成”而放宽门槛。

## 源码发布额外排除的本地依赖

- 个人策略 NAV、账户佣金校准及其绑定的初始策略合约、相关账本、真实签名材料、节流批准、凭据和配置不分发。
- 本地迁移计划、历史验收报告、Git 开发历史不上传；远端只延续已筛选的源码发布历史。
- 缺失本地权限或证据时，相关生产路径应保持不可用；不能从样例重新构造或冒充真实授权。
- 某些遗留迁移/工作区测试依赖上述本地资料，必须与可分发的源码测试分开说明。

## 完整流程的验收条件

至少需要分别证明：新鲜且已对账的账户/持仓；合约明确且满足五秒门槛的每腿 BBO 与交易所
时间；目标和基准的完整历史窗口及相同口径；有效的生产特征和真实授权；费用后与风险门槛；
实际调度执行；持久化一致性；可见 GUI 的自然刷新与安全禁用状态。

只有这些环节各自具有有效证据，才能讨论“完整研究/建议流程跑通”。没有合格候选时，
`NO_TRADE` 是允许且必要的结果；源码发布和单元测试都不证明某个策略有正期望收益。
