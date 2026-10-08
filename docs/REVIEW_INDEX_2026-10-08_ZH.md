# OptionsCopilot 项目审阅索引（2026-10-08）

## 先看结论

本次交付是**独立审阅分支上的文档整理**：给已有源码建立阅读顺序，核对已上传内容，并明确尚未并入的增量。应用代码、测试代码和依赖文件没有随本次整理更新。

- 已核对的远端基线：[`0cac427ad993d4b89ad43a721a6aa249df3aa440`](https://github.com/davidxxxxx/OptionsCopilot/commit/0cac427ad993d4b89ad43a721a6aa249df3aa440)，提交时间 2026-10-02 12:41:11 UTC。
- 基线完整目录树包含 **419 个文件，其中 392 个 Python 文件**。这是文件清点，不是执行或测试结果。
- 现有源码可以审阅本地只读应用、Jin10 采集基础、新闻证据存储、研究池和风险门槛。
- 后续的独立云端 Jin10 研究增量、Windows 市场观察采集与 HTTP 研究接入增量，**尚未出现在该远端基线中**。本次文档没有补入这些源码。
- 本次仅通过 GitHub 读取源码、目录和检查状态；没有执行安装、单元测试、实时数据采集、GUI 验收或券商连接。

审阅时请把“远端存在源码”“历史曾验证”“本次已重测”“生产可用”分开判断。

## 1. 建议阅读顺序

1. [项目 README](../README.md)：项目定位、永久只读契约、支持环境与入口。
2. [当前状态与已知阻塞](STATUS.md)：先了解哪些环节仍然关闭或需要真实授权。
3. [功能清单](FEATURES.md)：按场景查看已有实现、条件可用和研究预览范围。
4. [模块地图](MODULES.md)：沿源码入口、代表性测试和数据流深入。
5. [源码快照测试说明](TESTING.md)：选择可移植测试，识别不随公开仓库分发的前置条件。
6. [发布与隐私边界](PUBLISHING.md)：确认公开仓库应包含和排除的内容。
7. [应用操作说明](../options_copilot/README.md)：需要本地检查时再阅读配置和操作细节。

## 2. 远端已有架构与源码入口

以下路径均已对照上述基线目录树核验；功能可用性仍以对应运行证据为准。

| 层次 | 入口 | 审阅重点 |
| --- | --- | --- |
| 装配与配置 | [runtime.py](../options_copilot/runtime.py)、[production_runtime.py](../options_copilot/production_runtime.py)、[config.py](../options_copilot/config.py) | 依赖装配、生命周期、DIRECT / EXTERNAL 数据路径、失败关闭 |
| 本地 API / 页面 | [api/app.py](../options_copilot/api/app.py)、[frontend/](../options_copilot/frontend/) | FastAPI 与原生 HTML / CSS / JavaScript；状态展示与权限边界 |
| 券商只读数据 | [gateway/ibkr_readonly.py](../options_copilot/gateway/ibkr_readonly.py)、[gateway/broker_snapshot.py](../options_copilot/gateway/broker_snapshot.py) | 永久只读、合约与逐腿报价绑定、时间和快照一致性 |
| 外部批次输入 | [gateway/external_readonly_feed.py](../options_copilot/gateway/external_readonly_feed.py)、[external_bundle_commit.py](../options_copilot/external_bundle_commit.py) | 已有文件交换协议、哈希校验、原子替换和陈旧数据拒绝；不要将它认作后续 HTTP 市场观察适配器 |
| Jin10 基础 | [providers/jin10_mcp.py](../options_copilot/providers/jin10_mcp.py)、[providers/jin10.py](../options_copilot/providers/jin10.py) | 有界 MCP 只读会话、来源结果转换、实体映射和降级状态 |
| 新闻协调与证据 | [news_runtime.py](../options_copilot/news_runtime.py)、[storage/evidence.py](../options_copilot/storage/evidence.py)、[news/](../options_copilot/news/) | `NewsCoordinator`、研究投影、时间点证据、追加式存储和完整性校验 |
| 研究池与组合 | [equity_pool/](../options_copilot/equity_pool/)、[option_pool/](../options_copilot/option_pool/)、[strategies/](../options_copilot/strategies/) | 股票研究池、期权组合构建及缺失证据的降级 |
| 历史与分析 | [history_source_runtime.py](../options_copilot/history_source_runtime.py)、[feature_source_resolution.py](../options_copilot/feature_source_resolution.py)、[analytics/](../options_copilot/analytics/) | 历史覆盖、特征绑定与预览；生产特征入口仍有显式阻塞 |
| 决策与约束 | [decision/](../options_copilot/decision/)、[ranking/](../options_copilot/ranking/)、[risk/](../options_copilot/risk/)、[governance/](../options_copilot/governance/) | 证据、成本和风险门槛；真实授权与研究结果分离 |
| 调度、学习与维护 | [scanner/](../options_copilot/scanner/)、[learning/](../options_copilot/learning/)、[operations/](../options_copilot/operations/)、[scripts/](../scripts/) | 有界调度、影子评估、环境和权限检查 |

### 两条数据路径的阅读方式

**新闻研究路径：** `runtime.py` 的 provider 装配 → `Jin10McpHttpClient` / `Jin10EventProvider` → `NewsCoordinator` → `EvidenceStore` 与研究投影 → API / 页面。

**行情与候选路径：** 只读网关或已有外部文件批次 → 原子快照与历史/来源校验 → 研究池、组合与特征 → 风险/成本/授权门槛 → 排名与展示。

前一条提供支持性研究证据；后一条仍有 [STATUS.md](STATUS.md) 记载的生产阻塞。新闻采集成功不会自动解除行情、模型或授权缺口。

## 3. 已有文件与尚未上传的增量

### 3.1 Jin10：基础已在，独立云端增量未并入

**基线已有：**

- `options_copilot/providers/jin10_mcp.py`
- `options_copilot/providers/jin10.py`
- `options_copilot/news_runtime.py`
- `options_copilot/storage/evidence.py`
- [test_jin10_mcp.py](../tests/options_copilot/test_jin10_mcp.py)、[test_jin10_entity_linking.py](../tests/options_copilot/test_jin10_entity_linking.py)、[test_evidence_store.py](../tests/options_copilot/test_evidence_store.py)

**基线未包含：**

- 后续独立入口 `options_copilot/operations/cloud_jin10_research.py`
- 后续验收文档 `docs/CLOUD_JIN10_RESEARCH_ACCEPTANCE_ZH.md`
- 云端增量包对 `jin10_mcp.py` 和 `jin10.py` 的后续修改
- 该增量包的基线清单和离线验证记录

远端 provider 文件存在，只能证明旧版本基础存在。它们不代表后续云端覆盖包已上传。

**历史验证说明，非本次复测：** 后续独立云端研究曾在 2026-10-06 / 2026-10-08 记录过锁定基线检查，以及真实首页 20 条数据经 provider / coordinator、10 条展示投影、20 条证据入库、关闭重开后哈希校验的验证。该记录只覆盖对应时点和研究链路，不构成 24 小时持续服务、历史回填完整性、当前新闻时效或完整期权功能验收。本次未取得并逐字节复核该覆盖包，未重放上述测试；不能把历史结果归到本审阅分支。

### 3.2 Windows 市场观察与 HTTP 接入：未并入

基线没有以下后续增量路径：

- `options_copilot/gateway/market_observation.py`
- `options_copilot/market_observation_cli.py`
- `options_copilot/market_observation_research.py`
- `tests/options_copilot/test_market_observation.py`
- `docs/IBKR_MARKET_OBSERVATION.md`

同时，已读取的基线 `api/app.py` 不含后续 `POST /api/research/market-observation` 入口，`runtime.py` 不含 `market_observation` 接线。现有 `external_readonly_feed.py` 是另一套文件型批次输入，不能替代这些缺失文件。

这些功能有历史验收背景，但本次没有 Windows 工作区文件、HTTP 接线增量和可重跑环境，不能确认当前版本、实际部署状态或与云端研究的整合。前端接线也未在本次完成运行验收。

### 3.3 其他历史快照

历史 v4 材料本次未能取得可读取的完整文件，无法完成文件级差异和复测。它与上述 `0cac427` 基线属于不同版本线；不得把云端最小覆盖包直接视为 v4 的适用补丁。历史全功能口径仍保留 `TEST_ONLY`，本次不提升为完整可用。

## 4. 如何做源码检查

以下步骤沿用现有 [README 的 Verification](../README.md#verification) 和其链接的 [TESTING.md](TESTING.md)，没有发明新的安装或验收流程。

### 环境前提

- 支持环境是 Windows、PowerShell 7、CPython 3.12+。
- 在源码根目录使用项目自有的 `.venv`。
- 先按 [README Setup](../README.md#setup) 处理锁定依赖；下面命令不是新克隆的一键安装器。
- 不复制他人的环境、运行数据库、真实凭据或授权材料。不要为通过测试自行重建签名或放宽权限校验。

### 已有环境与源码检查命令

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

`Mode Verify` 包含独立生产环境的锁定依赖安装，可能访问配置的软件包索引；不要把它描述成完全离线操作。

### 可移植核心冒烟测试

以下七模块命令原载于 [TESTING.md](TESTING.md)，需要先准备符合锁文件的环境：

```powershell
& .\.venv\Scripts\python.exe -B -m pytest -q `
    tests/options_copilot/test_evidence_store.py `
    tests/options_copilot/test_domain_risk_ranking.py `
    tests/options_copilot/test_broker_snapshot.py `
    tests/options_copilot/test_ibkr_readonly_gateway.py `
    tests/options_copilot/test_config_security.py `
    tests/options_copilot/test_frontend_contract.py `
    tests/options_copilot/test_frontend_health_refresh.py
node --check options_copilot/frontend/app.js
```

这只是指定冒烟范围。完整测试目录还依赖未公开分发的真实授权、合约及迁移材料，可能在收集阶段或执行阶段失败。现有文档记载的 2026-10-02 **236 passed** 包括上述七模块和另一个明确列出的依赖文档检查；这是历史发布结果，**本次未运行**。

## 5. 当前检查状态与安全边界

### 本次实际核对

- 完整远端树可读取，返回未截断；基线及上述路径已核对。
- 初始分支列表只有 `main`；本次审阅内容另建独立文档分支。
- 基线树没有 `.github/workflows/` 文件。
- 查询时 GitHub Actions 返回 0 个运行，基线 commit 返回 0 个 check run、0 个 commit status。
- 空检查列表不等于 CI 通过；状态接口的 `pending` 也不能单独解释为有任务正在执行。
- 本次没有运行测试，不能补发“全绿”或生产可用结论。

### 审阅和本地配置须保留的边界

- 使用受支持的启动器，仅绑定 `127.0.0.1`；不因仓库公开而公开本地服务。
- IBKR 永久只读，不提交、修改或撤销订单。
- 保留 `SUPPORTING_ONLY`、研究/预览和 `NO_TRADE` 的区分。
- `analytics/signed_features.py` 的生产入口仍显式拒绝为 `FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED`。
- 依照 [STATUS.md](STATUS.md) 检查报价时间基准、上游恢复、历史覆盖、IV 可比性与生产资格等剩余问题。
- 凭据、账户与持仓数据、真实账本、数据库、日志、签名私钥和本地授权材料不应进入公开审阅提交。
- 新增文档不包含凭据值、私人文件下载地址、内部任务标识或账户金融明细。

## 6. 下一步验收清单

1. **先审阅远端基线。** 确认功能划分、安全门槛和已知缺口是否符合项目目标。
2. **单独整理云端增量。** 取得可读取文件，核验它声明的基线及逐文件哈希，区分新增文件与 provider 修改，并复跑覆盖包规定的检查。
3. **单独整理 Windows 增量。** 从实际工作区取得采集器、CLI、HTTP 研究接入、API/前端接线及测试，完成隐私筛选和文件级差异。
4. **明确版本线后再整合。** 对 v4 先取得完整材料并比较基线；不跨版本套用最小覆盖包。
5. **在真实目标环境重测。** 分别记录源码检查、锁定依赖、限定离线测试、只读集成与 GUI 结果，给每项绑定确切 commit 和时间。
6. **确认后另行合并。** 当前分支仅供审阅；源码整合、合并、部署、常驻采集和实时数据验收均需各自的实际交付证据。

---

本索引描述 2026-10-08 的仓库审阅范围。市场数据、账户状态与生产资格都具有时效性，不由本文或历史测试结果替代。
