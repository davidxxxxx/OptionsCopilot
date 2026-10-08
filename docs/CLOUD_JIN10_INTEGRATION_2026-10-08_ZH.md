# 云端 Jin10 源码整合与离线复测（2026-10-08）

## 审阅结论与已知限制

**本次仅恢复并整合可审阅的冻结源码，不认定其达到生产可用标准。独立复核发现以下两项边界缺口；历史包内对清理和硬时限的描述必须结合本节阅读。**

1. **初始化响应未完成解析时可能遗漏 session 清理。** 服务端在 initialize 响应 header 中提供合法 `Mcp-Session-Id`，但 body 为无效 JSON 或 RPC id 不匹配时，`_read_response()` / `_request()` 在返回前抛错；`fetch_news()` 尚未保存 session ID，因此不会 DELETE，清理摘要仍为 `NOT_REQUIRED`。离线模拟分别得到 `BAD_JSON` / `PROTOCOL_ERROR`，请求序列只有 `POST`。该路径失败关闭，不会获得 PASS，但不能宣称所有已分配 session 都被清理。
2. **内部 60 秒告警不是 finally 清理的绝对截止时间。** 一次性 SIGALRM 启动异常展开后，finally 中的 DELETE 仍可继续执行；socket timeout 是单次操作上限，无法保证整个清理阶段的绝对耗时。离线模拟中，0.03 秒告警加 0.12 秒清理于约 0.150 秒后退出，再向外传播 deadline 异常。该路径也不会获得 PASS，但不能宣称内部机制独立保证严格 60 秒结束。

如以后另获实时运行授权，**必须保留进程外超时保护**，例如原验收命令的
`timeout --foreground --kill-after=2s 60s ...`：60 秒发送终止信号，必要时再过 2 秒强制终止。
本次没有运行该实时命令，没有把这两项修成新版本，冻结包的 11 个文件保持字节一致。
后续若修复，应增加对应回归测试、重新独立审阅，并单独说明新版本与本冻结包的差异。

## 整合范围

本次将 2026-10-06 的最小云端 Jin10 交付包应用到独立审阅分支
`review/options-project-2026-10-08`。整合前提交为
`a63657db55a8b4122291ef52ab1b7a30b9094801`，其相对包声明的基线
`0cac427ad993d4b89ad43a721a6aa249df3aa440` 只增加了审阅索引。
四个声明的基线文件 SHA-256 全部匹配；没有跨版本套用到 v4。

交付包 `OptionsCopilot-cloud-jin10-acceptance-20261006.zip` 为 52,775 字节，
SHA-256 为 `0714331552f79d518e41f56391db222f7543772fbbf4e9b2cee311a7ed573c8c`。
解包前检查了绝对路径、父目录跳转、反斜线、重复路径、符号链接、加密条目和尺寸。
包内校验清单的 11 个交付文件全部核验通过，并保持原始字节：

- `options_copilot/providers/jin10_mcp.py`
- `options_copilot/providers/jin10.py`
- `options_copilot/operations/cloud_jin10_research.py`
- `tests/options_copilot/test_jin10_mcp.py`
- `tests/options_copilot/test_cloud_jin10_research.py`
- `requirements-dev.lock`
- `docs/CLOUD_JIN10_RESEARCH_ACCEPTANCE_ZH.md`
- `docs/cloud_jin10_baseline_manifest.json`
- `docs/cloud_jin10_offline_validation.json`
- `README_验收交付.md`
- `SANITIZED_LIVE_ACCEPTANCE.json`

锁文件原已相同，故没有依赖版本变更。其 SHA-256 为
`6cc4184118df942b008c8a855447a019526f99defa8da2b510cd14e6fdd66573`。
原包的 2026-10-06 验收材料作为历史证据保留；本文件和更新后的审阅索引记录本次整合事实。

## 新增代码边界

- 独立的一次性云端研究入口只消费当前进程已有的 `JIN10_MCP_NETWORK_TOKEN`。
- 固定 `https://mcp.jin10.com/mcp`，只调用 `list_flash({})` 首屏，最多 20 条，不跟随 cursor。
- 保留生产凭据合同，不读取或复制本地凭据，不访问 DPAPI。
- MCP 配置主时限 50 秒、单请求上限 8 秒、session 清理预算 2 秒、入口告警 60 秒；绝对时限限制见上方已知缺口，必须保留进程外保护。
- 对成功返回给调用方的 session 记录清理确认，清理未确认时不得返回 PASS；初始化解析提前失败的 session 追踪缺口见上方说明。
- 接收时间在远端调用完成后采集；证据入库后关闭、重开并核验完整性与哈希。
- 测试注入产生 `TEST_PASS`；live 入口不接受 opener、environment、clock 或 symbols 注入。
- 结果仅为 `SUPPORTING_ONLY`，不授予 production、approval、account、IBKR 或订单权限。

## 本次环境与检查结果

环境为 Linux、CPython 3.12.14；使用新的隔离虚拟环境，以
`uv pip sync --require-hashes requirements-dev.lock` 安装哈希锁定依赖。
依赖安装可访问软件包索引；下列 Jin10 测试使用模拟 transport，不进行实时 MCP 调用。
测试子进程移除了两个 Jin10 凭据环境变量。

| 检查 | 本次结果 |
| --- | --- |
| Jin10 聚焦测试 | 31 passed |
| Jin10 相关回归 | 164 passed，4 deselected |
| 指定核心冒烟集，默认 `America/Los_Angeles` 时区 | 234 passed，1 failed，1 skipped |
| 同一核心冒烟集，明确 `TZ=Asia/Shanghai` | 235 passed，1 skipped |
| 全部应用、测试与脚本 Python AST 解析 | 394 个文件通过 |
| 静态订单权限审计 | 210 个应用文件、5,336 个 callable；0 违规 |
| 独立追加边界检查 | 确认上述初始化 session 追踪与 finally 超时两项限制；不提升为生产通过 |
| 前端 JavaScript 语法 | 通过 |
| 完整暂存差异 `git diff --cached --check` | 原包 `README_验收交付.md` 末尾有一个空行警告；保留冻结字节，其余文件无空白错误 |
| `uv pip check` | 29 个已安装包兼容 |
| 原锁定环境核验器 | 仅缺少 Windows marker 包 `pywin32`、`tzdata`；无 surplus，无版本不匹配 |

四个 deselected 是既有 `provider_probe` 测试，fixture 要求 Windows `G:` 盘语义；
本次没有修改或绕过其断言。一个 skipped 是明确标记的 Windows DPAPI 测试。

核心冒烟默认时区失败来自
`test_frontend_health_refresh.py::test_periodic_health_refresh_updates_real_daily_and_next_window_surfaces`：
它期待 `23:30`，当前系统本地化输出为 `08:30`。在整合前的干净 `a63657d` 工作树上复现相同失败；
明确设置该测试期待的上海时区后，整个指定冒烟范围通过。前端源码和该测试均未修改。
因此不将默认环境结果称为全绿，也不将该已有时区假设归因于 Jin10 增量。

## 可重跑命令

以下在仓库根目录执行；`PYTHON` 指向上述锁定虚拟环境中的解释器。

```bash
export PYTHON=/path/to/isolated-venv/bin/python
env -u JIN10_MCP_NETWORK_TOKEN -u JIN10_MCP_TOKEN PYTHONPATH=. \
  "$PYTHON" -B -m pytest -q \
  tests/options_copilot/test_jin10_mcp.py \
  tests/options_copilot/test_cloud_jin10_research.py

env -u JIN10_MCP_NETWORK_TOKEN -u JIN10_MCP_TOKEN PYTHONPATH=. \
  "$PYTHON" -B -m pytest -q \
  tests/options_copilot/test_jin10_mcp.py \
  tests/options_copilot/test_cloud_jin10_research.py \
  tests/options_copilot/test_jin10_credentials.py \
  tests/options_copilot/test_event_providers.py \
  tests/options_copilot/test_news_runtime.py \
  tests/options_copilot/test_evidence_store.py \
  -k 'not provider_probe'

env -u JIN10_MCP_NETWORK_TOKEN -u JIN10_MCP_TOKEN TZ=Asia/Shanghai PYTHONPATH=. \
  "$PYTHON" -B -m pytest -q \
  tests/options_copilot/test_evidence_store.py \
  tests/options_copilot/test_domain_risk_ranking.py \
  tests/options_copilot/test_broker_snapshot.py \
  tests/options_copilot/test_ibkr_readonly_gateway.py \
  tests/options_copilot/test_config_security.py \
  tests/options_copilot/test_frontend_contract.py \
  tests/options_copilot/test_frontend_health_refresh.py \
  tests/options_copilot/test_dependency_lock.py::test_readme_describes_index_capable_verification_without_offline_claim

"$PYTHON" scripts/verify_locked_environment.py --lock requirements-dev.lock
node --check options_copilot/frontend/app.js
PYTHONPATH=. "$PYTHON" -B -m options_copilot.operations.authority_audit --json
git diff --cached --check
```

最后一组中的锁定环境核验器在 Linux 上会报告上述两个 Windows marker 包缺失，
不能把其原始 `MISMATCH` 返回改称 PASS；其余已安装包的精确版本与哈希安装另有核验。

## 未完成或未执行

- Windows 市场观察采集器、CLI、HTTP 研究适配器及对应 API / 前端增量尚未取得，仍未整合。
- 未恢复或整合 v4；未修改 `DESIGN.md`。
- 未运行新一轮实时金十 MCP 验收、24 小时采集、券商连接、GUI 或完整期权流程。
- 未运行需要非公开金融合约、账户授权或历史迁移材料的完整测试套件。
- 未上传真实凭据、session 值、账户或订单数据、新闻原文、运行数据库或日志。
- 变更只供独立分支审阅；未合入 `main`，未创建 PR、部署或更改权限。

历史真实验收摘要并不证明当前新闻时效、长期服务、完整历史覆盖或生产可用性。
