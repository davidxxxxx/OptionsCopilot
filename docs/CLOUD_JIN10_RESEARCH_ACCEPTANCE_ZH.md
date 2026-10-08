# 金十云端公开新闻一次性验收说明

## 身份与范围

- 新审计基线：Git checkout `0cac427ad993d4b89ad43a721a6aa249df3aa440`，分支 `work`。
- 该基线独立于 Library v4；本文和本补丁不声明二者内容相同。
- Library v4 未被恢复、替换或修改。本补丁没有提交、推送或部署。
- 唯一远端目标固定为 `https://mcp.jin10.com/mcp`。
- 只读取当前进程已有的 `JIN10_MCP_NETWORK_TOKEN` 网络代理占位认证；不读取、设置或复制 `JIN10_MCP_TOKEN`，不访问 DPAPI。
- 仅运行公开新闻研究链，不组合账户、IBKR、审批、指令或下单服务。

## 审计发现

原有生产 composition 只接受本地 `JIN10_MCP_TOKEN`、本地绑定及 DPAPI 激活证据，不能合法消费云端 Network secret。原有 MCP 新闻方法还会同时请求 `list_flash` 和 `list_news`，不满足本次只验收已核实 `list_flash` schema 的边界。

补丁保留生产凭据和生产 composition 原样，新增独立的一次性云端研究入口：

```text
JIN10_MCP_NETWORK_TOKEN
  → fixed Jin10 MCP transport (flash_only)
  → Jin10EventProvider (最多 20 条)
  → NewsCoordinator.refresh_once()
  → 独立 EvidenceStore
  → close → reopen → integrity/hash reread
```

`list_flash` 只接受以下已核实 input schema：object、唯一 optional `cursor:string`、`additionalProperties=false`。首次调用参数严格为 `{}`，不跟随 `next_cursor`，不调用 `list_news`。

## 强制边界

- 一次运行、一个 MCP session、一页、最多 20 条。
- live 入口自身用不可被 transport/provider/coordinator 的 `except Exception` 捕获的内部 deadline 信号强制总时限 60 秒；MCP session 上限 50 秒，为独立的 2 秒 session DELETE 预算及本地关闭、重开和摘要留出余量。验收命令外层 `timeout` 是第二道进程级保护。
- 单请求上限 8 秒；响应上限 2 MiB。
- 禁止 redirect；initialize 响应一旦分配 session，立即保存 session ID，后续协议、capability、通知、工具发现或调用异常均在 `finally` 中清理。DELETE 使用独立的 2 秒清理 deadline，不复用可能已经耗尽的主 deadline。机器摘要只记录固定枚举清理证据；已分配 session 时，只有 DELETE 返回 200/202/204 才是 `CONFIRMED`，否则整次验收失败，未分配 session 才是 `NOT_REQUIRED`。
- 429 不重试；认证失败以固定原因码显式失败。
- 源发布时间来自金十记录；接收时间在 MCP 调用返回后采集。
- 覆盖状态固定为 `BOUNDED_FIRST_PAGE_ONLY`，`complete_24h=false`。
- 所有记录为 `SUPPORTING_ONLY`；production、approval、account、IBKR、order 权限均为 false。
- 摘要不包含标题、正文、认证值或请求 header，只包含来源标识、时间、内容哈希和 row hash。
- live CLI 不接受 opener、environment、clock 或 symbols 注入；测试专用内部入口产生 `TEST_PASS` 和 `transport.mode=TEST_INJECTED`，不能冒充 live `PASS`。
- 真实新闻数据库及运行摘要只留在显式指定的新临时目录，不上传。

## 离线复核

测试环境由 `requirements-dev.lock` 创建，锁文件 SHA-256 为 `6cc4184118df942b008c8a855447a019526f99defa8da2b510cd14e6fdd66573`。

锁定环境核验器在本 Linux 环境报告仅缺少 Windows marker 包 `pywin32`、`tzdata`，无 surplus、无版本不匹配；`uv` 按平台 marker 正常跳过了这两个 Windows 包。

精确聚焦测试命令：

```bash
PYTHONPATH=. /tmp/options-copilot-audit-venv/bin/python -m pytest -q \
  tests/options_copilot/test_jin10_mcp.py \
  tests/options_copilot/test_cloud_jin10_research.py
```

独立审查缺陷及 session 清理证据门禁修复后的最终复跑结果：`31 passed in 0.68s`。

相关回归命令：

```bash
PYTHONPATH=. /tmp/options-copilot-audit-venv/bin/python -m pytest -q \
  tests/options_copilot/test_jin10_mcp.py \
  tests/options_copilot/test_cloud_jin10_research.py \
  tests/options_copilot/test_jin10_credentials.py \
  tests/options_copilot/test_event_providers.py \
  tests/options_copilot/test_news_runtime.py \
  tests/options_copilot/test_evidence_store.py \
  -k 'not provider_probe'
```

结果：`164 passed, 4 deselected in 7.75s`。四项 `provider_probe` 是固定断言 Windows `G:` 盘的既有平台专用测试；在 Linux 云环境直接运行会于 fixture setup 报 `root.drive == "G:"`，与本补丁逻辑无关。

## 真实验收命令

独立复核确认冻结点后，选择一个不存在的新临时目录运行：

```bash
PYTHONPATH=. timeout --foreground --kill-after=2s 60s \
  /tmp/options-copilot-audit-venv/bin/python \
  -m options_copilot.operations.cloud_jin10_research \
  --output-dir /tmp/options-copilot-jin10-live-<unique>
```

成功时目录内生成以下主文件（SQLite 在打开期间还可能出现标准 WAL/SHM 辅助文件）：

- `jin10_cloud_news_evidence.sqlite3`
- `jin10_cloud_news_evidence.news-analysis.sqlite3`
- `weekly_briefs.sqlite3`
- `jin10_cloud_research_summary.json`

机器摘要会报告真实 provider health、实际记录数、源发布时间、接收时间、重开校验状态、content hash、row hash 和摘要 canonical SHA-256，不包含秘密或新闻正文。
