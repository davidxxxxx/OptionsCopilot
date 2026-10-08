# OptionsCopilot 云端金十一次性验收交付

## 结论与身份边界

本包是应用于 OptionsCopilot Git 基线 `0cac427ad993d4b89ad43a721a6aa249df3aa440` 的最小源码覆盖包。它不是完整仓库，也不是 Library 文件 `OptionsCopilot-cloud-audited-source-20261002.zip` 的 v4，未声明与 v4 等价。本次没有恢复、替换或覆盖 v4，没有 Git 提交、推送或部署。

已在云环境完成真实程序链验收：

```text
JIN10_MCP_NETWORK_TOKEN
  → 固定 https://mcp.jin10.com/mcp
  → list_flash({})（一页，最多 20 条）
  → Jin10EventProvider
  → NewsCoordinator.refresh_once()
  → 独立 EvidenceStore
  → close / reopen / integrity / hash 重读
```

这不是独立 MCP 探针。生产凭据合同保持原样；新增入口只消费当前进程已有的 Network secret，不读取或复制本地 `JIN10_MCP_TOKEN`，不访问 DPAPI。

## 包内容和使用方式

将本包中保留相对路径的源码及测试文件覆盖到上述精确 Git 基线上。包内不含完整工作树、`.git`、运行数据库、新闻标题或正文、认证值、请求 header、session ID、账户资料或私有 URL。

主要文件：

- `options_copilot/providers/jin10_mcp.py`
- `options_copilot/providers/jin10.py`
- `options_copilot/operations/cloud_jin10_research.py`
- `tests/options_copilot/test_jin10_mcp.py`
- `tests/options_copilot/test_cloud_jin10_research.py`
- `requirements-dev.lock`
- `docs/` 下的验收说明、冻结 manifest 和离线验证记录
- `SANITIZED_LIVE_ACCEPTANCE.json`：脱敏后的真实验收结果
- `FILES_SHA256.txt`：包内内容哈希

## 依赖环境

在仓库根目录：

```bash
uv venv /tmp/options-copilot-audit-venv --python 3.12
uv pip sync --python /tmp/options-copilot-audit-venv/bin/python requirements-dev.lock
```

`requirements-dev.lock` SHA-256：

```text
6cc4184118df942b008c8a855447a019526f99defa8da2b510cd14e6fdd66573
```

Linux 上锁定环境核验器只报告 Windows marker 包 `pywin32` 和 `tzdata` 未安装；没有 surplus 或版本不匹配。

## 离线测试

聚焦测试：

```bash
PYTHONPATH=. /tmp/options-copilot-audit-venv/bin/python -m pytest -q \
  tests/options_copilot/test_jin10_mcp.py \
  tests/options_copilot/test_cloud_jin10_research.py
```

实现方最终结果：`31 passed in 0.68s`。

相关回归：

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

实现方结果：`164 passed, 4 deselected in 7.75s`；同一独立审阅者复跑结果：`164 passed, 4 deselected in 7.93s`。四项排除均是既有 `provider_probe` 测试，其 fixture 固定断言 Windows `G:` 盘，在 Linux 云环境不适用；它们不是金十云端逻辑失败。

## 有界真实运行

必须使用一个不存在的新临时目录：

```bash
PYTHONPATH=. timeout --foreground --kill-after=2s 60s \
  /tmp/options-copilot-audit-venv/bin/python \
  -m options_copilot.operations.cloud_jin10_research \
  --output-dir /tmp/options-copilot-jin10-live-<unique>
```

本次最终真实运行退出码为 0，约 3.068 秒完成；机器摘要 canonical SHA-256 为：

```text
783de840fc034a00f6fac8611a9c044dcb1a33ea9a54de62642d02d5e3acca9c
```

实际收到 20 条公开快讯并持久化，Coordinator 公共投影为 10 条。来源发布时间范围为 `2026-10-06T05:47:42Z` 至 `2026-10-06T06:20:17Z`，实际接收时间为 `2026-10-06T06:32:56.244729Z`。20 个 content hash 和 20 个 row hash 均由独立审阅者按数据库内容重算一致；三个 SQLite 主库均通过 `PRAGMA integrity_check=ok`。

服务端本次没有返回 MCP session ID，因此 `session_allocated=false`、`delete_attempted=false`、`result=NOT_REQUIRED`。若 session 已分配但 DELETE 未获得确认状态，当前实现会以 `SESSION_CLEANUP_FAILED` 阻止 PASS。

## 权限与限制

- 覆盖范围固定为 `BOUNDED_FIRST_PAGE_ONLY`，不代表完整 24 小时新闻。
- 全部记录仅为 `SUPPORTING_ONLY`。
- production、approval、account、IBKR、order create 和 order submit 均为 false。
- 不生成或激活新凭据，不改变网络或环境权限。
- 本包不包含运行资料；原始新闻及数据库未上传。

