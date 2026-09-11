# 源码快照测试说明

本仓库是经隐私筛选的源码归档，不包含原安装环境的金融合约、账本和历史迁移材料。
因此必须区分可移植的离线测试、本地授权测试，以及真正的运行时验收。

## 可移植核心检查

在 Windows / PowerShell 7 中，先准备与 `requirements-dev.lock` 一致的 CPython 3.12+
虚拟环境；不要复制其他人的 `.venv`、密钥或运行数据。以下命令假定已经位于源码根目录，
且 `.venv` 已存在。锁定环境核验命令见 [README](../README.md#setup)。

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

这是明确列举的核心冒烟集，不是所有测试，也不证明其他代码全部通过。
它检查合成证据库迁移、确定性风控、原子快照、模拟只读网关、配置安全与前端契约。
证据库迁移样例来自合成 SQL，在测试临时目录重建数据库，不需要分发真实 DB 文件。

## 需要原安装材料的测试

以下范围仍保留测试源码，但不能靠一个新的 clone 复现全部前置条件：

| 测试范围 | 缺少的真实前置条件 |
| --- | --- |
| NAV、campaign、proposal gate、bridge、Top-10 NAV economics | 个人策略 NAV 合约及其绑定 |
| execution cost resolver、scenario、A-grade、signed features、经济门槛 | 账户佣金校准合约或其绑定的初始策略合约 |
| policy authority、部分 learning/outcomes/runtime/production/Top-10 正向集成用例 | 上述合约与真实授权的组合依赖 |
| plan allowlist、phase-2 security、部分 governance source-bound 断言 | 被排除的旧计划、P0 证据和迁移语料 |

`test_economic_gates.py`、`test_policy_authority.py` 在收集测试时就读取不分发的合约。
直接运行整个目录可能在 collection 阶段报错；绕过 collection 后，其他依赖真实合约的
用例仍可能报 `FileNotFoundError`、`SIGNED_EXECUTION_COST_CONTRACT_INVALID`、
`POLICY_UNAVAILABLE` 或 `PRODUCTION_COMPOSITION_UNAVAILABLE`。这些错误不能用
编造签名、放松校验或回填真实账户资料到 GitHub 的方式解决。

六个包含真实观察持仓几何的本地测试文件没有分发，名单见 [发布边界](PUBLISHING.md)。
所以发布包的测试数量本来就少于原安装环境；不应把缺少的测试称为通过。

## 发布核验口径

2026-09-12 核验结果：上面的七模块核心冒烟集在干净副本上 **235 passed**。
这不包含被排除的私有持仓测试，也不需要个人金融合约。

整理过程中还在一个候选副本上执行了扩大诊断集：**4107 passed、262 failed、7 errors、
2 skipped**（另外 2 deselected）。该候选当时尚包含后来排除的部分持仓测试，所以这不是
最终发布包的全套验收结果。失败涉及未分发的成本/策略授权、独立副本没有自己的 `.venv`
而触发环境身份断言，以及其他集成断言；不把它们全部称为通过或静默隐藏。
本次工作不修复交易/模型业务路径，也不承诺完整测试套件全绿。

发布检查使用经逐文件哈希核验的独立干净副本，Python 导入位置限定在该副本；
不借用原目录中的私有合约和数据库。测试报告与临时目录留在本机忽略目录，不上传。
应用 Python 代码在本次整理中未修改；唯一测试行为调整是把二进制 DB 样例改为合成 SQL 重建。

源码语法检查、锁定依赖核验和静态权限审计只提供对应范围的证据。完整研究/建议流程还需要
当前券商对账、逐腿新鲜报价、完整历史口径、模型/授权、日程执行以及可见 GUI 验收；
见 [STATUS.md](STATUS.md)。本次发布不执行这些实盘验收，也不签名或创建交易指令。
