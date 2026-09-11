# 发布与隐私边界

## 本次发布

- 目标：用户批准的私有 GitHub 仓库 `davidxxxxx/OptionsCopilot`。
- 内容：当前应用源码、经过核验的文本测试样例、测试、脚本、锁定依赖和整理后的功能文档。
- 方式：从当前文件建立独立、无旧父提交的发布快照；不上传本地开发分支的整段历史。
- 原本的本地开发分支、暂存区、未提交改动和运行数据保留。后续同步应继续使用经审查的
  文件清单，不能把 `master`、全部分支或 tags 一并推送来替代该清单。

## 不上传

账户/持仓/成交快照、真实 NAV 数据与个人化策略 NAV 合约、凭据、私钥、DPAPI 材料、
本地授权证据、SQLite / DB 文件及 WAL/SHM、日志、备份、虚拟环境、缓存、临时文件，
以及 `.planning/`、`.codex/`、`.omx/` 和旧的本地操作/验收记录。

以下治理文件不分发：`strategy_nav_contract.v1.json` 包含个人财务状态，
`execution_cost_contract.v1.json` 包含账户衍生的佣金校准与执行证据，
`initial_champion_scenario_policy.v1.json` 绑定了上述私有合约。它们均位于
`options_copilot/governance/`；本地原件不改写、不重签、不删除。

部分持仓测试复用了真实观察的期权组合，因此以下文件也不分发：
`test_holdings_payoff.py`、`test_holdings_close.py`、`test_holdings_projection.py`、
`test_position_management_runtime_adapter.py`、`test_gld_management_pipeline.py`、
`test_position_manager.py`。本地测试原件全部保留。

源码及对应测试仍保留固定的 `human:xujie` 操作员标识、联系信息占位标识、历史 contract/hash 常量，
以及少量仅按 ticker 区分的既有行为逻辑（不含真实持仓到期日、行权价、方向和数量组合）。
这些是既有协议身份和校验逻辑，不是密钥，也不是新环境的授权；本次未为了去标识化而改变
应用行为或重签合约。因此这是一份经隐私筛选的私有源码归档，不是完全匿名化的公开发行版。

私有仓库不是凭据保险箱。仍需检查文件内容、Git 对象树和将要推送的可达提交，而不只依赖
`.gitignore`。实际凭据检查只在本地进行，不把密钥提交给第三方扫描服务。

## 测试与可重复性

`tests/options_copilot/fixtures/evidence_v1.sql` 是一个合成新闻证据样例，不含账户或行情记录。
迁移测试在 pytest 的临时目录中重建 v1 数据库；源码包不携带二进制数据库。原有本地 `.db`
样例保留但不发布。

测试范围和命令见 [TESTING.md](TESTING.md)。测试运行需要项目锁定环境。测试通过只能覆盖其实际输入；本地完整研发副本的测试结果与
排除个人状态/历史资料后的源码快照测试结果须分别记录，不得互相替代。发布检查不调用
IBKR，不运行现场验收脚本，不签名、不创建指令、不变更订单。

## 新环境使用限制

仓库用于源码阅读、离线研究和继续开发，不是可直接恢复个人账户的备份。Windows / PowerShell 7 /
CPython 3.12+ 与项目 `.venv` 是支持环境；当前环境维护脚本还要求项目位于 `G:\OptionsCopilot`。
不要用 clone 或安装步骤覆盖已有的运行目录。

克隆不会复制 API 权限、市场数据订阅、真实人类签名、策略 NAV 账本或本机 DPAPI 身份。
需要本地授权的流程可能明确不可用，这属于设计边界而不是需要绕过的报错。
运行状态与未完成项以 [STATUS.md](STATUS.md) 为准。
