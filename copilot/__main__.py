"""Command line: ``python -m copilot {serve,run,news,check}``."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="copilot", description="OptionsCopilot 期权副驾")
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="启动网页界面和自动调度")
    serve.add_argument("--port", type=int, help="覆盖配置中的端口")
    serve.add_argument("--no-scheduler", action="store_true", help="只开界面，不自动运行")

    run = commands.add_parser("run", help="立即完整运行一次")
    run.add_argument("--no-ibkr", action="store_true", help="跳过 IBKR（只做新闻和观点）")
    run.add_argument("--no-llm", action="store_true", help="跳过大模型，使用中性观点")

    commands.add_parser("news", help="只采集一次新闻")
    commands.add_parser("check", help="检查配置、密钥和 IBKR 连通性")

    args = parser.parse_args(argv)
    if sys.stdout is None or sys.stderr is None:  # pythonw / scheduled task: no console
        _redirect_console()
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    if args.command == "serve":
        return _serve(args)
    if args.command == "run":
        return _run(args)
    if args.command == "news":
        return _news()
    return _check()


def _redirect_console() -> None:
    from copilot.config import load_settings

    log_dir = load_settings().log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    stream = open(log_dir / "console.log", "a", encoding="utf-8", buffering=1)  # noqa: SIM115
    sys.stdout = stream
    sys.stderr = stream


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    from copilot.runtime import build_runtime
    from copilot.web.app import create_app

    runtime = build_runtime(with_scheduler=not args.no_scheduler)
    port = args.port or runtime.settings.web.port
    app = create_app(runtime)
    if runtime.scheduler is not None:
        runtime.scheduler.start()
    print(f"OptionsCopilot 已启动：http://{runtime.settings.web.host}:{port}")
    try:
        uvicorn.run(app, host=runtime.settings.web.host, port=port, log_level="warning")
    finally:
        runtime.close()
    return 0


def _run(args: argparse.Namespace) -> int:
    from copilot.pipeline import run_pipeline
    from copilot.runtime import build_runtime

    runtime = build_runtime(with_scheduler=False, use_ibkr=not args.no_ibkr, use_llm=not args.no_llm)
    try:
        run_id = run_pipeline(runtime.deps, trigger="cli", slot="manual")
        run = runtime.store.get_run(run_id) or {}
        stages = run.get("stages", {})
        print(json.dumps({
            "run_id": run_id,
            "status": run.get("status"),
            "error": run.get("error"),
            "news": stages.get("news"),
            "ibkr": stages.get("ibkr"),
            "tagging": stages.get("tagging"),
            "views": {k: v for k, v in (stages.get("views") or {}).items() if k != "changes"},
            "view_changes": [
                f"{c['symbol']} {c['action']} score={c['score']}" for c in (stages.get("views") or {}).get("changes", [])
            ],
            "brief": stages.get("brief"),
            "options": {k: v for k, v in (stages.get("options") or {}).items() if k != "warnings"},
            "ideas": [
                f"{idea['rank']}. {idea['symbol']} {idea['structure_name']} {idea['expiry']} "
                f"max_loss=${idea['max_loss']} edge=${idea['edge']}"
                for idea in run.get("ideas", [])
            ],
            "timings": stages.get("timings"),
        }, ensure_ascii=False, indent=2))
        return 0 if run.get("status") in ("ok", "partial") else 1
    finally:
        runtime.close()


def _news() -> int:
    from copilot.news.collect import collect_news
    from copilot.runtime import build_runtime

    runtime = build_runtime(with_scheduler=False, use_ibkr=False, use_llm=False)
    try:
        results = collect_news(
            runtime.settings, runtime.store, runtime.deps.news_sources,
            now=datetime.now(timezone.utc), company_symbols=runtime.settings.core_symbols,
        )
        for result in results:
            print(f"{result.source:18} {result.status:8} +{result.count} {result.detail}")
        return 0 if all(result.status != "failed" for result in results) else 1
    finally:
        runtime.close()


def _check() -> int:
    from copilot.config import load_settings
    from copilot.market.ibkr import IbkrMarket, IbkrUnavailable
    from copilot.secrets import load_secrets

    settings = load_settings()
    secrets = load_secrets(settings.secrets_file)
    print(f"数据目录: {settings.data_dir}")
    print(f"密钥文件: {settings.secrets_file} ({'存在' if settings.secrets_file.exists() else '不存在'})")
    for name, ok in secrets.configured().items():
        print(f"  {name:24} {'已配置' if ok else '未配置'}")
    print(f"IBKR: {settings.ibkr.host}:{settings.ibkr.port} client_id={settings.ibkr.client_id}")
    market = IbkrMarket(settings.ibkr)
    try:
        market.connect()
        account = market.account()
        print(f"  已连接（只读）。净清算价值: {account.net_liquidation} {account.currency}，持仓 {len(account.positions)} 个")
        return 0
    except IbkrUnavailable as exc:
        print(f"  未连接: {exc}")
        return 1
    finally:
        market.close()


if __name__ == "__main__":
    sys.exit(main())
