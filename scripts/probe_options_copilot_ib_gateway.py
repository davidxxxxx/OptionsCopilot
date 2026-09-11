"""Bounded IB Gateway listener probe that never authenticates or starts Gateway."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from options_copilot.config import OptionsCopilotConfig  # noqa: E402
from options_copilot.gateway.ibkr_readonly import (  # noqa: E402
    GatewayReadinessStatus,
    probe_ibkr_gateway_readiness,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Probe the configured IB Gateway TCP listener and ib_insync dependency "
            "without an IB API handshake, login, account read, or order call."
        )
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=1.0,
        help="TCP probe timeout, bounded internally to 0.05-5.0 seconds.",
    )
    args = parser.parse_args(argv)

    config = OptionsCopilotConfig.from_env()
    result = probe_ibkr_gateway_readiness(
        config,
        timeout_seconds=args.timeout_seconds,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if result.status is GatewayReadinessStatus.LISTENER_READY else 2


if __name__ == "__main__":
    raise SystemExit(main())
