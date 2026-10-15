"""One-shot CLI for local, market-only IBKR research observations."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from decimal import Decimal
import json
import logging
import sys

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.market_observation import (
    IBMarketObservationSession,
    OptionObservationRequest,
    build_research_market_input,
    collect_market_observation,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect one bounded IBKR market-only research observation")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--expiration", required=True, help="YYYYMMDD")
    parser.add_argument("--strike", required=True, type=Decimal)
    parser.add_argument("--rights", default="C,P")
    parser.add_argument("--seconds", default=8.0, type=float)
    parser.add_argument("--client-id", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    logging.getLogger("ib_insync").setLevel(logging.CRITICAL)
    try:
        config = OptionsCopilotConfig.from_env()
        client_id = args.client_id if args.client_id is not None else config.ibkr_client_id + 1
        if client_id == config.ibkr_client_id:
            raise ValueError("market observation requires an independent client id")
        request = OptionObservationRequest(
            symbol=args.symbol, expiration=args.expiration, strike=args.strike,
            rights=tuple(args.rights.split(",")), collection_seconds=args.seconds,
        )
        observation = collect_market_observation(
            request,
            session_factory=lambda: IBMarketObservationSession(
                host=config.ibkr_host, port=config.ibkr_port, client_id=client_id,
                timeout=config.ibkr_timeout_seconds,
            ),
        )
        result = {"observation": observation, "research_input": build_research_market_input(observation)}
    except Exception:
        sys.stderr.write("IBKR_MARKET_OBSERVATION_FAILED\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True, allow_nan=False) + "\n")
    return 0 if observation["status"] in {"OBSERVED", "PARTIAL"} else 3


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
