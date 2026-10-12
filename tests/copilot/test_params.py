from __future__ import annotations

from dataclasses import replace

from copilot.params import params_version


def test_params_version_is_stable_and_tracks_only_scoring_settings(settings) -> None:
    version = params_version(settings)
    assert params_version(settings) == version
    assert params_version(replace(
        settings,
        strategy=replace(settings.strategy, keep_margin=settings.strategy.keep_margin + 0.01),
    )) != version
    assert params_version(replace(
        settings,
        rules=replace(
            settings.rules,
            portfolio_risk_cap_pct=settings.rules.portfolio_risk_cap_pct + 0.01,
        ),
    )) != version
    assert params_version(replace(
        settings,
        web=replace(settings.web, port=settings.web.port + 1),
    )) == version
    assert params_version(replace(
        settings,
        ibkr=replace(settings.ibkr, port=settings.ibkr.port + 1),
    )) == version
