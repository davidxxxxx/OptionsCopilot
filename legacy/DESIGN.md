# Design

## Source of truth
- Status: Active
- Last refreshed: 2026-09-09
- Primary product surfaces: loopback Options Copilot overview and the read-only news/event workspace at `http://127.0.0.1:8891/`.
- Evidence surfaces: `options_copilot/frontend/index.html`, `options_copilot/frontend/app.js`, `options_copilot/frontend/styles.css`, `/api/news`, `/api/calendar`, `/api/weekly-brief`, `/api/research-top10`, and provider-configuration read models. Historical local review artifacts are not part of the source publication.
- Management boundary: `options_copilot/positions/generator.py`, `options_copilot/positions/manager.py`, `options_copilot/positions/runtime_adapter.py`, the cost contract, `/api/positions` and `/api/management/current`; strategy grouping and executable quotes must be verified from the operator's current data, never inferred from this document.

## Brand
- Personality: sober, evidence-led, compact, professional trading workstation.
- Trust signals: explicit freshness, provenance, expected-versus-actual time, classifier identity, authority labels, and honest unavailable states.
- Avoid: promotional trading language, decorative dashboards, hidden uncertainty, filler candidates, and colors that imply an order recommendation.

## Product goals
- Goals: let one operator understand today's material news, this week's scheduled risks, next week's outlook, and the path from evidence to at-most-ten option combinations.
- Non-goals: no browser-side signal invention, approval, instruction creation, order submission, or replacement for the immutable ranking pipeline.
- Success signals: the operator can distinguish provider health, article publisher, classifier, research pool, and action pool without reading implementation terminology.

## Personas and jobs
- Primary personas: the single OptionsCopilot operator reviewing US stock and ETF option opportunities.
- User jobs: scan material changes, prepare for scheduled catalysts, understand timing and uncertainty, inspect supporting evidence, and decide whether to continue to broker review.
- Key contexts of use: Beijing evening before the US session, pre-market, the first minutes after the open, and daily retrospective review.

## Information architecture
- Primary navigation: Overview; News.
- Core routes/screens: account/readiness overview; chronology-first news/event workspace; formal weekly brief; conditional option Top-10; detailed evidence and calendar reaction state.
- Content hierarchy: one-screen decision summary first; actionable and research candidates second; daily/weekly briefing third; provider, model, hash, and diagnostic evidence behind explicit detail disclosures.

## Design principles
- Lead with decisions in time: today, this week, next week, then detailed feeds.
- Use progressive disclosure: the default view answers `现在能否行动 / 为什么 / 下一次何时更新`; technical evidence remains available without competing with the decision.
- Keep three truth lanes visually distinct: `当前可执行`, `研究候选`, and `数据/模型覆盖`; never combine their counts or status colors.
- Separate concepts: provider health is not article publisher; publisher is not classifier; classifier is not trading authority.
- Sort transparently: news uses existing `event_impact_score`; calendar uses existing `importance`, then market-wide macro scope, exactness, and scheduled time.
- Preserve uncertainty: expected, actual/released, and source-published times are separate fields and missing values remain visible.
- Tradeoffs: the briefing is a live browser projection for readability; immutable weekly, scan, and ranking artifacts remain the authority-bearing records.

## Visual language
- Color: retain the dark graphite palette; cyan for navigation/data, amber for supporting or pending states, green only for verified readiness, red only for blocked/error states.
- Typography: Segoe UI/Microsoft YaHei for reading; Cascadia Code/monospace for hashes, timestamps, and protocol status.
- Spacing/layout rhythm: 8–12px internal rhythm, 12px panel gaps, a compact four-cell decision strip, and short cards with secondary fields collapsed by default.
- Shape/radius/elevation: existing 5px radius and border-led elevation; no new shadow system.
- Motion: existing short hover transitions only.
- Imagery/iconography: text and status chips; no decorative imagery.

## Components
- Existing components to reuse: panels, status chips, count chips, news cards, calendar rows, weekly evidence cards, and empty states.
- New/changed components: overview decision strip, compact research cards, news capability strip, chronology briefing panel, impact badge, dual-time row, collapsible provider-health details, and collapsible research/workbench evidence.
- Holdings-close observation: reuse management-preview articles, proof-grid metrics and per-leg rows for the separate `options_copilot.holdings_close_preview.v1` schema. This is the exact full observed inventory, not an inferred strategy or new trade recommendation. Do not render old combination counts, gcd ratios, entry-based 60% thresholds, HOLD states or the legacy exit-plan template for this schema.
- Variants and states: major/high/medium/ordinary impact; scheduled/released/unverified; ready/degraded/down; deterministic/structured classifier.
- Token/component ownership: CSS variables and browser-native components remain in `options_copilot/frontend/styles.css` and `app.js`.

## Accessibility
- Target standard: practical WCAG 2.1 AA for text contrast, semantics, and keyboard operation.
- Keyboard/focus behavior: existing buttons and selects remain native; briefing cards are non-interactive articles.
- Contrast/readability: impact badges use text labels in addition to color; unavailable timestamps use explicit words.
- Screen-reader semantics: briefing regions and lists use labelled sections and live regions without hiding authority disclaimers.
- Reduced motion and sensory considerations: no required animation or color-only meaning.

## Responsive behavior
- Supported breakpoints/devices: current Windows desktop browser plus narrow/mobile fallback.
- Layout adaptations: three-column briefing collapses to one column; detailed news workbench retains its existing responsive rules.
- Touch/hover differences: no briefing interaction depends on hover.

## Interaction states
- Loading: existing placeholders remain until both news and calendar read models arrive.
- Empty: explain whether no current-day evidence exists or the source is unavailable.
- Error: provider-specific degraded/down status remains visible; other valid rows continue read-only.
- Success: show counts, last refresh, classifier coverage, and chronological cards.
- Disclosure: collapsed summaries state the important conclusion and item count; expansion reveals hashes, provider diagnostics, raw feeds, and model evidence without changing authority.
- Disabled: all news surfaces remain `SUPPORTING_ONLY` with no approval or instruction controls.
- Holdings-close disabled state: always `PREVIEW_UNVERIFIED / NO_TRADE`; show unverified calendar, assignment, basket-fill, legging and margin limitations beside the conditional all-legs-flat result. Missing/expired source quotes make current financial figures unavailable; retained hashes/timestamps identify historical evidence, never refreshed authority. No new request, order or review-instruction button.
- Offline/slow network: retain last rendered content while provider status/freshness communicates staleness.

## Content voice
- Tone: concise Chinese operational language with stable English protocol/status codes where diagnostically useful.
- Terminology: use `文章来源/发布者`, `采集 Provider`, `分类器`, `研究池`, and `行动池` as distinct labels.
- Microcopy rules: say `预计发生`, `实际发布/释放`, and `来源发布时间`; never collapse them into one ambiguous `时间`. Say `持续采集` instead of `全部实时`, `DeepSeek shadow` instead of `AI 已分析`, and `最多 10 个` instead of promising filler.
- Holdings terminology: `全部已观察持仓`, `策略归属未推断`, `逐腿自然买卖价合计`, and `扣除一次退出成本的条件估值`. Never call the sum a guaranteed atomic basket price. Distinguish historical entry cost, current gross liquidation reference, and the conditional intrinsic-payoff loss relative to that reference plus one future-exit reserve. The latter is not broker margin or capital released. Zero after-state is conditional on all legs being filled and reconciled, not an execution guarantee or proposed leg sequence.

## Implementation constraints
- Framework/styling system: unbundled HTML, CSS, and browser-native ES modules served by FastAPI.
- Design-token constraints: extend existing CSS custom properties; do not add a frontend toolchain.
- Performance constraints: bound briefing rows and the detailed calendar projection, derive them from already-fetched read models, and make the displayed-versus-total count explicit; no additional browser requests.
- Compatibility constraints: Windows, current Chromium-class browser, no `innerHTML`, no secret values in API or GUI.
- Test/screenshot expectations: Node syntax, frontend contract tests, focused live browser verification at desktop width, and explicit read-only/no-action checks.

## Open questions
- [ ] Whether a future server-side immutable daily digest should supplement the current live browser projection after sufficient DeepSeek shadow evidence exists / product owner / affects historical replay only.
- [ ] Whether the current QQQ put spread and long call are one intended strategy / user / blocks inferred grouping and strategy-specific stop/profit or partial-reduction recommendations, not exact full-inventory mathematical observations.
