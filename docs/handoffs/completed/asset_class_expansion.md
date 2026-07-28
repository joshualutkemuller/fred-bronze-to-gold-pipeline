# Path B: Asset-class breadth expansion

**Status: IN PROGRESS.** One of two candidate "what's next" directions (see
`docs/handoffs/governance_and_access_control.md` for the other). This doc
scopes the real remaining *data* gaps once the equity/ETF, macro, and
credit-spread universes already in place are accounted for.

**Item 1a (realized volatility) — DONE.** `gold.realized_volatility` is
built: `writer/equity_views.py::compute_realized_volatility`, wired into
both Gold backends, DDL in `sql/50_gold.sql` / `local_store.py`, data
dictionary + Power BI catalog entries, tests in `tests/test_equity.py`. See
the item below for the as-built grain/methodology.

**Item 2 (bond/CDS/muni pricing) — DONE / CLOSED (2026-07-26).**
`manifests/corporate_bond_yields.yml` (32 series) closes the free aggregate
half via a FRED `discover` pass. Individual-issuer/CUSIP pricing is
explicitly **not being pursued** (user decision) — see the item below for
what was found and why the muni side is a dead end without a paid vendor.

**Item 3 (commodities futures) — SCOPED, not built (2026-07-26).** Live-
verified against `api.eia.gov`: EIA genuinely published exactly this data
(20 NYMEX futures instruments, WTI/heating oil/gasoline/propane, contracts
1-4, back to 1980) but discontinued it on 2024-04-05 — over two years
stale, not usable as a live source. This item is now gated on the same
paid-vendor decision as 1b. See the item below for the full finding,
including a separate discovered bug (the existing `EIAClient` hits a now-
dead legacy route and would fail for *any* EIA series today, unrelated to
futures specifically).

Item 1b remains unstarted.

## Context

**What's already covered — confirmed by direct manifest inspection, not
assumption:**
- **FX: already extensive.** `manifests/international.yml` has 24 spot pairs
  (`DEXUSEU, DEXJPUS, DEXCHUS, DEXCAUS, DEXKOUS, DEXUSUK, DEXMXUS, DEXINUS,
  DEXBZUS, DEXUSAL, DEXSZUS, DEXTHUS, DEXMAUS, DEXVZUS, DEXTAUS, DEXHKUS,
  DEXSFUS, DEXSIUS, DEXSDUS, DEXUSNZ, DEXNOUS, DEXDNUS, DEXSLUS`) plus the
  `DTWEXAFEGS`/`DTWEXBGS` trade-weighted indices. An earlier, looser framing
  of this project's own next steps assumed FX was a gap — it checked out
  false on inspection, so it's deliberately **excluded** from the list below.
- **Credit spreads: covered at the aggregate/index level.**
  `manifests/ice_credit.yml` has 12 ICE BofA OAS/effective-yield series
  spanning IG (`BAMLC0A0CM`) through the full rating ladder
  (AAA/AA/A/BBB/BB/B/CCC) plus EM corporate (`BAMLEMCBPIOAS`). This answers
  "what's the market's spread for BBB credit today" but not "what's this
  specific bond worth" — that distinction drives item 2 below.
- **Equities: broad and getting broader.** 85 ETFs (58 curated + 27
  gap-filling additions just added) plus dynamic S&P 500 constituent pricing
  via Tiingo (507 names, daily price + adjusted close).
- **Commodities: spot only.** `manifests/market_indices.yml` has exactly one
  commodity series, `DCOILWTICO` (WTI spot). No futures curve, no other
  commodities (gold/silver/copper/nat gas/ags) beyond the ETF wrappers
  already in `equity_tiingo.yml` (`GLD` added this session, plus `GDX`/
  `GDXJ`). An ETF price is a decent spot proxy but carries none of the
  term-structure/roll-yield information that actually matters for a
  commodities desk.
- **Options/volatility: completely absent.** Repo-wide search confirms the
  *only* volatility-related series anywhere is `VIXCLS` (the index level
  itself, from FRED). There is no options chain data, no implied-vol
  surface, no realized-vol calculation, anywhere in Bronze, Silver, or Gold.

## 1. Options / volatility-surface data — *(highest value, hardest sourcing)*

**Item 1a status: DONE.** `gold.realized_volatility` — one row per ticker ×
date × trailing window (21/63/126/252 trading days ≈ 1mo/1qtr/2qtr/1yr):
`realized_vol_pct` is the annualized sample stdev of daily log returns over
the window (log returns, not `equity_return_daily`'s simple returns, since
they're time-additive — required for the sqrt(time) annualization scaling
to be valid). A row only emits once its window is fully populated (no
partial-window stats), matching the rolling-companion convention used by
`curve_spread_rolling`/`treasury_curve_rolling` elsewhere in Gold. Item 1b
(implied vol/options chains) below is unchanged — still paid-vendor-gated.

**The problem.** `VIXCLS` gives one number (30-day S&P implied vol). It
can't answer "what's AAPL's 25-delta skew," "what's the term structure of
SPX implied vol," or anything a derivatives or vol-arb desk would need.
This is the single largest capability gap relative to what a real trading
desk would expect from a terminal-like product.

**Build.**
1. **Sourcing is the real blocker, not engineering.** Unlike every existing
   source in this pipeline (all free/public), real options-chain and
   implied-vol-surface data is not available for free at any meaningful
   quality — CBOE DataShop, OPRA-derived vendors (ORATS, CBOE LiveVol,
   Polygon.io options tier) all charge, several with redistribution
   restrictions that would need to go through exactly the licensing-review
   process scoped in `docs/handoffs/governance_and_access_control.md` item 1
   before onboarding. **This item is coupled to Path A** — don't onboard a
   paid options vendor without the licensing register that doc proposes,
   or this repeats the ad hoc CME situation.
2. If a source is approved: a new `sources/<vendor>.py` client following the
   existing `tiingo.py`/`fred.py` shape (rate-limited HTTP client → Bronze
   JSON), a new manifest (e.g. `manifests/options.yml`) scoped initially to
   a small curated underlier list (SPX, a handful of the most liquid single
   names already in the ETF/constituent universe) rather than the full
   listed-options universe (which is enormous and mostly illiquid strikes).
3. Gold layer: a `gold.options_surface` table (grain: `underlier · as_of_date
   · expiry · strike · option_type · implied_vol · delta · open_interest`)
   plus a derived `gold.implied_vol_summary` (ATM vol and 25-delta skew per
   underlier/expiry, the kind of compact view a terminal `VOL` module would
   read) — same config-driven engine pattern as `terminal_views.py`.
4. **Cheaper fallback if a paid feed isn't approved**: a realized-vol-only
   version computed purely from prices already in Gold (rolling stdev of
   log returns on the equity/ETF Silver series, annualized) — a
   `gold.realized_volatility` table. This is genuinely free (no new source)
   and gets partway there (realized, not implied, vol) — worth building
   regardless of the options-vendor decision, and independently useful.

**Verdict:** split this into two: **realized vol (cheap, do anytime)** vs.
**implied vol / full options chain (valuable, but gated on a paid-vendor
licensing decision)**. Don't block the cheap half on the expensive half.

## 2. Individual bond / CDS / municipal-curve pricing — *(narrower, moderate cost)*

**Status: aggregate/benchmark half DONE; individual-issuer half explicitly
descoped (2026-07-26 decision — not pursuing individual-issuer/CUSIP
pricing).** The FRED `discover` pass below was run and confirmed real,
live results — `manifests/corporate_bond_yields.yml` (32 series: Moody's
seasoned Aaa/Baa corporate yields + 10Y-Treasury- and Fed-Funds-relative
spreads, daily since the 1980s; ~26 more ICE BofA yield-to-worst and
total-return-index cuts across the rating ladder, complementing the
OAS/effective-yield series already in `ice_credit.yml`). The municipal side
of the same search came up empty for anything current — all 7 hits were
NBER macrohistory archive series ending in 1966-67, confirmed via direct
metadata lookup — so muni-curve coverage still has no free path. Since
individual-issuer pricing is explicitly out of scope going forward, this
item is considered closed: the free aggregate expansion is captured, and
CUSIP-level/CDS/muni-curve data (TRACE, Markit/IHS) won't be pursued.

**The problem (as originally scoped).** The 12 ICE BofA series answer
"what's the market OAS for BBB credit" in aggregate. They don't answer
"what's this specific CUSIP worth," "what's this issuer's CDS spread," or
"what's the muni curve for this state." A credit desk pricing individual
names or building relative-value trades needs issuer-level data, not just
the index — this remains true, but per the above, is not being pursued.

**Build.**
1. **Sourcing**, roughly in order of cost/complexity:
   - Cheapest: FRED already carries a handful of issuer-level and
     benchmark-curve series beyond the 12 already ingested (e.g. Moody's
     seasoned corporate yields, state muni benchmark yields) — worth a
     `discover` pass (`fred_pipeline discover --search "corporate bond
     yield"` / `"municipal"`) before reaching for a paid vendor, exactly the
     workflow already used for the regional-FRED-series expansion earlier
     this project.
   - Moderate: Tiingo's fixed-income coverage (if any beyond what's already
     used for equities) or FINRA TRACE (free, delayed, but genuinely
     transaction-level bond pricing) for actual traded-bond prices.
   - Most expensive: dedicated CDS data (Markit/IHS) — likely not worth it
     unless there's a specific credit-desk use case driving it; flag as
     lowest priority within this item.
2. Gold layer: `gold.bond_pricing` (grain: `cusip_or_issuer · as_of_date ·
   price · yield · spread_to_benchmark`) — issuer/CUSIP dimension is new to
   this codebase (everything today keys on `series_id` or `ticker`), so this
   needs a small new dimension table (`gold.bond_reference`: cusip → issuer
   → sector → rating) rather than forcing it through the existing
   series-manifest shape.

**Verdict:** start with the free FRED `discover` pass — it may close part of
this gap at zero marginal engineering cost, the same way the regional-data
expansion did. Only reach for TRACE/paid vendors if the FRED pass comes up
short on what's actually needed.

## 3. Commodities futures / term structure — *(moderate value, moderate cost)*

**Status: SCOPED (2026-07-26) — EIA's free path checked live and confirmed
dead; this now needs the same paid-vendor/licensing decision as item 1b.**

**The problem.** `DCOILWTICO` (WTI spot) plus a few precious-metals ETFs is
spot-only. Commodities trading is fundamentally about the curve (contango/
backwardation, roll yield) — spot price alone misses the entire investable
thesis for a commodities desk.

**EIA's futures data exists — and was discontinued over two years ago.**
Live-queried directly against `api.eia.gov` (not assumed): EIA's
`petroleum/pri/fut` category is/was **exactly** the right dataset — 20
NYMEX futures instruments across 4 commodities × contracts 1–4 (`RCLC1`-
`RCLC4` WTI crude; NY Harbor Heating Oil, Regular Gasoline, and RBOB
Gasoline; Mont Belvieu Propane), daily, back to **1980-01-04**. But the
category's own metadata reports `endPeriod: 2024-04-05` — confirmed by
pulling `RCLC1`'s actual last rows: the most recent observation is
**2024-04-05**, over two years stale as of today. EIA discontinued this
survey; it is **not usable as a live/ongoing source**, only as a one-time
historical backfill (10,305 daily rows available for `RCLC1` alone, if a
fixed historical WTI curve for backtesting is ever wanted independent of
current data).

**A false alarm, corrected on re-verification.** An initial pass flagged
`sources/eia.py`'s legacy `seriesid/{id}` compatibility route as dead (a
404 seen against several series, including the already-shipped
`PET.RWTC.M` spot series). Re-tested multiple times through the actual
`EIAClient` code before acting on it: the route responds normally and
consistently (`PET.RWTC.M` → 486 rows through 2026-06; `PET.RCLC1.D` → 5000
rows, still ending 2024-04-05). The original 404 was a transient EIA-side
or network hiccup at that moment, not a real deprecation — `sources/eia.py`
needs no fix. Retracted here so this doc doesn't carry a false claim; the
discontinuation finding above is unaffected (reconfirmed a second way, via
the legacy route this time, not just the modern one).

**Build (if pursued).**
1. **Sourcing, now that EIA's free path is confirmed dead**: CME settlement
   data (previously reviewed and rejected for Fed Funds futures on cost
   grounds in `terminal_phase0_gaps.md` — re-verify the rejection still
   holds rather than assuming, since terms/pricing can change) or a
   commercial vendor (Quandl/Nasdaq Data Link has some free continuous-
   contract series that might be sufficient for a first pass). Both are
   the same kind of paid-vendor decision as item 1b — **worth deciding
   together** rather than running two separate vendor evaluations.
2. Gold layer: `gold.futures_curve` (grain: `commodity · as_of_date ·
   contract_month · settle_price · days_to_expiry`) plus a derived
   `gold.futures_term_structure` (front-month vs. 2nd/3rd month spread —
   the standard contango/backwardation signal).
3. Given cost uncertainty, start with **one commodity** (WTI, since
   `DCOILWTICO` already establishes it's a series of interest) end-to-end
   before generalizing the manifest/config shape to others.

**Verdict:** the "check EIA first, might be nearly free" hypothesis is
**falsified** — EIA had this exact data and killed it in April 2024. This
item is now squarely in the same bucket as 1b: gated on a paid-vendor
decision, best revisited once
`docs/handoffs/governance_and_access_control.md` item 1 (licensing
register) exists so the two vendor decisions (options data + futures data)
can be made together instead of separately.

## Summary

| # | Item | Real gap? | Sourcing cost | Priority |
|---|---|---|---|---|
| — | FX | **No — already 24 pairs, excluded from scope** | — | — |
| — | Credit spread indices | **No — 12 ICE BofA series already cover this** | — | — |
| 1a | Realized volatility (derived from existing prices) | Yes | Free (no new source) | **DONE** |
| 1b | Options chains / implied-vol surface | Yes | Paid, licensing-gated | High value, but coupled to Path A |
| 2 | Individual bond/CDS/muni pricing | Aggregate half closed via `discover`; individual-issuer half **descoped, not pursuing** | Free (aggregate) | **DONE / CLOSED** |
| 3 | Commodities futures/term structure | Yes | EIA's free path confirmed dead (discontinued 2024-04-05) — paid vendor only | Coupled to Path A, same as 1b |

Recommended order: **1a (realized vol) — done. Item 2 — done/closed**
(aggregate expansion built; individual-issuer pricing explicitly descoped).
Remaining: items 1b and 3 both hinge on a vendor/licensing decision, so
they're natural candidates to revisit once
`docs/handoffs/governance_and_access_control.md` item 1 (the licensing
register) exists — at which point onboarding a paid source is a documented
decision instead of an ad hoc one.
