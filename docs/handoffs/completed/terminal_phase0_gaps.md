# Pipeline build handoff: 3 gaps blocking terminal Phase 1

**Status: ALL 3 ITEMS COMPLETE** (merged to `main` via PR #29, 2026-07-18).
Item 2 was already built before this handoff was actioned; items 1 and 3 were
built, tested (both backends, including a live Spark+Delta integration test
for item 3), and verified against live FRED/Fed data. See the per-item
verdicts below for exactly what shipped and what live-data quirks were found
along the way.

**Audience:** an agent working in **this** repo (`fred-bronze-to-gold-pipeline`).
**Why:** the sibling `market_terminal` is cutting over to read macro/market data
**only** from this Gold layer (its plan: `../market_terminal/docs/GOLD_DB_MIGRATION_HANDOFF.md`).
Almost everything it needs already exists in Gold. **Three surfaces don't have a
clean Gold source yet.** This doc scopes each so the terminal can finish Phase 1
without keeping a live-API fallback for them.

Follow the repo's established pattern for every item: **config-driven YAML → one
pure-Python engine function (dict-in/dict-out, no Spark) → both backends
(`gold.py` Spark path + `LocalWarehouse` SQLite) → `sql/50_gold.sql`(+`60_views.sql`)
DDL → `docs/dictionary/data_dictionary.md` entry → a `gold.powerbi_catalog` row →
tests asserting both backends match.** New manifests ship `active: false` with the
`⚠️ VERIFY BEFORE ACTIVATING` header (egress is blocked in the build env).

Priority order below is also the recommended build order (ascending effort).

---

## 1. Economic release calendar — `gold.release_calendar`  *(smallest lift, no new source)*

**Terminal need.** The `CAL` module (`/api/econ/calendar`) renders a forward
stream of economic-release dates with importance + category, beat/miss vs
consensus, and desk-sensitivity tags. It currently calls FRED `/releases/dates`
live. Release *dates* are not an observation series, so nothing in Gold carries
them yet.

**Good news.** The FRED client already speaks to the releases endpoints — see
`src/fred_pipeline/sources/fred.py` (`get_release_series`, `list_series` on
`release/series`, and the vintage/`releases` machinery around line 203/315). You
are adding a **release-date fetch + a Gold table**, not a new source client.

**Build.**
1. Add a method to the FRED client to pull `/fred/releases/dates`
   (`include_release_dates_with_no_data=true`, `realtime_start=today`,
   `realtime_end=today+~120d`, `sort_order=asc`) → rows of
   `{release_id, release_name, date}`. Mirror the terminal's `fredReleaseDates`
   in `../market_terminal/src/lib/server/fred.ts` for the exact params.
2. Config `config/release_calendar.yml`: the curated release set the terminal
   shows (map `release_id` → `importance` HIGH/MEDIUM/LOW, `econ_category`, and a
   representative `series_id` so a release can join to its headline print). Keep
   it curated, not all ~300 FRED releases.
3. Engine `terminal_views.compute_release_calendar(release_dates, cfg)` →
   `gold.release_calendar`.

**Gold contract (grain: 1 / release × scheduled date):**
`release_id · release_name · release_date · importance · econ_category ·
representative_series_id · is_future · fetched_at`.
(Beat/miss + desk-sensitivity tags stay computed terminal-side from the joined
series print + its own config — do **not** try to encode desk semantics here.)

**Notes.** The calendar is forward-looking and changes daily; refresh it on every
pipeline run and stamp `fetched_at` so the terminal can show staleness. This is
the only Gold object that is intentionally *not* point-in-time (it's a schedule,
not a revised observation).

**Verdict for the terminal:** once shipped, `CAL` becomes a clean Tier-A
DB read. Until then it stays a flagged live exception.

**Status: DONE.** Shipped as designed: `FredClient.get_release_dates()`,
`config/release_calendar.yml` (curated), `terminal_views.
compute_release_calendar`, `Warehouse.write_release_calendar` on both
backends, wired into `FredPipeline.run()` (the one Gold table populated by a
live fetch rather than `build_gold()`, per the design note above). **Live-data
finding:** FRED release_id 101 ("FOMC Press Release") does not behave as a
discrete ~8x/year event in `/fred/release(s)/dates` — over a 120-day forward
window it degenerates into a placeholder for nearly every business day
(confirmed live; every other curated release returns clean, correctly-spaced
dates). Dropped from the curated list — FOMC coverage is already handled far
better by item 3's dedicated, live-verified Fed calendar.

---

## 2. PCE item-level inflation — BEA manifest (extends `gold.inflation_explorer`)  *(no new code)*

**Terminal need.** `INFL` has a CPI⇄PCE basket toggle and drills PCE to item
level. `gold.inflation_explorer` already ships the **CPI** item tree and **PCE
headline/core only** (`PCEPI`/`PCEPILFE`). The PCE *components* are the gap — the
pipeline's own §4.2 / open-question #1 flagged this as the deferred BEA follow-up.

**Good news.** The BEA source client already exists and is wired
(`src/fred_pipeline/sources/bea.py`, `BEAClient`; factory `_make_bea` and secret
`bea_api_key` in `src/fred_pipeline/pipeline.py:152/166`; silver handling in
`data/silver.py:69`). A BEA `series_id` is `dataset:table:line:frequency`
(e.g. `NIPA:T20404:1:M`). **This is a manifest + config change, not new code.**

**Build.**
1. New manifest `manifests/bea_pce_items.yml` (`source: bea`, `active: false`,
   verify header): PCE price-index components from **BEA Table 2.4.4** (price
   indexes) — the major groups the terminal shows (Goods: durable/nondurable;
   Services; and the sub-lines: housing, health care, transportation,
   recreation, food services, financial, energy goods & services). Encode each as
   `NIPA:T20404:<line>:M`. Look the line numbers up in the live BEA table; do not
   reuse the FRED `D*RG3M086SBEA` ids the terminal hardcoded — its own gap doc
   confirmed several of those 400 on FRED.
2. Extend `config/inflation_items.yml`: add the **PCE tree** parent/level
   hierarchy + relative-importance weights (BEA Table 2.4.5 nominal shares) so
   the existing `compute_inflation_explorer` engine emits `contribution_pp` and
   the waterfall for PCE exactly as it does for CPI. No engine change — the tree
   config is the only new input.

**Gold contract:** unchanged — the new PCE rows land in the existing
`gold.inflation_explorer` + `gold.inflation_contribution` with `basket = 'PCE'`.
The terminal's PCE toggle "just works" once rows appear.

**Notes.** BEA weights refresh annually; note the vintage in the config header
like the CPI weights already do. Requires `BEA_API_KEY` at ingest time.

**Verdict for the terminal:** ship `INFL` now with CPI-full + PCE-headline; the
PCE drill fills in the moment this manifest is activated + verified. No terminal
code change needed when it lands.

**Status: DONE** (was already built before this handoff was actioned).
`manifests/bea_pce_items.yml` and the PCE tree in `config/inflation_items.yml`
exist exactly as scoped, with `tests/test_pce_items.py` covering both.

---

## 3. FOMC rate probabilities — compute from Gold rate series  *(DECIDED: option A — no CME connector)*

**Terminal need.** The `FOMC` module renders CME-FedWatch-style meeting hike/cut
odds (Fed Funds futures → day-weighted FOMC probabilities), an implied path, and
a dot-plot overlay. In the terminal this is computed by the `macro_data_etl`
FedProbabilityEngine.

**Decision (locked 2026-07-17): DO NOT build a CME connector.** The
probabilities are *model output*, not a raw feed — derive them from FRED
short-rate/target series **already ingested** in this pipeline. See the cost
rationale below for why the CME path was rejected.

**Why not CME (the rejected path).** Building a real CME Fed Funds futures
connector was scoped and declined on cost/compliance grounds, not just effort:
- **Licensing.** CME Fed Funds futures (`ZQ`) price/settlement data is **not
  freely redistributable.** The FedWatch tool is free to *view*, but there is no
  free licensed API to rebuild a product on. Historical data (CME DataMine) is a
  paid per-dataset purchase; real-time/delayed feeds need a CME market-data
  agreement. Critically, the terminal is **display + redistribution use** (it
  shows CME-derived data to end users), which is CME's high, negotiated
  enterprise tier — not a single-seat subscription — and carries usage-reporting
  obligations. No fixed price; requires signing a CME agreement.
- **Engineering + ongoing compliance.** A new `sources/cme.py` client (auth,
  response shape, silver normalization, `_make_cme` + secret in `pipeline.py`,
  entitlement management in every runtime) plus permanent redistribution-reporting
  overhead — a recurring operational tax, not a one-time build.

Option A avoids **all** of the above: free FRED inputs, no license, no CME
agreement, deterministic, and it matches the terminal's own documented fallback
(its FOMC module already derives from FRED short rates when CME is unavailable).
Trade-off: a *model-implied* path, not actual futures-priced odds — acceptable
for a macro dashboard.

**Build (option A).** No new source client, no new manifest — **config + engine only.**
1. Confirm the inputs are already ingested (they are, per the gap-doc additions):
   FRED target range `DFEDTARU` / `DFEDTARL`, effective rate `EFFR` (and `DFF`),
   plus any OIS-implied-path proxy series the terminal used. If any is missing,
   it's a free-FRED manifest line, not a source client.
2. Port the `macro_data_etl` FedProbabilityEngine math into a pure-Python engine
   `terminal_views.compute_fomc_probability(rate_rows, meetings, cfg)` (dict-in/
   dict-out, no Spark) — day-weighted meeting probabilities from the current
   target midpoint + the implied path, over the scheduled FOMC meeting dates.
3. Config `config/fomc.yml`: the scheduled FOMC meeting dates (forward calendar),
   the bucket step (25 bps), and which input series feed the implied path.
4. Emit two Gold tables (both backends, DDL in `sql/50_gold.sql`, dictionary
   entry, `powerbi_catalog` rows, both-backends-match tests):

**Gold contract.**
`gold.fomc_probability` (grain: 1 / meeting × outcome-bucket):
`meeting_date · target_lower_bps · target_upper_bps · outcome_bps · probability ·
model_vintage · n_inputs`.
`gold.fomc_meeting_path` (grain: 1 / meeting): `meeting_date · implied_rate ·
implied_move_bps · cumulative_move_bps · model_vintage`.

**Notes.** Meeting dates are a forward schedule (like the release calendar,
item 1) — stamp `model_vintage` so the terminal shows staleness. Probabilities
per meeting should sum to ~1 across buckets; assert this in tests.

**Verdict for the terminal:** `FOMC` becomes a clean Gold read like REGIME —
a compute module, no live external feed, no CME dependency.

**Status: DONE.** Shipped as designed, with one resolved open question: the
"implied path" input (left unspecified above) is a **forward-rate bootstrap
off the short end of the Treasury curve** (`DGS1MO`/`DGS3MO`/`DGS6MO`/`DGS1`)
— `f` s.t. `(1+y1)^t1 · (1+f)^(t2-t1) = (1+y2)^t2` between consecutive
meeting horizons — chosen over simpler nearest-tenor bracketing because it
gives a genuinely distinct expected move for every meeting rather than
smearing the same yield across meetings 3+. The distribution ladder and
meeting-chaining loop are ported verbatim from `macro_data_etl/src/
analytics/fed_probability.py`; only the futures-price input is replaced.
Meeting dates (`config/fomc.yml`) verified live against
federalreserve.gov's published calendar through Dec 2027. Verified end-to-end
on **both** backends, including a real local Spark+Delta session (`tests/
test_spark_integration.py::test_fomc_tables_build_end_to_end_on_spark`) —
probabilities sum to ~1.0 for every meeting on both.

---

## Summary

| # | Item | Effort | New source client? | New manifest? | New Gold object | Blocks terminal | Status |
|---|---|---|---|---|---|---|---|
| 1 | Release calendar | S | no (FRED client extension) | no (config only) | `gold.release_calendar` | `CAL` | ✅ DONE |
| 2 | PCE item level | S–M | no (BEA already wired) | yes (`bea_pce_items.yml`) + config | *extends* `inflation_explorer`/`inflation_contribution` | `INFL` PCE drill | ✅ DONE |
| 3 | FOMC probabilities (option A) | M | no (compute from Gold rates) | no (config only) | `gold.fomc_probability` + `fomc_meeting_path` | `FOMC` | ✅ DONE |

**All three are complete and merged to `main` (PR #29, 2026-07-18).** Item 3
was locked to **option A** (compute from FRED short-rate/target series
already ingested via a Treasury-curve forward-rate bootstrap; **no CME
connector** — rejected on licensing/redistribution cost). None of the three
blocked the rest of terminal Phase 1 (ECON/rates/credit/funding/markets),
which read Gold objects that already existed. The terminal's `market_terminal`
repo can now cut over `CAL` and `FOMC` to Tier-A Gold reads, and activate the
PCE item-level drill in `INFL` once `manifests/bea_pce_items.yml` is verified
and flipped to `active: true`.
