# Series discovery handoff: FRED coverage gaps worth filling

**Audience:** an agent working in **this** repo (`fred-bronze-to-gold-pipeline`).
**Why:** the series universe has grown organically (2,395 active FRED series
across 11 manifests) by expanding categories that were already touched —
rates, inflation, labor, production/housing, national accounts, money/banking,
international. Nobody has walked FRED's own top-level category tree end to
end to check for a category that's **entirely missing**, as opposed to
thin coverage within a category already started. This doc reports that walk
and scopes the one substantial gap it found, plus the tooling to close it —
no new code, no new source. `python -m fred_pipeline discover` (`src/
fred_pipeline/catalogs/discovery.py` + `cli.py`) already turns a FRED
category/release/search into a validated, deduped manifest; this is a
discovery-and-curation task, not a build task.

**Method.** Walked `GET /fred/category/children` from the root (`category_id=0`)
and cross-referenced each branch against `manifests/*.yml`'s `category` field
and a full-text grep for known series/survey names. FRED has exactly 8
top-level categories:

| FRED top-level category | id | Manifest coverage |
|---|---|---|
| Money, Banking, & Finance | 32991 | `money_banking.yml` (367), `rates.yml`, `fed_funding.yml` |
| Population, Employment, & Labor Markets | 10 | `labor.yml`, `labor_extra.yml` (356) |
| National Accounts | 32992 | `national_accounts_extra.yml` (332), `growth.yml` |
| Production & Business Activity | 1 | `production_housing.yml` (361) |
| Prices | 32455 | `prices_extra.yml` (800), `inflation.yml` |
| International Data | 32263 | `international.yml` (102), `worldbank_global.yml` (13) |
| **U.S. Regional Data** | **3008** | **none — 0 series** |
| Academic Data | 33060 | none (by design — see item 2) |

---

## 1. U.S. Regional Data (category 3008) — the real gap *(build this)*

**Finding.** Confirmed via the live category tree **and** a repo-wide grep
(no hits for `series_id: CAUR`/`NYUR`/etc., "Philadelphia Fed", "Empire
State", "Richmond Fed", "Dallas Fed", "Kansas City Fed", "Chicago Fed",
"state unemployment" anywhere in `manifests/`): this entire FRED branch is
untouched. It has 6 sub-branches:

| Sub-category | id | Size | What it is |
|---|---|---|---|
| Federal Reserve Districts | 32071 | 12 (one per district) | Regional Fed manufacturing/services surveys — **Philadelphia Fed Business Outlook, NY Fed Empire State, Richmond Fed, Dallas Fed, Kansas City Fed, Chicago Fed National Activity Index**, etc. |
| States | 27281 | 53 (50 states + DC + territories) | State-level unemployment, labor force, housing, income |
| Census Regions | 32043 | 4 (Midwest/Northeast/South/West) | Regional aggregates |
| BEA Regions | 32061 | 8 (Far West, Great Lakes, …) | Regional income/GDP aggregates |
| BLS Regions | 32849 | 7 (urban regions + population-size classes) | Regional CPI |
| Freddie Mac Regions | 32233 | 5 | Regional house-price indices |

**Why it matters.** The Fed-district manufacturing/services surveys
(Philly Fed, Empire State, Richmond, Dallas, KC Fed) are widely-watched,
market-moving leading indicators — exactly the kind of series a
macro/market terminal is expected to carry, and they're structurally
distinct from anything in `production_housing.yml` (those are national
aggregates; these are regional, higher-frequency sentiment surveys).
State-level series matter for anyone doing regional/cross-sectional
analysis (e.g., labor-market dispersion, regional housing divergence).

**Build (discovery + curation, no new code).**
1. **Priority 1 — Federal Reserve Districts (12 sub-categories under
   32071).** For each district id (32146–32156, 133), run:
   ```bash
   PYTHONPATH=src python -m fred_pipeline discover --name regional_fed_surveys \
       --category-id <district_id> --frequencies m --min-popularity 20 \
       --dry-run
   ```
   Review the preview, keep the headline diffusion-index series per district
   (each district publishes one flagship survey — Business Outlook,
   Empire State Manufacturing, etc. — plus sub-indices; start with the
   flagship, add sub-indices only if a clear downstream use emerges).
   Merge the kept ids into one `manifests/regional_fed_surveys.yml` (one
   file for all 12 districts — this branch is small enough that splitting
   by district would be over-fragmentation).
2. **Priority 2 — States (27281, 53 sub-categories).** This branch is
   large; do **not** blindly discover every series in every state category
   (thousands of series, mostly low-value: state-level CPI components,
   niche housing metrics). Scope to a **curated cross-state panel** of a
   handful of headline series types (state unemployment rate, state
   coincident index, state house price index) via `--search` per series
   type rather than `--category-id` per state, e.g.:
   ```bash
   PYTHONPATH=src python -m fred_pipeline discover --name state_unemployment \
       --search "unemployment rate" --frequencies m --min-popularity 10 \
       --dry-run
   ```
   then filter the preview to the 2-letter-state-suffixed ids (e.g.
   `CAUR`, `NYUR`, `TXUR`) and drop metro-area/county-level hits. Treat
   this as three separate small manifests (`state_unemployment.yml`,
   `state_coincident_index.yml`, `state_house_price.yml`) so each stays
   independently reviewable.
3. **Priority 3 — Census/BEA/BLS/Freddie Mac regional aggregates (4+8+7+5 =
   24 sub-categories, small each).** Same `--category-id` discovery per
   sub-category; these are few series each (regional composite indices),
   so a single `manifests/regional_aggregates.yml` covering all four
   families is reasonable.

**Ship inactive.** Per the repo's established convention (see
`manifests/bea_pce_items.yml`'s header for the template), new manifests
from this discovery should ship with `active: false` and a
`⚠️ VERIFY BEFORE ACTIVATING` header until a human has spot-checked the
discovered titles/ids/units against the live FRED page and confirmed
frequency/vintage settings — `discover`'s popularity-based priority and
frequency mapping are a starting point, not a substitute for review.

**Verdict:** highest-value, cleanly-scoped, zero new code. Priority 1
(regional Fed surveys) alone is worth doing even if priorities 2–3 are
deferred.

---

## 2. Academic Data (category 33060) — explicitly out of scope

**Finding.** 15 sub-categories, almost entirely historical/research
datasets (e.g. "Banking and Monetary Statistics, 1914–1941", "A Millennium
of Macroeconomic Data for the UK", "Penn World Table", "New England Textile
Industry, 1815–1860"). Not useful for a live macro/market pipeline.

**Exception worth a look:** *Economic Policy Uncertainty* (category 33201,
under Academic Data) publishes the well-known EPU index, which shows up
independently in FRED's regular release calendar (release_id 279, seen live
during the item-1 release-calendar build in `docs/handoffs/completed/
terminal_phase0_gaps.md`) — it may already be reachable as a mainline
series outside the Academic Data branch. Worth a `--search "economic policy
uncertainty"` discovery pass on its own; don't pull in the rest of Academic
Data.

**Verdict:** skip this branch except the EPU spot-check above.

---

## 3. Spot-check the 6 already-covered top-level categories

Not a gap in the same sense as item 1, but worth a lighter pass: each of
the 6 categories with existing manifests was expanded by hand/ad hoc over
many sessions, so there's no guarantee every *popular* series within them
was caught. A quick `discover --category-id <top-level-id> --min-popularity
60 --include-existing` (report-only via `--dry-run`, not a write) per
category, diffed against the existing manifest ids, would surface any
high-popularity series that were simply missed — lower priority than item 1
since these categories are already substantially built out, but cheap to
run.

---

## Summary

| # | Item | Effort | New source/code? | New manifest(s) | Priority |
|---|---|---|---|---|---|
| 1 | U.S. Regional Data (Fed district surveys, states, regional aggregates) | S–M (discovery + curation only) | no | `regional_fed_surveys.yml`, `state_*.yml` (x3), `regional_aggregates.yml` | **High** |
| 2 | Academic Data | — | — | none (skip; EPU spot-check only) | Low |
| 3 | Spot-check existing 6 categories for missed high-popularity series | S | no | possible small additions to existing manifests | Medium |

Everything here uses the existing `python -m fred_pipeline discover` CLI
and `discovery.py` engine — this is a curation task (review generated YAML,
set `vintage_enabled`/`validation_profile`, ship `active: false` pending
verification), not a new-feature build.
