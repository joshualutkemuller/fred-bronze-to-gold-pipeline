# Validation Rules — Where They Live and What They Are

This is the review document for the handoff checklist's **Quant** items —
*"Approve validation rules"*, *"Identify vintage-sensitive series"*, and
*"Validate outputs"* (`handoff.md`). It has two purposes: point you at the
exact file/line where each rule is defined, and show what the rules
currently evaluate to across the live 2,312-series universe, so you can
review and approve (or ask for changes) without having to read the source
yourself.

Nothing here has been signed off by anyone but the pipeline's implementation
— treat every number below as a proposal, not an approved baseline.

---

## 1. Structural validation — manifest schema

**Where:** `manifests/manifest.schema.json`, enforced by
`fred_pipeline.manifest.SeriesSpec.__post_init__` (`src/fred_pipeline/manifest.py:99`)
and run via `fred_pipeline validate --manifests manifests`.

Every series entry must have `series_id`, `title`, `frequency` (required);
everything else has a default. Enforced at load time, before any FRED call:

| Field | Rule |
|---|---|
| `series_id` / `title` | non-empty string |
| `frequency` | one of `d, w, bw, m, q, sa, a` (or long form) |
| `load_type` | `full` or `incremental` |
| `validation_profile` | `strict`, `standard`, or `lenient` |
| `priority` | integer 1 (highest) – 5 (lowest) |
| `restate_records` | integer ≥ 1, or omitted |
| unknown fields | **rejected** — typos in a manifest fail loudly rather than being silently ignored |
| duplicate `series_id` | rejected, both within one manifest file and across all manifest files combined |

This is purely structural (is the manifest well-formed?), not economic — it
doesn't know or care whether `min_value: 0` makes sense for a given series.

---

## 2. Data-quality checks — run on every ingested batch

**Where:** `src/fred_pipeline/quality.py`. Each check is a pure function
over the normalized Silver rows for one series' one run; `run_quality_checks`
(`quality.py:245`) runs the applicable set and produces a `QualityReport`.
Results are persisted to `audit.data_quality_result` for every run — nothing
here is silent.

| Check | Severity | What fails it |
|---|---|---|
| `non_empty` | error | FRED returned zero observations |
| `no_duplicate_keys` | error | duplicate `(series_id, observation_date, realtime_start)` — would corrupt the Silver MERGE key |
| `dates_parseable` | error | an `observation_date` that isn't valid `YYYY-MM-DD` |
| `values_numeric` | error | a non-missing value that isn't a genuine float after parsing |
| `missing_ratio` | warning | more than **50%** of a run's observations are FRED's `.` (missing) sentinel |
| `no_future_dates` | warning | an observation dated after today |
| `value_bounds` *(only if the series sets `min_value`/`max_value`)* | error | a non-missing value outside the inclusive `[min_value, max_value]` range |
| `freshness` *(only if `frequency` is known)* | warning | latest observation older than the frequency allows (see table below) |

**Freshness thresholds** (`FREQUENCY_MAX_AGE_DAYS`, `manifest.py:43`) — how
many days old the latest observation can be before a series is flagged
stale. These are generic per-frequency defaults, not tuned per series:

| Frequency | Max age before stale |
|---|---|
| daily | 10 days |
| weekly | 21 days |
| biweekly | 30 days |
| monthly | 75 days |
| quarterly | 200 days |
| semiannual | 380 days |
| annual | 550 days |

### Validation profiles — how failures translate to pass/fail

**Where:** `QualityReport.passed` (`quality.py:225`), set per series via
`validation_profile` in the manifest.

| Profile | Behavior | Series count (of 2,312) |
|---|---|---|
| `strict` | **Any** failing check (including warnings) fails the series run | **7** |
| `standard` (default) | Only **error**-severity failures fail the run; warnings are recorded but don't block | **2,305** |
| `lenient` | Never fails on quality — advisory only. Runs only the two cheapest structural checks (`non_empty`, `dates_parseable`) | **0** |

The 7 `strict` series are the ones most likely to feed a headline number or
a backtest directly:

```
GDP, GDPC1        (GDP, real GDP)
CPIAUCSL, CPILFESL, PCEPI, PCEPILFE   (headline/core CPI & PCE)
PAYEMS            (nonfarm payrolls)
```

**Review question for you:** is `strict` on only these 7 the right call, or
should more of the 2,312 (e.g. the other rates/labor seed series) also be
`strict` rather than the `standard` default?

### Series with explicit value bounds

**Where:** `min_value` / `max_value` per series in the manifest, checked by
`check_value_bounds` (`quality.py:142`). Only **2** of 2,312 series set
these — every other series has no bounds check at all:

| Series | Bounds | Rationale (implicit, not documented anywhere else) |
|---|---|---|
| `UNRATE` | `[0, 100]` | unemployment rate is a percentage |
| `DGS1MO` | `[-5, 25]` | 1-month Treasury yield; allows for a brief negative-rate episode |

**Review question for you:** should other obviously-bounded series (other
rates, other percentage-of-population series in `labor_extra`, etc.) get
bounds too? The mechanism exists; it's just unused almost everywhere.

---

## 3. Vintage (point-in-time) tracking

**Where:** `vintage_enabled` per series in the manifest (default `true`,
`manifest.py:82`). Governs whether the pipeline requests/retains FRED's full
ALFRED revision history for a series, or just the single current value.

**Currently `false` for 21 of 2,312 series** — all either:
- **Treasury/market rates** (`DGS1MO` … `DGS30`, `FEDFUNDS`, `SOFR`,
  `T5YIE`, `T10YIE`): provably non-revised market-quoted series, so
  tracking vintages is a cheap no-op the pipeline's default philosophy
  disables to save storage/runtime — this was a design decision made before
  this session, not reviewed by you yet either.
- **9 housing sub-series** (`EXHOSLUSM495S`, `HOSINVUSM495N`,
  `HOSMEDUSM052N`, `HOSSUPUSM673N`, `EXSFHSUSM495S`, `HSFMEDUSM052N`,
  `EXHOSLUSM495N`, `HSFINVUSM495N`, `HSFSUPUSM673N`): these don't exist in
  ALFRED at all (FRED API confirmed this directly — they errored with *"does
  not exist in ALFRED"* under vintage tracking), so this isn't a judgment
  call, it's a hard API constraint.

**Review question for you:** every other series in the 2,312 — including all
2,285 newly discovered ones across money/banking, prices, labor,
production/housing, international, and national accounts — defaults to
`vintage_enabled: true`. That's the pipeline's deliberately conservative,
leak-free-by-default posture (see `manifest.py:77-81`), but it means every
one of those series pulls and stores full revision history whether or not it
actually ever gets revised. Worth a pass to flag more "obviously never
revised" series (e.g. other FX/rate quotes if any exist in the new domains)
the same way the original 10 rate series were.

---

## 4. Metadata drift & lifecycle reconciliation

**Where:** `src/fred_pipeline/reconcile.py`, run via `fred_pipeline reconcile`
(optionally `--fail-on-drift` to gate CI). Compares each manifest's declared
intent against FRED's live `/series` metadata — this runs independently of
data ingestion and doesn't touch Silver/Gold.

| Finding | Severity | Trigger |
|---|---|---|
| `frequency_mismatch` | error | FRED's reported frequency no longer matches the manifest's `frequency` |
| `discontinued` | warning | FRED's title contains the literal string `DISCONTINUED` |
| `units_changed` | info | FRED's `units` no longer matches the manifest's `units` (only compared when the manifest specifies units) |
| `not_found` | error | the series_id no longer resolves via the FRED API at all |

Separately, a **lifecycle snapshot** (`reconcile.py:102`) is recorded per
series per reconcile run: FRED's reported observation range, `last_updated`,
popularity, and a **staleness** verdict using the same
`FREQUENCY_MAX_AGE_DAYS` table as the freshness DQ check above, but computed
from FRED's `observation_end` rather than what we last ingested — i.e. "did
FRED actually publish on schedule," independent of whether our pipeline ran.

This has **not yet been run** against the full 2,312-series universe in this
session — it's a good next validation step (`fred_pipeline reconcile
--no-persist` is read-only and safe to run any time).

---

## 5. Gold-layer feature definitions (not a DQ check, but affects "is the
   output right")

**Where:** `config/spreads.yml`, loaded by
`fred_pipeline.spread_config.load_spread_defs`. Defines the cross-series
spreads/ratios in `gold.fred_curve_spread` — currently just the original 4
Treasury curve legs (`T10Y2Y`, `T10Y3M`, `T2Y3M`, `T30Y10Y`), all `op:
spread`. See `handoff.md`'s Gold Layer Feature Engineering Roadmap, item 3,
for what else could go here (real yields, credit spreads, etc.) — that's an
open item pending your sign-off on which pairs are meaningful.

The z-score in `gold.fred_feature_transforms` is an **expanding,
point-in-time-safe** calculation (mean/std from observations at-or-before
each row's date only) — this was a correctness fix made this session (it was
previously full-sample and leaked future data into historical rows); no
further review needed there unless you want to sanity-check actual values.

---

## Where to make changes

| To change... | Edit... |
|---|---|
| Which series load, their frequency/category/owners | `manifests/*.yml` |
| A series' validation strictness | `validation_profile:` in its manifest entry |
| A series' value bounds | `min_value:` / `max_value:` in its manifest entry |
| Whether a series tracks vintages | `vintage_enabled:` in its manifest entry |
| Freshness/staleness thresholds by frequency | `FREQUENCY_MAX_AGE_DAYS` in `manifest.py:43` (shared by DQ + reconcile) |
| The DQ checks themselves (add/remove/change logic) | `src/fred_pipeline/quality.py` |
| Drift/lifecycle rules | `src/fred_pipeline/reconcile.py` |
| Cross-series spreads/ratios | `config/spreads.yml` |

All of the above are plain YAML or well-isolated pure-Python functions
covered by the unit test suite (`tests/test_manifest.py`,
`tests/test_features.py`, `tests/test_gold_polars_parity.py`,
`tests/test_spark_integration.py`) — a change to a threshold or a manifest
field doesn't require touching pipeline orchestration code.
