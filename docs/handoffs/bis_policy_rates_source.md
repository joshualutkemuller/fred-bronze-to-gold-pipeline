# BIS policy rates → `gold.global_policy_rates`

**Status: COMPLETE (pipeline) — terminal integration in progress.**
**Written**: 2026-07-30, from `market_terminal` while diagnosing why the
terminal's macro ETL (`macro_data_etl`, a sibling repo) is still load-bearing
after the Gold DB cutover.
**Updated**: 2026-07-31 — BIS connector ported and running live; `gold.global_policy_rates` now populated with 28+ countries. Terminal side wired to `/api/econ/global-policy-rates`.
See `market_terminal/docs/gaps/SNAPSHOT_FIXTURE_GAPS.md` §G11 for the terminal-side context this closes.

## The gap, confirmed by direct inspection

`gold.global_policy_rates` (GPOL in the terminal) covers **2 countries** —
United States and Euro Area — because `config/global_series.yml`'s
`policy_rates:` list has exactly two entries, both sourced from FRED
(`FEDFUNDS`, `ECBDFR`).

The terminal's `macro_data_etl` sibling repo covers **30 countries** for the
same concept, via a BIS (Bank for International Settlements) connector already
written and working: `macro_data_etl/src/connectors/bis.py`, hitting BIS's
`WS_CBPOL` (Central Bank Policy Rates) dataset. `macro_data_etl` has not been
run since 2026-06-24 and is not wired to any live consumer in the terminal
anymore — it is a frozen JSON export the terminal reads as a last-resort
fallback. This plan ports the *connector*, not the frozen output, into this
pipeline as a proper Bronze→Silver→Gold source.

### Why FRED can't close this gap

Already checked, so this doesn't get re-attempted: `manifests/global_policy.yml`
carries five FRED ids beyond the two currently active, intended to widen this
exact table. Checked each against Gold's own `fred_latest_observation`:

| FRED series | Country | Latest observation | Usable |
|---|---|---|---|
| `ECBMRRFR` | Euro Area | 2026-07-17 | live, but duplicates existing `ECBDFR` coverage |
| `IRSTCB01JPM156N` | Japan | 2023-12-01 | discontinued |
| `IRSTCB01CAM156N` | Canada | 2023-12-01 | discontinued |
| `IRSTCB01BRM156N` | Brazil | 2023-12-01 | discontinued |
| `BOERUKM` | United Kingdom | 2017-01-01 | discontinued |
| `INTDSRJPM193N` | Japan | 2017-04-01 | discontinued |

The manifest's header comment — "Verified against live FRED (2026-07-17): all
ids below resolve" — is true but misleading: it verifies the *endpoint
resolves*, not that the series is still updated. Worth fixing that comment
while touching this file, so the next person doesn't repeat the check. The
FRED OECD `IRSTCB01*` policy-rate family was broadly discontinued at end-2023;
the manifest already lists ten more ids (US, GB, AU, CH, MX, KR, SE, NO, NZ,
TR duplicates) that 400 outright and should not be added either.

**BIS is the only path to real coverage here.**

## What already exists (in `macro_data_etl`, this repo's sibling)

- **Connector**: `macro_data_etl/src/connectors/bis.py` — `BISConnector`,
  ~180 lines. Hits `https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/{freq}.{ref_area}?startPeriod={start}`,
  keyless, returns CSV (`Accept: application/vnd.sdmx.data+csv;version=1.0.0`),
  parsed with Polars. `freq=M` (monthly). Has retry-on-5xx via `tenacity`, a
  per-request rate-limit sleep, and per-`ref_area` error isolation (`fetch_all_rates`
  logs and skips a failing country rather than aborting the batch).
- **Country list**: `macro_data_etl/config/series_catalog.yaml` → `countries[].policy_rate.bis_ref_area`,
  38 countries with ISO-2 BIS ref areas mapped to ISO-3. Euro-area members
  (Germany, France, Italy, Spain) all map to `XM`, matching BIS's convention of
  one series for the currency union — copy this pattern, don't invent per-country
  Euro rows.
- **What it does NOT have**: any Bronze archival, Silver normalization, DQ
  checks, vintage tracking, or a manifest-driven `active: true/false` toggle —
  `macro_data_etl` is a standalone script-style ETL, not built on this
  pipeline's `SourceClient` contract. None of its plumbing is reusable as-is;
  only the *connector logic and the country list* are.

## The pattern to follow: `worldbank.py` is the precedent

This pipeline already has a non-FRED, keyless, "series_id encodes more than
one dimension" source: `src/fred_pipeline/sources/worldbank.py` (208 lines),
registered in `src/fred_pipeline/pipeline.py`. BIS should be built the same
way — same shape, same registration points, same manifest convention. Concretely:

### 1. `src/fred_pipeline/sources/bis.py` — new `SourceClient`

Implement the `SourceClient` protocol (`src/fred_pipeline/sources/base.py:73`):
`get_observations(series_id, **kwargs) -> raw payload` (archived verbatim to
Bronze) and `normalize(series_id, payload, ...) -> list[silver rows]`
(`SILVER_COLUMNS` shape — copy `normalize_worldbank_observations` in
`worldbank.py:76-113` as the template: `source`, `series_id`,
`observation_date`, `realtime_start`/`realtime_end` blank (BIS carries no
vintages, like World Bank/BLS/EIA), `value`, `raw_value`, `is_missing`,
`row_hash`, `ingested_at`, `run_id`).

Subclass `HTTPSource` (`sources/base.py:97`) like `WorldBankClient` does, not
`BISConnector`/`FallbackHTTPClient` from `macro_data_etl` — that gives you the
shared retry/rate-limit transport for free and keeps one HTTP stack across all
sources.

`series_id` convention: follow World Bank's `<dim>:<dim>` pattern
(`worldbank.py:36-44`, `_parse_series_id`) — encode `<ref_area>` alone is
enough here since BIS's dataflow is single-purpose (`WS_CBPOL`), e.g.
`series_id = "BIS:US"`, `"BIS:XM"`, `"BIS:JP"`. Don't reuse a bare ISO-2 code
as `series_id` — it'll collide with nothing today but is fragile if a second
BIS dataflow (e.g. exchange rates) gets added later.

`observations_endpoint(series_id)` returns the BIS path for Bronze lineage,
same as `WorldBankClient.observations_endpoint` (`worldbank.py:145-148`).

### 2. `config/bis.yml` (or extend `manifests/global_policy.yml`) — the country list

Port `series_catalog.yaml`'s `countries[].policy_rate.bis_ref_area` mapping.
Suggested manifest shape, following `manifests/worldbank_global.yml`'s
convention exactly:

```yaml
name: bis_policy_rates
description: >
  Central bank policy rates from the BIS WS_CBPOL dataflow (source: bis).
  Keyless. series_id is 'BIS:<ref_area>' (ISO-2 BIS reference area; XM = Euro
  area). Ported from macro_data_etl/config/series_catalog.yaml
  (countries[].policy_rate.bis_ref_area), which covers 30 countries via
  src/connectors/bis.py — see docs/handoffs/bis_policy_rates_source.md.
version: 1

series:
  - series_id: "BIS:GB"
    title: Policy Rate (United Kingdom)
    category: rates
    frequency: m
    units: Percent
    active: true
    source: bis
    load_type: incremental
    expected_update_frequency: monthly
    vintage_enabled: false
    validation_profile: standard
    downstream_use_case: global_policy_rates
    priority: 2
    min_value: -2
    max_value: 25
    tags: [rates, policy, international, bis]
  # ... one entry per country in series_catalog.yaml's policy_rate.bis_ref_area,
  # skipping US (FEDFUNDS) and XM (ECBDFR) — already FRED-sourced and active.
```

Do not duplicate US/Euro Area here — `config/global_series.yml`'s existing
`FEDFUNDS`/`ECBDFR` entries stay; this manifest fills the other ~28 countries
only.

### 3. Register the source — three touch points in `src/fred_pipeline/pipeline.py`

Mirroring the World Bank wiring exactly:

- `_make_bis(config) -> SourceClient` factory (pattern at `pipeline.py:85-89`,
  `_make_worldbank`) — keyless, so no `SOURCE_KEY_REQUIREMENTS` entry needed.
- Add `"bis": _make_bis` to `SOURCE_FACTORIES` (`pipeline.py:178`).
- Add a `"bis"` entry to `_rate_limit_for_source`'s `defaults` dict
  (`pipeline.py:~250`) — BIS has no published rate limit; start conservative
  (`30`/min) and tune from observed 429s, matching how `worldbank: 60` and
  `tiingo: 10` were chosen empirically for this repo.
- Optionally add `"bis": min(8, total)` to `_extract_workers_for_source`
  (`pipeline.py:~290`, next to the `worldbank` case) — ~28 series is small
  enough this may not matter, but World Bank got a worker cap for the same
  reason (many small per-country requests).

### 4. `config/global_series.yml` — widen `policy_rates:`

Add one entry per newly-ingested country, `series_id` matching what step 1's
`normalize()` emits (`BIS:<ref_area>`):

```yaml
policy_rates:
  - {country: United States, iso3: USA, region: AMER, series_id: FEDFUNDS}
  - {country: Euro Area,     iso3: EMU, region: EMEA, series_id: ECBDFR}
  - {country: United Kingdom, iso3: GBR, region: EMEA, series_id: "BIS:GB"}
  - {country: Japan,         iso3: JPN, region: APAC, series_id: "BIS:JP"}
  # ... etc, one per manifest entry from step 2
```

No code change needed in `compute_global_policy_rates`
(`writer/global_views.py:95-156`) — it already reads `latest_rows` filtered by
`{d.series_id for d in cfg.policy_rates}` regardless of source, exactly the
same way it already ingests World Bank inflation series (`FP.CPI.TOTL.ZG`)
alongside FRED CPI. This is the whole reason the pipeline's config-driven
design makes this a plumbing task, not an analytics task — confirmed by
reading `global_views.py` end to end.

### 5. Country list — full port target

From `macro_data_etl/config/series_catalog.yaml` (38 countries; the terminal's
JSON export shows 30 actually populate — some catalog entries may 400 or emit
no rows, verify per-country when the connector runs against live BIS):

```
United States(US) Canada(CA) Mexico(MX) Brazil(BR) Argentina(AR) Chile(CL)
Colombia(CO) Peru(PE) United Kingdom(GB) Germany(XM) France(XM) Italy(XM)
Spain(XM) Switzerland(CH) Sweden(SE) Norway(NO) Poland(PL) Turkey(TR)
Russia(RU) South Africa(ZA) Saudi Arabia(SA) Israel(IL) Nigeria(NG) Egypt(EG)
Japan(JP) China(CN) India(IN) South Korea(KR) Australia(AU) New Zealand(NZ)
Indonesia(ID) Thailand(TH) Malaysia(MY) Philippines(PH) Vietnam(VN)
Taiwan(TW) Singapore(SG) Hong Kong(HK)
```

US and Euro-area (Germany/France/Italy/Spain all map to `XM`) are already
FRED-covered — exclude their `BIS:*` series_ids from the manifest to avoid two
Gold rows per print for one economy.

## Validation, before calling this done

1. **Row counts match the terminal's frozen export as a sanity floor.**
   `macro_data_etl`'s `policy_rate_timeseries.json` has 317 monthly rows across
   30 countries as of its 2026-06-24 freeze. A fresh BIS pull should meet or
   beat that per-country count (BIS keeps updating; the JSON is frozen).
2. **Spot-check 2–3 countries against a public source** (e.g. BOE Bank Rate,
   BoJ policy rate) for the most recent print — BIS aggregates central-bank
   submissions and the odd stale/misaligned print is possible.
3. **Confirm `real_rate_pct` in `compute_global_policy_rates`
   (`global_views.py:143-153`) still joins correctly** — it pairs a policy
   rate to the same `iso3`'s inflation print via `REAL_RATE_MAX_STALENESS_DAYS`
   (550 days, widened for annual World Bank CPI dated at period start).
   Newly-added BIS countries need a `gold.global_inflation` row for the same
   `iso3` to get a non-null real rate.
4. **Run the CI `powerbi_catalog` coverage assertion** if one exists for GPOL
   (see the migration handoff, `docs/handoffs/completed/market_terminal_gold_views.md`,
   §11 "Removal / enforcement checklist" pattern) — confirms the new manifest
   entries actually land in `gold.global_policy_rates`, not just Silver.

## Downstream: what unblocks in `market_terminal` once this ships

- `market_terminal/docs/gaps/SNAPSHOT_FIXTURE_GAPS.md` §G11's "policy rates —
  blocked" verdict flips to unblocked. The terminal's `economics/policy-rates`
  page can retire `etlPolicyRate()` / `src/data/etl/policy_rate_timeseries.json`
  and read `gold.global_policy_rates` directly (mirroring how `economics/global-cpi`
  will read `gold.global_inflation`).
- Once policy rates *and* the FOMC cutover (already unblocked — see §G11
  "FOMC — retire the ETL path today") both land, `macro_data_etl`'s remaining
  consumer in the terminal is inflation coverage (12 Gold countries vs. 37 in
  the frozen ETL — a separate, already-scoped decision, not blocked on this
  work). At that point `src/data/etl/` in the terminal repo can be deleted
  entirely, pending only the inflation country-list call.
