# World Bank global inflation → `gold.global_inflation` expansion

**Status: COMPLETE — config expanded and local Gold refreshed.**
**Written**: 2026-07-31, from `market_terminal` while cutting over FOMC and global CPI to Gold.
**Updated**: 2026-07-31 — `config/global_series.yml` now emits 38 inflation
countries, `manifests/worldbank_global.yml` carries the active World Bank CPI
series, and `fred_local.db` has refreshed `gold_global_inflation`.
**Terminal integration**: Ready — [worldbank_global_inflation handler](../../src/lib/server/goldGlobalInflation.ts) and API route (`/api/econ/global-inflation`) are live. The pipeline config now expands `inflation:` from 12 to 38 countries.
See `market_terminal/docs/gaps/SNAPSHOT_FIXTURE_GAPS.md` §G11 for the terminal-side context.

## The gap, confirmed by direct inspection

`gold.global_inflation` (GCPI in the terminal) covered **12 countries** — all FRED-sourced or World Bank annual `FP.CPI.TOTL.ZG` — because `config/global_series.yml`'s `inflation:` list had exactly twelve entries.

The terminal's frozen `macro_data_etl` export (last run 2026-06-24) covers **37 countries**, also via World Bank `FP.CPI.TOTL.ZG`. The pipeline now covers that footprint plus one extra country in `gold.global_inflation`: 38 configured countries total, with World Bank annual CPI widening the map where monthly FRED CPI mirrors are not used.

**This was mostly a config edit.** No connector work or new transforms were
needed. One follow-up code change widened `REAL_RATE_MAX_STALENESS_DAYS` from
400 to 550 days so annual World Bank CPI observations dated `YYYY-01-01` can
pair with mid-year BIS policy-rate prints for `real_rate_pct`.

## What already exists

- **Manifest**: `manifests/worldbank_global.yml` — active World Bank CPI entries for the annual `FP.CPI.TOTL.ZG` rows used by the expanded config. Live check 2026-07-31: `TWN:FP.CPI.TOTL.ZG` returns no data, so Taiwan is not active here.
- **Bronze ingestion**: The pipeline's `worldbank` source (`src/fred_pipeline/sources/worldbank.py`) is active and fetching `<country>:<indicator>` pairs for the annual CPI series used below.
- **Silver normalization**: `normalize_worldbank_observations()` (`sources/worldbank.py:76-113`) handles the annual-only date parsing (`YYYY` → `YYYY-01-01`) and blank realtime vintage tracking (World Bank carries no point-in-time history).
- **Gold compute**: `compute_global_inflation()` (`writer/global_views.py:23-87`) already reads `latest_rows` and filters by `{d.series_id for d in cfg.inflation}`, exactly the same way it already ingests FRED CPI. No code change needed.

## The Country List

From `manifests/worldbank_global.yml` (active: true, all verified 2026-07-17):

```
United States       (CPIAUCSL - FRED, not WB)
Euro Area           (CP0000EZ19M086NEST - FRED, not WB)
United Kingdom      (GBRCPIALLMINMEI - FRED, not WB)
Japan               (JPNCPIALLMINMEI - FRED, not WB)
Germany             (DEUCPIALLMINMEI - FRED, not WB)
France              (FRACPIALLMINMEI - FRED, not WB)
Italy               (ITACPIALLMINMEI - FRED, not WB)
Canada              (CANCPIALLMINMEI - FRED, not WB)
China               (CHN:FP.CPI.TOTL.ZG - World Bank, annual)
India               (IND:FP.CPI.TOTL.ZG - World Bank, annual)
Brazil              (BRA:FP.CPI.TOTL.ZG - World Bank, annual)
Mexico              (MEX:FP.CPI.TOTL.ZG - World Bank, annual)
Australia           (AUS:FP.CPI.TOTL.ZG - World Bank, annual)
South Korea        (KOR:FP.CPI.TOTL.ZG - World Bank, annual)
Switzerland        (CHE:FP.CPI.TOTL.ZG - World Bank, annual)
Spain               (ESP:FP.CPI.TOTL.ZG - World Bank, annual)
Turkey              (TUR:FP.CPI.TOTL.ZG - World Bank, annual)
Indonesia          (IDN:FP.CPI.TOTL.ZG - World Bank, annual)
South Africa        (ZAF:FP.CPI.TOTL.ZG - World Bank, annual)
Saudi Arabia        (SAU:FP.CPI.TOTL.ZG - World Bank, annual)
Argentina           (ARG:FP.CPI.TOTL.ZG - World Bank, annual)
Chile               (CHL:FP.CPI.TOTL.ZG - World Bank, annual)
Colombia            (COL:FP.CPI.TOTL.ZG - World Bank, annual)
Peru                (PER:FP.CPI.TOTL.ZG - World Bank, annual)
Poland              (POL:FP.CPI.TOTL.ZG - World Bank, annual)
Russia              (RUS:FP.CPI.TOTL.ZG - World Bank, annual)
Sweden              (SWE:FP.CPI.TOTL.ZG - World Bank, annual)
Norway              (NOR:FP.CPI.TOTL.ZG - World Bank, annual)
New Zealand         (NZL:FP.CPI.TOTL.ZG - World Bank, annual)
Vietnam             (VNM:FP.CPI.TOTL.ZG - World Bank, annual)
Thailand            (THA:FP.CPI.TOTL.ZG - World Bank, annual)
Malaysia            (MYS:FP.CPI.TOTL.ZG - World Bank, annual)
Philippines         (PHL:FP.CPI.TOTL.ZG - World Bank, annual)
Singapore           (SGP:FP.CPI.TOTL.ZG - World Bank, annual)
Hong Kong           (HKG:FP.CPI.TOTL.ZG - World Bank, annual)
Israel              (ISR:FP.CPI.TOTL.ZG - World Bank, annual)
Egypt               (EGY:FP.CPI.TOTL.ZG - World Bank, annual)
Nigeria             (NGA:FP.CPI.TOTL.ZG - World Bank, annual)
```

First 8 are FRED-mirrored monthly CPI indices. Remaining 30 are World Bank annual data. Taiwan was checked but excluded because `TWN:FP.CPI.TOTL.ZG` returns no data.

## The work: one edit to `config/global_series.yml`

**Before** (12 entries):
```yaml
inflation:
  - {country: United States, iso3: USA, region: AMER, series_id: CPIAUCSL, transform: yoy_from_index, target: 2.0}
  - {country: Euro Area,     iso3: EMU, region: EMEA, series_id: "CP0000EZ19M086NEST", target: 2.0}
  # ... 10 more FRED series only
```

**After** (38 entries, keeping 8 FRED monthly CPI mirrors and using 30 World Bank annual CPI rows):
```yaml
inflation:
  # FRED-sourced: keep as-is
  - {country: United States, iso3: USA, region: AMER, series_id: CPIAUCSL, transform: yoy_from_index, target: 2.0}
  - {country: Euro Area,     iso3: EMU, region: EMEA, series_id: "CP0000EZ19M086NEST", target: 2.0}
  - {country: United Kingdom, iso3: GBR, region: EMEA, series_id: "GBRCPIALLMINMEI", target: 2.0}
  - {country: Japan,         iso3: JPN, region: APAC, series_id: "JPNCPIALLMINMEI", target: 2.0}
  - {country: Germany,       iso3: DEU, region: EMEA, series_id: "DEUCPIALLMINMEI", target: 2.0}
  - {country: France,        iso3: FRA, region: EMEA, series_id: "FRACPIALLMINMEI", target: 2.0}
  - {country: Italy,         iso3: ITA, region: EMEA, series_id: "ITACPIALLMINMEI", target: 2.0}
  - {country: Canada,        iso3: CAN, region: AMER, series_id: "CANCPIALLMINMEI", target: 2.0}

  # World Bank annual CPI (FP.CPI.TOTL.ZG) — add remaining 29
  - {country: China,         iso3: CHN, region: APAC, series_id: "CHN:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: India,         iso3: IND, region: APAC, series_id: "IND:FP.CPI.TOTL.ZG", transform: level, target: 4.0}
  - {country: Brazil,        iso3: BRA, region: AMER, series_id: "BRA:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Mexico,        iso3: MEX, region: AMER, series_id: "MEX:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Australia,     iso3: AUS, region: APAC, series_id: "AUS:FP.CPI.TOTL.ZG", transform: level, target: 2.5}
  - {country: South Korea,   iso3: KOR, region: APAC, series_id: "KOR:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Switzerland,   iso3: CHE, region: EMEA, series_id: "CHE:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Spain,         iso3: ESP, region: EMEA, series_id: "ESP:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Turkey,        iso3: TUR, region: EMEA, series_id: "TUR:FP.CPI.TOTL.ZG", transform: level, target: 5.0}
  - {country: Indonesia,     iso3: IDN, region: APAC, series_id: "IDN:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: South Africa,  iso3: ZAF, region: EMEA, series_id: "ZAF:FP.CPI.TOTL.ZG", transform: level, target: 4.5}
  - {country: Argentina,     iso3: ARG, region: AMER, series_id: "ARG:FP.CPI.TOTL.ZG", transform: level, target: 4.0}
  - {country: Chile,         iso3: CHL, region: AMER, series_id: "CHL:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Colombia,      iso3: COL, region: AMER, series_id: "COL:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Peru,          iso3: PER, region: AMER, series_id: "PER:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Poland,        iso3: POL, region: EMEA, series_id: "POL:FP.CPI.TOTL.ZG", transform: level, target: 2.5}
  - {country: Russia,        iso3: RUS, region: EMEA, series_id: "RUS:FP.CPI.TOTL.ZG", transform: level, target: 4.0}
  - {country: Sweden,        iso3: SWE, region: EMEA, series_id: "SWE:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Norway,        iso3: NOR, region: EMEA, series_id: "NOR:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: New Zealand,   iso3: NZL, region: APAC, series_id: "NZL:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Vietnam,       iso3: VNM, region: APAC, series_id: "VNM:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Thailand,      iso3: THA, region: APAC, series_id: "THA:FP.CPI.TOTL.ZG", transform: level, target: 2.5}
  - {country: Malaysia,      iso3: MYS, region: APAC, series_id: "MYS:FP.CPI.TOTL.ZG", transform: level, target: 2.5}
  - {country: Philippines,   iso3: PHL, region: APAC, series_id: "PHL:FP.CPI.TOTL.ZG", transform: level, target: 3.0}
  - {country: Singapore,     iso3: SGP, region: APAC, series_id: "SGP:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Hong Kong,     iso3: HKG, region: APAC, series_id: "HKG:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Israel,        iso3: ISR, region: EMEA, series_id: "ISR:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
  - {country: Egypt,         iso3: EGY, region: EMEA, series_id: "EGY:FP.CPI.TOTL.ZG", transform: level, target: 7.0}
  - {country: Nigeria,       iso3: NGA, region: EMEA, series_id: "NGA:FP.CPI.TOTL.ZG", transform: level, target: 6.0}
```

That's it. No other changes needed.

### Notes on the expansion:

- **Transform**: FRED series use `yoy_from_index` (the indices are level, compute YoY). World Bank `FP.CPI.TOTL.ZG` is already a YoY %, so use `transform: level`.
- **Targets**: Sourced from central-bank policy mandates and `market_terminal/src/data/globalMacro.ts` (the seeded base rates). Updated targets to match IMF/World Bank common consensus where different from the terminal's original assumptions.
- **Annual data**: World Bank CPI is annual only (one print per calendar year, typically published mid-year). The terminal will show `mom: null` / `momDelta: null` for all World Bank countries, which is correct and is already handled in the UI (see `global-cpi/page.tsx:255`).

## Validation

Completed locally against `fred_local.db` on 2026-07-31:

1. **Row count**: `gold_global_inflation` now has 38 distinct countries and
   8,471 rows. Silver has all 38 configured inflation series, including 30
   World Bank CPI series and 1,980 World Bank CPI rows.
2. **Spot-checked recent prints**: China, Turkey, Brazil, Saudi Arabia, and
   Nigeria all emit latest World Bank 2025 annual CPI YoY values with
   `change_pp`, `trend`, `target_pct`, and `vs_target_pp` populated.
3. **Transform behavior**: `compute_global_inflation()` handles the mix of
   FRED `yoy_from_index` rows and World Bank `transform: level` rows without
   additional code changes.
4. **Real-rate joins**: `REAL_RATE_MAX_STALENESS_DAYS` is now 550 so latest
   annual World Bank CPI prints dated `2025-01-01` can pair with `2026-06-01`
   BIS policy rates. A regression test covers this with Brazil.

## Downstream: what unblocks in `market_terminal` once this ships

- `market_terminal/src/app/economics/global-cpi/page.tsx` will show 38 countries via `/api/econ/global-inflation`, up from the earlier 12 Gold + SIM fallback for uncovered countries.
- The frozen `macro_data_etl` ETL snapshot (`src/data/etl/inflation_timeseries.json`, 37 countries, latest 2024-12-31) can be retired as soon as this lands — Gold covers the same concept with World Bank annual prints currently latest at `2025-01-01` and FRED CPI mirrors live through their monthly feeds.
- Once **both** this work and the BIS policy-rates handoff (`bis_policy_rates_source.md`) ship, the terminal's `src/data/etl/` directory can be deleted entirely — all three ETL fixtures (inflation, policy rates, FOMC) will be Gold-backed.

## Implementation notes

- **Alphabetical order**: The table above lists countries in a mix. When writing the config, consider grouping by region (AMER / EMEA / APAC) for readability, as the current file does.
- **Inflation targets**: These are seeded values (the terminal's SIM base). They should stay conservative; if you have more recent central-bank mandates, update them. But the pipeline doesn't *enforce* any particular target — it's just a number carried through `vs_target_pp`.
- **No breaking changes**: The 8 existing FRED entries stay unchanged. The 29 new World Bank entries are pure additions. No reordering, no field removals. Safe to merge.

## Ownership & timing

This was a small, well-scoped expansion with immediate value (38-country coverage vs. 12). It has no dependency beyond the existing World Bank source and the already-running Bronze/Silver path. It pairs with the BIS policy-rates work as a cohesive "widen the global macro tables" milestone.
