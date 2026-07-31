# World Bank global inflation → `gold.global_inflation` expansion

**Status: PROPOSED — ready to implement (pure config edit).**
**Written**: 2026-07-31, from `market_terminal` while cutting over FOMC and global CPI to Gold.
**Terminal integration**: Ready — [worldbank_global_inflation handler](../../src/lib/server/goldGlobalInflation.ts) and API route (`/api/econ/global-inflation`) are live. Just waiting for the pipeline config to expand `inflation:` list from 12 to 37 countries.
See `market_terminal/docs/gaps/SNAPSHOT_FIXTURE_GAPS.md` §G11 for the terminal-side context.

## The gap, confirmed by direct inspection

`gold.global_inflation` (GCPI in the terminal) covers **12 countries** — all FRED-sourced or World Bank annual `FP.CPI.TOTL.ZG` — because `config/global_series.yml`'s `inflation:` list has exactly twelve entries.

The terminal's frozen `macro_data_etl` export (last run 2026-06-24) covers **37 countries**, also via World Bank `FP.CPI.TOTL.ZG`. This country list is already declared in `manifests/worldbank_global.yml` and is live in the Bronze layer — meaning the World Bank series are being ingested, normalized to Silver, and available in `latest_by_observation`, but `compute_global_inflation()` never reads them because `config/global_series.yml`'s `inflation:` list is incomplete.

**This is purely a config edit.** No connector work, no new ingestion, no new transforms. The World Bank data is already here; it's just ungated.

## What already exists

- **Manifest**: `manifests/worldbank_global.yml` — 37+ entries, all marked `active: true`, all verified to resolve against World Bank's live API (as of 2026-07-17).
- **Bronze ingestion**: The pipeline's `worldbank` source (`src/fred_pipeline/sources/worldbank.py`) is already active and fetching `<country>:<indicator>` pairs, including all CPI series listed below.
- **Silver normalization**: `normalize_worldbank_observations()` (`sources/worldbank.py:76-113`) handles the annual-only date parsing (`YYYY` → `YYYY-01-01`) and blank realtime vintage tracking (World Bank carries no point-in-time history).
- **Gold compute**: `compute_global_inflation()` (`writer/global_views.py:23-87`) already reads `latest_rows` and filters by `{d.series_id for d in cfg.inflation}`, exactly the same way it already ingests FRED CPI. No code change needed.

## The country list — already verified, now gated

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
Taiwan              (TWN:FP.CPI.TOTL.ZG - World Bank, annual)
Singapore           (SGP:FP.CPI.TOTL.ZG - World Bank, annual)
Hong Kong           (HKG:FP.CPI.TOTL.ZG - World Bank, annual)
Israel              (ISR:FP.CPI.TOTL.ZG - World Bank, annual)
Egypt               (EGY:FP.CPI.TOTL.ZG - World Bank, annual)
Nigeria             (NGA:FP.CPI.TOTL.ZG - World Bank, annual)
```

First 8 are FRED-mirrored CPI indices (already in the config). Remaining ~30 are World Bank annual data.

## The work: one edit to `config/global_series.yml`

**Before** (12 entries):
```yaml
inflation:
  - {country: United States, iso3: USA, region: AMER, series_id: CPIAUCSL, transform: yoy_from_index, target: 2.0}
  - {country: Euro Area,     iso3: EMU, region: EMEA, series_id: "CP0000EZ19M086NEST", target: 2.0}
  # ... 10 more FRED series only
```

**After** (37 entries, keeping the 8 FRED ones + adding 29 World Bank):
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
  - {country: Taiwan,        iso3: TWN, region: APAC, series_id: "TWN:FP.CPI.TOTL.ZG", transform: level, target: 2.0}
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

## Validation, before calling this done

1. **Row count**: `gold_global_inflation` should grow from 12 country × 1 observation = 12 rows to ~37 countries × varying observation counts (World Bank annual since ~1960s, FRED monthly since 1990s/2000s depending on series). Run the pipeline and confirm `SELECT COUNT(*), COUNT(DISTINCT iso3) FROM gold_global_inflation` shows 37+ distinct countries.
2. **Spot-check recent prints**: pick 3–4 countries (e.g., China, Turkey, Brazil) and verify the latest YoY % is reasonable (cross-check against World Bank's website or IMF World Economic Outlook).
3. **Verify `compute_global_inflation()` still emits correct fields**: the new World Bank rows should have `cpi_yoy_pct`, `change_pp`, `trend`, `streak`, `target_pct`, `vs_target_pp`, all properly computed despite the `transform: level` difference.
4. **Confirm real-rate joins still work** (`compute_global_policy_rates`, `global_views.py:143–153`): the new World Bank inflation countries should join to their policy rates (if they exist in `policy_rates:` list) and emit non-null `real_rate_pct` where both are present.

## Downstream: what unblocks in `market_terminal` once this ships

- `market_terminal/src/app/economics/global-cpi/page.tsx` will show 37 countries via `/api/econ/global-inflation`, up from today's 12 Gold + 25 SIM fallback for uncovered countries.
- The frozen `macro_data_etl` ETL snapshot (`src/data/etl/inflation_timeseries.json`, 37 countries, latest 2024-12-31) can be retired as soon as this lands — Gold covers the same countries at fresher vintage (World Bank ~2026-06, FRED live).
- Once **both** this work and the BIS policy-rates handoff (`bis_policy_rates_source.md`) ship, the terminal's `src/data/etl/` directory can be deleted entirely — all three ETL fixtures (inflation, policy rates, FOMC) will be Gold-backed.

## Implementation notes

- **Alphabetical order**: The table above lists countries in a mix. When writing the config, consider grouping by region (AMER / EMEA / APAC) for readability, as the current file does.
- **Inflation targets**: These are seeded values (the terminal's SIM base). They should stay conservative; if you have more recent central-bank mandates, update them. But the pipeline doesn't *enforce* any particular target — it's just a number carried through `vs_target_pp`.
- **No breaking changes**: The 8 existing FRED entries stay unchanged. The 29 new World Bank entries are pure additions. No reordering, no field removals. Safe to merge.

## Ownership & timing

This is a small, well-scoped config edit with immediate value (37x coverage vs. 12). It has no dependencies beyond what's already running (World Bank is already ingested, already in Silver, already has a live source). Recommend pairing it with the BIS policy-rates work for a cohesive "widen the global macro tables" milestone in the next pipeline run.
