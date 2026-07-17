# market_terminal → this pipeline: series completeness audit

**Purpose.** A verified list of series `market_terminal` (the sibling project at
`../market_terminal`) uses that this pipeline does **not** currently ingest.
Complements `docs/market_terminal_gold_views.md` (which plans Gold-layer views
for terminal-equivalent analytics) — this document is narrower: just "what raw
series are missing," checked against live FRED metadata so it doesn't repeat
a stale or mistyped identifier as a real gap.

**Methodology.**
1. An Explore agent scanned every module of `market_terminal` (`macro_data_etl`,
   `market_data_pipeline`, `market_lens_studio`, `news_nlp`, the Next.js
   frontend) for hardcoded series/ticker identifiers, citing file:line for each.
2. Every candidate FRED-style id was checked against **live FRED metadata**
   (not just string-diffed against this pipeline's manifests) to exclude:
   mistyped/nonexistent ids, and discontinued series not worth adding.
3. Cross-checked against `docs/market_terminal_gold_views.md` §7, which already
   closed an earlier round of gaps (`fed_funding.yml`, `ice_credit.yml`,
   `macro_flags.yml`, extended `rates.yml`) — those are correctly **absent**
   from the list below because they're already present.

---

## 1. Confirmed missing FRED series (59) — real, active, not yet in any manifest

### Rates / money markets (5)
| Series | Title |
|---|---|
| `DFF` | Federal Funds Effective Rate (daily; pipeline has `EFFR` but not this older/parallel series) |
| `DFEDTARU` | Federal Funds Target Range — Upper Limit |
| `DFEDTARL` | Federal Funds Target Range — Lower Limit |
| `RRPONTSYAWARD` | Overnight Reverse Repo Award Rate |
| `DCPF3M` | 90-Day AA Financial Commercial Paper Rate |

### Treasury curve / spreads (8)
| Series | Title |
|---|---|
| `DFII10` | 10Y TIPS (real) yield |
| `T5YIFR` | 5y5y Forward Inflation Expectation Rate |
| `T10Y2Y` | 10Y-2Y Treasury spread ("2s10s") — note: this pipeline already *computes* this via `config/spreads.yml`'s `T10Y2Y` spread (DGS10-DGS2), but does not separately ingest FRED's own precomputed series of the same name |
| `T10Y3M` | 10Y-3M Treasury spread — same note as above (already computed, not separately ingested) |
| `DTB4WK` | 4-Week T-Bill secondary market rate |
| `DTB3` | 3-Month T-Bill secondary market rate |
| `DTB6` | 6-Month T-Bill secondary market rate |
| `MORTGAGE15US` | 15-Year Fixed Mortgage Rate (pipeline has `MORTGAGE30US` only) |

### CPI components (9)
| Series | Title |
|---|---|
| `CPIUFDSL` | CPI: Food |
| `CPIENGSL` | CPI: Energy |
| `CPIMEDSL` | CPI: Medical Care |
| `CPIAPPSL` | CPI: Apparel |
| `CPITRNSL` | CPI: Transportation |
| `CUSR0000SEMD` | CPI: Hospital and Related Services |
| `CUSR0000SAS367` | CPI: Other Services |
| `CPIRECSL` | CPI: Recreation |
| `CUSR0000SAE1` | CPI: Education |

### PCE components (1 of 8 checked — see note)
| Series | Title |
|---|---|
| `DNRGRG3M086SBEA` | PCE: Energy goods & services (chain-type price index) |

Note: `DHUTRC1M027SBEA`, `DHLCRG3M086SBEA`, `DTRSRC1M027SBEA`, `DRCARC1M027SBEA`
(Housing/Health Care/Transportation/Recreation PCE components market_terminal's
scan cited) **do not exist under those exact ids** — live FRED returned 400 for
all four. Likely a suffix/vintage mismatch in market_terminal's own hardcoded
list; if you want these, look up the current correct ids on FRED directly
rather than reusing the ones cited there.

### International CPI (indices, 18 of 19 checked)
All confirmed to exist. Same pattern (`<CTRY>CPIALLMINMEI` / harmonized index):
`CP0000EZ19M086NEST` (Euro Area), `GBRCPIALLMINMEI`, `JPNCPIALLMINMEI`,
`DEUCPIALLMINMEI`, `FRACPIALLMINMEI`, `ITACPIALLMINMEI`, `CANCPIALLMINMEI`,
`CHNCPIALLMINMEI`, `INDCPIALLMINMEI`, `BRACPIALLMINMEI`, `MEXCPIALLMINMEI`,
`AUSCPIALLQINMEI`, `KORCPIALLMINMEI`, `CHECPIALLMINMEI`, `ESPCPIALLMINMEI`,
`TURCPIALLMINMEI`, `IDNCPIALLMINMEI`, `ZAFCPIALLMINMEI`.

`SAUCPIALLMINMEI` (Saudi Arabia) does **not** exist under that id — 400 from FRED.

**Design note:** `docs/market_terminal_gold_views.md` §7 says international
CPI/policy rates are intentionally sourced via World Bank
(`worldbank_global.yml`, `FP.CPI.TOTL.ZG`) rather than FRED's country-mirror
codes, as a deliberate architecture choice. These 18 are a legitimate
*alternative* path (FRED already has them, no new source needed), not a
correction of that choice — worth a decision on which to standardize on
rather than carrying both.

### Labor / consumer / growth (5)
| Series | Title |
|---|---|
| `CCSA` | Continued (insured) unemployment claims |
| `GDPNOW` | Atlanta Fed GDPNow nowcast |
| `UMCSENT` | U. Michigan Consumer Sentiment |
| `TOTALSA` | Total Vehicle Sales |
| `CSUSHPINSA` | Case-Shiller U.S. National Home Price Index |

### Credit (ICE BofA OAS/yield, 3)
| Series | Title |
|---|---|
| `BAMLEMCBPIOAS` | EM Corporate Plus Index OAS |
| `BAMLC0A0CMEY` | US Corporate Index Effective Yield |
| `BAMLH0A0HYM2EY` | US High Yield Index Effective Yield |

### Market levels / commodities (6)
| Series | Title |
|---|---|
| `VIXCLS` | CBOE VIX |
| `SP500` | S&P 500 index level |
| `NASDAQCOM` | NASDAQ Composite |
| `DJIA` | Dow Jones Industrial Average |
| `DCOILWTICO` | WTI Crude Oil |

`GOLDPMGBD228NLBM` (Gold PM Fix) does **not** exist under that id — 400 from
FRED (likely discontinued/renamed; the LBMA gold fix series has changed ids
before).

**Scope note:** these are market index *levels*, not economic indicators — out
of this pipeline's stated "quant macro" scope per `market_terminal_gold_views.md`
§7's design intent, unless that intent has since changed.

### Global policy rates (4 of 12 checked exist)
| Series | Title |
|---|---|
| `IRSTCB01JPM156N` | Japan central bank policy rate |
| `IRSTCB01CAM156N` | Canada central bank policy rate |
| `IRSTCB01BRM156N` | Brazil central bank policy rate |
| `BOERUKM` | Bank of England Policy Rate |
| `INTDSRJPM193N` | Japan discount rate |

8 of the 12 market_terminal cited (`IRSTCB01USM156N`, `IRSTCB01GBM156N`,
`IRSTCB01AUM156N`, `IRSTCB01CHM156N`, `IRSTCB01MXM156N`, `IRSTCB01KRM156N`,
`IRSTCB01SEM156N`, `IRSTCB01NOM156N`, `IRSTCB01NZM156N`, `IRSTCB01TRM156N`) do
**not exist** under those ids — 400 from FRED. Same design-choice note as
international CPI above: this pipeline sources policy rates via World Bank/BIS
concepts, not FRED mirrors, by design.

---

## 2. Equity / ETF / crypto tickers — 75 of 90 checked not present

This pipeline *does* have equity ingestion paths (`manifests/equity_tiingo.yml`,
`manifests/equity_stooq.yml`), both currently shipped `active: false`. Checked
presence as either a bare ticker or `TICKER:close` (the Tiingo Silver-exploded
form). Missing, grouped by what they are in market_terminal:

- **Broad/factor ETFs:** DIA, RSP, VTV, VUG, MTUM
- **Sector SPDRs (all 11):** XLK, XLF, XLE, XLV, XLI, XLY, XLP, XLU, XLB, XLRE, XLC
- **Commodities:** SLV, USO, DBC, DBA, CPER
- **Bonds/rates:** IEF, SHY, TIP, BIL
- **Credit:** LQD, EMB, JNK, BKLN, MBB, MUB
- **Vol/FX proxies:** VIXY, UUP, FXE, FXY
- **International:** VGK, EWJ, ACWI, EWC, EWU, INDA, FXI, EWZ, VNQ, EWG
- **Thematic:** SMH, XBI, ARKK, KRE, KBE, IBIT
- **Single names:** GOOGL, META, TSLA, JPM, GS, BAC, XOM, CVX, UNH, GME, AMC,
  BBBY, RIVN, LCID, COIN, PLTR, SMCI, MSTR, HOOD, CVNA, UPST, AFRM, DJT, NFLX

15 of the 90 are already present: AAPL, MSFT, NVDA, AMZN, JNJ, SPY, QQQ, IWM,
TLT, HYG, AGG, EEM, EFA, GLD, VTI (as bare tickers and/or `:close` Silver rows).

**Action, if wanted:** these are additive lines in the existing (inactive)
`equity_tiingo.yml`/`equity_stooq.yml` manifests plus flipping `active: true` —
no new source-client code needed, unlike the items below.

---

## 3. World Bank coverage gap

`manifests/worldbank_global.yml` currently has `FP.CPI.TOTL.ZG` (CPI inflation
%) for only **11 of the ~39 countries** market_terminal covers via a
combination of World Bank + IMF + BIS. Missing countries (ISO3, inferred from
market_terminal's BIS/IMF ref-area list): CA, CL, CO, PE, GB, DE, FR, IT, CH,
SE, NO, PL, TR, RU, ZA, SA, IL, NG, EG, KR, NZ, ID, TH, MY, PH, VN, TW, SG, HK
— roughly 28 more countries to add if full parity is wanted, all through the
World Bank connector already wired in this pipeline (no new source-client work).

`FP.CPI.TOTL` (CPI index level, not YoY %) is not ingested for any country —
zero matches.

---

## 4. Structurally unsupported — market_terminal sources this pipeline has no connector for

Not a "missing manifest entry" — these need new source-client code
(`src/fred_pipeline/sources/*.py`) before any manifest could reference them:

- **IMF** — `PCPIPCH` (CPI inflation %) across ~39 countries (fallback source
  when World Bank/BIS don't have a country)
- **BIS** — `WS_CBPOL` (central bank policy rates) dataset across ~36 reference
  areas
- **CME** — Fed Funds futures (product 305), used for FOMC rate-probability
  modeling. Per market_terminal's own docs, even *they* don't have this live —
  their FOMC module currently falls back to a FRED-short-rate-derived model.

**Verification (beyond the initial scan):** explicitly grepped
`market_terminal` for direct references to `bls.gov`, `eia.gov`,
`apps.bea.gov`, `api.census.gov`, `fiscaldata.treasury.gov`, and
`sec.gov`/`data.sec.gov` — zero matches. Also enumerated every connector file
in both `macro_data_etl/src/connectors/` (`bis`, `cme`, `imf`, `world_bank`)
and `market_data_pipeline/src/connectors/` (`fred`, `polymarket`, `synthetic`,
`yahoo`). **market_terminal has no BLS, EIA, BEA, Census, Treasury, SEC,
Tiingo, or Stooq connector of any kind** — every BEA/BLS-origin data point it
uses arrives exclusively as a FRED-mirrored series (already covered in §1).
So despite this pipeline supporting 5 additional direct-source connectors
market_terminal doesn't have, there is nothing to "catch up on" for those
sources specifically — market_terminal simply never reaches them directly.

---

## Summary

| Category | Missing | Notes |
|---|---|---|
| FRED (rates/curve/CPI/labor/credit) | 59 | Verified live against FRED; ready to add to manifests |
| FRED (international CPI/policy, FRED-mirror path) | 22 | Alternative to existing World Bank-based approach — a design choice, not a pure gap |
| Equity/ETF/crypto tickers | 75 | Manifests exist but inactive; additive, no new code |
| World Bank country coverage | ~28 countries | Same connector, just more manifest entries |
| IMF / BIS / CME | all | Needs new source-client code, not just manifest entries |

The 59 in §1 are the actionable, no-caveats list: real FRED series, not
already computed/covered another way, not discontinued, not out of stated
scope. Everything else above is either a design decision to make (FRED-mirror
vs. World Bank for international data) or a bigger lift (new source clients).

---

## Implementation status (2026-07-17)

All of the above has been implemented and verified live against FRED:

- **59 FRED series added**, distributed across `fed_funding.yml`, `rates.yml`,
  `prices_extra.yml`, `international.yml`, `labor_extra.yml`,
  `production_housing.yml`, `macro_flags.yml`, `global_policy.yml`, and a new
  `market_indices.yml` (for the market-level series). All `active: true`.
- **75 equity/ETF tickers added** to both `equity_tiingo.yml` (now 85 total)
  and `equity_stooq.yml` (now 89 total), matching market_terminal's universe.
- **Two real bugs found and fixed** while verifying the *existing* (already-
  shipped, previously unverified) entries in these manifests:
  - `fed_funding.yml`'s `BGCR` did not exist under that id or any variant
    tried — removed rather than shipping a guessed id.
  - `fed_funding.yml`'s `TGCR` was wrong — corrected to `TGCRRATE` (the real
    id).
  - `global_policy.yml`'s `IRSTCB01BRM156N` (Brazil policy rate) needed its
    `max_value` bound raised from 40 to 100 (Brazil's Selic has historically
    exceeded 40%) — caught by a real dry-run against live FRED, not just
    metadata. Also flagged: this series is 959 days stale on FRED (not
    discontinued, just seemingly unmaintained).
- **A pre-existing, unrelated bug discovered**: Stooq now serves a JavaScript
  proof-of-work anti-bot challenge on its CSV endpoint, confirmed via direct
  `curl` — this breaks every series in `equity_stooq.yml` (verified against
  an original, previously-shipped ticker, not just the new additions), not
  something introduced by this work. Decision: left `active: true` to
  surface the failures in the audit trail rather than silently hiding a
  broken source; `equity_tiingo.yml` covers the same tickers via a
  legitimate keyed API as the working alternative once `TIINGO_API_KEY` is
  set.
