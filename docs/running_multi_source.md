# Running the Pipeline with Multiple Sources (API Keys, Activation, Commands)

How to go from "I have an API key for EIA/BEA/BLS/…" to a fully built
Bronze → Silver → Gold database. Three steps: provide the keys, activate the
series, run. Nothing else — Gold (including the market-terminal analytical
views) rebuilds automatically at the end of every non-dry run; there is no
separate "build gold" command.

The pipeline only demands keys for sources that have **active** series in the
manifests: a FRED-only run never asks for an EIA key, and vice versa. If an
active source *is* missing its key, `fred-pipeline run` fails fast before
extracting anything, naming the exact environment variable to set.

---

## 1. Provide the API keys

Set environment variables (the recommended way — nothing lands in git):

| Source | Env var | Needed? | Where to get one |
|---|---|---|---|
| FRED | `FRED_API_KEY` | **Required** | https://fred.stlouisfed.org/docs/api/api_key.html |
| EIA | `EIA_API_KEY` | **Required** | https://www.eia.gov/opendata/register.php |
| BEA | `BEA_API_KEY` | **Required** | https://apps.bea.gov/api/signup/ |
| BLS | `BLS_API_KEY` | Optional — keyless works at a lower daily quota | https://data.bls.gov/registrationEngine/ |
| Census | `CENSUS_API_KEY` | Optional — keyless works | https://api.census.gov/data/key_signup.html |
| SEC | `SEC_USER_AGENT` | Not a key — set to your **contact email**; SEC requires an identifying User-Agent | — |
| US Treasury | — | Nothing needed (fully open) | — |
| World Bank | — | Nothing needed (fully open) | — |

```bash
export FRED_API_KEY="your-fred-key"
export EIA_API_KEY="your-eia-key"
export BEA_API_KEY="your-bea-key"
export SEC_USER_AGENT="you@example.com"
# optional quota bumps:
export BLS_API_KEY="your-bls-key"
export CENSUS_API_KEY="your-census-key"
```

Alternatively put them in a git-ignored `config/config.yaml`
(`cp config/config.example.yaml config/config.yaml`, then add e.g.
`eia_api_key: "..."` under `default:`). Precedence, highest wins:
**explicit CLI/arg → environment variable → config file → built-in default**
(and on Databricks, the FRED key additionally falls back to a secret scope).

## 2. Activate the series you want

**Every non-FRED manifest ships `active: false`** — plus some FRED additions
(`macro_flags.yml`, the `DGS3/DGS7/DGS20` curve tenors, `fed_funding.yml`,
`ice_credit.yml`) — because the series ids were assembled from documentation,
not verified against the live APIs. Each manifest's header says exactly where
to verify its ids.

For each series you want, confirm the id at the source, then flip:

```yaml
# manifests/eia_energy.yml
  - series_id: ELEC.PRICE.US-ALL.M
    ...
    active: true        # was false
```

Manifests you may want to activate:

| Manifest | Source | Contents |
|---|---|---|
| `bls_labor.yml`, `bls_cpi_basket.yml`, `bls_cpi_basket_sa.yml` | bls | unemployment; full CPI-U item basket (NSA + SA) |
| `eia_energy.yml` | eia | energy prices/volumes |
| `treasury_fiscal.yml` | treasury | debt/fiscal series |
| `worldbank_global.yml` | worldbank | global indicators |
| `bea_national_accounts.yml` | bea | NIPA tables |
| `census_indicators.yml` | census | economic indicators |
| `sec_financials.yml` | sec | company XBRL fundamentals |
| `macro_flags.yml` | fred | `USREC` — lights up every `is_recession` column |
| `fed_funding.yml` | fred | EFFR/IORB/OBFR/BGCR/TGCR/… — funding tape + stress gauge |
| `ice_credit.yml` | fred | ICE BofA OAS — credit-spread table |
| `rates.yml` (tail entries) | fred | DGS3/7/20 (full curve), DPRIME, MORTGAGE30US |

A wrong id is cheap: failures are **per-series isolated** — that one series
errors, everything else loads, and the run finishes `partial` with the error
recorded in `audit_etl_series_run`.

## 3. Run

```bash
pip install -e ".[local]"      # once; [local] adds polars for faster Gold builds

# 0) sanity-check the manifests (schema, duplicate ids, ...)
fred-pipeline validate --manifests manifests

# 1) DRY RUN — extract + data-quality checks in memory, nothing written.
#    The cheapest way to confirm a new key works and a series id is right:
fred-pipeline run --dry-run --series ELEC.PRICE.US-ALL.M

# 2) real local run: full Bronze → Silver → Gold into one SQLite file
fred-pipeline run --local --db-path fred_local.db
```

Useful flags for `fred-pipeline run`:

| Flag | Effect |
|---|---|
| `--series ID1,ID2` | run only those series (they must be `active`) |
| `--full` | force a full-history re-pull, ignoring the incremental restate watermark |
| `--dry-run` | extract + validate in memory; no writes, no Gold |
| `--local --db-path f.db` | persist to SQLite instead of Databricks/Delta |
| `--env dev\|test\|prod` | pick the environment / catalog |
| *(no `--local`)* | Databricks/Spark backend — run inside a workspace job |

Exit codes: `0` = succeeded or partial, `1` = failed, `2` = an active source
is missing its API key (the message names the env var).

## 4. Inspect the result

```bash
# per-source coverage & freshness (is anything stale/missing?)
sqlite3 fred_local.db "SELECT * FROM gold_v_source_coverage;"

# the ECON dashboard / curve lab / funding tape (market-terminal views)
sqlite3 fred_local.db "SELECT * FROM gold_macro_indicator_dashboard LIMIT 10;"
sqlite3 fred_local.db "SELECT * FROM gold_treasury_curve_metrics ORDER BY as_of_date DESC LIMIT 5;"
sqlite3 fred_local.db "SELECT * FROM gold_spread_inversion_episode;"

# what failed, if anything (per-series isolation writes the reason here)
sqlite3 fred_local.db "SELECT series_id, status, error_message
                       FROM audit_etl_series_run WHERE status != 'succeeded';"
```

## How it behaves (what you can rely on)

* **Key gating is per-source and activation-aware.** Keys are only required
  for sources with at least one active series (see
  `pipeline.SOURCE_KEY_REQUIREMENTS`); BLS and Census run keyless at reduced
  quotas; Treasury, World Bank, and SEC need no key at all (SEC wants
  `SEC_USER_AGENT`).
* **Idempotent re-runs.** Silver upserts on the natural key
  `(source, series_id, observation_date, realtime_start)` — activate a
  source, run, fix a bad id, run again; no cleanup needed.
* **Incremental by default.** After the first full load, each run re-pulls
  only the last `restate_last_n` observations per series (default 90) to
  capture revisions; `--full` overrides. See
  `docs/incremental_loading.md`.
* **Gold always rebuilds.** Every persisted run finishes by rebuilding all
  Gold tables — the classic feature tables *and* the market-terminal views
  (`docs/market_terminal_gold_views.md`). Tables whose inputs aren't ingested
  yet are simply empty and fill in as you activate series (e.g. every
  `is_recession` column stays NULL until `USREC` in `macro_flags.yml` is
  activated).

Related docs: [`architecture.md`](architecture.md) ·
[`adding_a_source.md`](adding_a_source.md) ·
[`incremental_loading.md`](incremental_loading.md) ·
[`deployment_runbook.md`](deployment_runbook.md) (Databricks jobs/secrets) ·
[`data_dictionary.md`](data_dictionary.md).
