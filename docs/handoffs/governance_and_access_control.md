# Path A: Governance, licensing, and access-control infrastructure

**Status: IN PROGRESS.** One of two candidate "what's next" directions (see
`docs/handoffs/asset_class_expansion.md` for the other). This doc scopes
what it would take to make this pipeline safely usable *across multiple
teams inside a financial firm*, as opposed to a single-user research
warehouse.

**Item 1 (per-source data-licensing register) — DONE (2026-07-26).**
`config/data_licensing.yml` (11 sources) + `governance/licensing.py`
(loader + `check_commercial_use`) + `fred_pipeline validate --commercial`
guardrail + a data-dictionary summary table. Live-tested: correctly flags
`tiingo` (85 active series, free-tier-personal-use) and `ishares` (1 active
series, requires-agreement/unreviewed) as not clearing commercial use, and
exits non-zero. See the item below for the as-built detail.

**Item 2 (query-level access logging) — DONE (2026-07-26).** Local backend:
`LocalWarehouse.query()` now logs every call to a new `audit_query_log`
table (`queried_at`, a SHA-256 hash of the query text, an optional `caller`
label) — live-tested. Databricks: no code needed, just documented — Unity
Catalog's system tables already capture this once Gold/Silver are
registered there (`docs/deployment/deployment_runbook.md` A10). Item 3
remains unstarted.

## Context

**Why this, not more data.** The series universe is already broad — 2,500+
FRED series, a full equity/ETF universe (85 ETFs + 507 S&P 500
constituents), credit spreads by rating bucket, 24 FX pairs, and a working
ML/forecasting layer. Data breadth has strongly diminishing returns from
here. What a real financial firm actually needs before it can point more
than one desk at this warehouse is **trust infrastructure**: proof that
each data source's usage terms are being honored, and control over who can
see what. This was flagged concretely earlier in this project's own
history — a CME Fed Funds futures connector was scoped and explicitly
rejected on licensing/redistribution-cost grounds (see
`docs/handoffs/completed/terminal_phase0_gaps.md` item 3's rationale) —
that's a real instance of the exact risk this doc is about, decided on an
ad hoc, one-off basis rather than via any systematic process.

**Current state, confirmed by direct inspection:**
- `governance/audit.py` — thorough **operational** lineage (`audit.etl_run`,
  `audit.etl_series_run`, `audit.data_quality_result`): what ran, when, how
  many rows, per-series pass/fail. No concept of *who* ran it or *who else*
  later read the output.
- `governance/reconcile.py` — metadata drift/staleness only (FRED-sourced
  series). Not licensing-related.
- `governance/notify.py` — a Slack-webhook run-status notifier. No
  data-access alerting.
- `tools/secret_manager.py` — a solid, portable, layered credential
  resolver (env → OS keychain → file → vault). This solves *storing* API
  keys; it does not solve *authorizing* who can query the data those keys
  pulled in.
- **Nothing exists for**: per-source licensing/redistribution terms tracked
  in the codebase, role-based query access, or a query-level access log
  ("who looked at what, when"). Confirmed via repo-wide search — zero hits
  for role/permission/RBAC/access-control code, and zero hits for any
  license/redistribution tracking outside of a couple of prose comments
  (the Stooq anti-bot note, the ishares.py holdings-URL caveat).

## 1. Per-source data-licensing register — *(smallest lift, highest immediate value)*

**Status: DONE (2026-07-26).** `config/data_licensing.yml` registers all 11
sources actually in use today (the ~13 estimate below included ICE, which
turned out to ride in via FRED rather than being its own `source` value —
confirmed by checking the live distinct-source list across every manifest).
`governance/licensing.py` provides the loader
(`load_data_licensing_config`) and the check
(`check_commercial_use` — fails closed on an unregistered source too, not
just an explicitly-disallowed one). `fred_pipeline validate --commercial`
wires it in and was live-tested against the real manifest universe: it
correctly flags `tiingo` (85 active series, free-tier-personal-use) and
`ishares` (1 active series, requires-agreement/unreviewed), exits non-zero.
A summary table lives in `docs/dictionary/data_dictionary.md`. Stooq and
iShares are both marked `requires-agreement`/commercial-use-disallowed as
conservative defaults pending an actual terms review — the register makes
that gap visible rather than resolving it.

**The problem (as originally scoped).** This pipeline pulls from ~13 distinct upstream sources
(FRED, BLS, EIA, BEA, Census, Treasury, World Bank, SEC, Tiingo, Stooq,
iShares/SSGA, ICE via FRED, plus whatever `discover` onboards next). Each
has different terms: FRED/BLS/Treasury/Census/World Bank/SEC are U.S.
government or multilateral public data (effectively public domain/open
license); Tiingo's free tier is explicitly **personal-use** (see
`handoff.md`'s "Commercial use — Decided: personal use" note); Stooq and
iShares/SSGA holdings pages carry their own (unclear, largely unchecked)
terms. None of this is centrally tracked — it lives in scattered comments
across manifest headers.

**Build.**
1. A single `config/data_licensing.yml` (or a `licensing` column set on
   each manifest's own header, whichever stays easier to keep current):
   per source, `license_type` (public-domain / open-data / free-tier-
   personal-use / free-tier-commercial-ok / requires-agreement),
   `redistribution_allowed` (bool), `commercial_use_allowed` (bool),
   `attribution_required` (bool + text), `terms_url`, `last_reviewed_date`.
2. A `fred_pipeline validate` (or new `licensing-check`) extension: fail
   loudly if a manifest activates a source whose `commercial_use_allowed`
   is `false` while a `--commercial` / environment flag is set — a cheap
   guardrail against exactly the kind of ad hoc, easy-to-forget judgment
   call the CME decision required manually this time.
3. Surface `license_type` per table in `docs/dictionary/data_dictionary.md`
   (one column addition) so anyone building a report on top of Gold can see
   at a glance what they're allowed to do with the output.

**Verdict:** this is a documentation-and-guardrail exercise, not new
engineering — a few days, and it converts "we happened to catch the CME
issue because someone asked" into "the pipeline itself won't let you
activate a source outside its license terms without a conscious override."

## 2. Query-level access logging — *(medium lift)*

**Status: DONE (2026-07-26).** Both halves built as scoped below: the local
`audit_query_log` table (`io/local_store.py`'s `LocalWarehouse.query()`/
`_log_query()`, tests in `tests/test_local_store.py`) and the Databricks
documentation (`docs/deployment/deployment_runbook.md` A10). Retention/
read-access are left as checklist items in the runbook — genuine policy
questions for whoever owns compliance at the firm, not something to
silently default.

**The problem (as originally scoped).** `audit.etl_run` records *what the pipeline itself did*
(extraction, DQ, Gold rebuild). It records nothing about *who read Gold
afterward* — which matters the moment more than one person/team points a
BI tool or notebook at the same warehouse. A firm's compliance/security
review will ask "can you show me every time someone queried company X's
SEC fundamentals" long before it asks about model accuracy.

**Build.**
1. On Databricks: this is largely **already available** via Unity Catalog's
   built-in query history/audit logs once tables are registered there —
   the cheap win here may just be *documenting how to pull and retain
   that*, not building anything new.
2. For the local SQLite backend (which has no such built-in log): a thin
   wrapper around `LocalWarehouse.query()` that appends
   `(queried_at, query_text_hash, caller)` to an `audit_query_log` table.
   Deliberately minimal — this backend is for individual dev/research use,
   not the multi-user case, so don't over-build it.
3. Document retention expectations (how long logs are kept, who can read
   them) — a policy question for the user/firm, not something to default
   silently.

**Verdict:** mostly a documentation task on Databricks (Unity Catalog
already does the hard part); a small, optional addition on the local
backend. Lower priority than item 1 since it only matters once multiple
consumers exist.

## 3. Role-based read access to Gold — *(largest lift, most firm-specific)*

**The problem.** Right now, anyone with warehouse access sees every table —
macro data alongside individual company fundamentals (`gold.
fred_company_fundamentals`, SEC-sourced) and equity positions-adjacent data
(constituent weights, factor attribution). A trading desk, a compliance
team, and an external-facing reporting team plausibly need different
slices of the same Gold layer.

**Build.**
1. On Databricks: Unity Catalog's native grant system
   (`GRANT SELECT ON TABLE ... TO ...`) already provides this — the actual
   work is **defining the role→table mapping** (a policy exercise, ideally
   done with whoever owns compliance at the firm), then codifying it as a
   `resources/*.yml`-style declarative grants file so it's version
   controlled and reviewable like everything else here, not applied by
   hand once and forgotten.
2. On the local SQLite backend: genuine RBAC isn't really meaningful for a
   single-file dev database — skip it there; note explicitly that the
   local backend is single-user only and role separation requires the
   Databricks/Delta path.

**Verdict:** highest value for genuine multi-team firm use, but it's a
policy decision (who should see what) dressed as an engineering task —
don't start the engineering until the role→table mapping is agreed, or
it'll need redoing.

## Summary

| # | Item | Effort | Engineering vs. policy | Priority |
|---|---|---|---|---|
| 1 | Per-source data-licensing register + activation guardrail | S | Mostly engineering (a config file + a validate check) | **DONE** |
| 2 | Query-level access logging | S–M | Mostly documentation on Databricks; small build for local backend | **DONE** |
| 3 | Role-based read access to Gold tables | M–L | Mostly policy (role→table mapping) + light engineering to codify it | Depends on firm's org readiness |

Recommended order: **1 — done. 2 — done.** Item 3 needs input from whoever
would own compliance/security at the firm this serves (the role→table
mapping is a policy decision, not an engineering one) — worth scoping now,
but don't build it until that conversation happens.
