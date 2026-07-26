# Point-in-time, weight-adjusted index total return

**Status: PROPOSED — not started.** Scopes a specific Gold-layer gap
surfaced while reviewing constituent-pricing coverage: the pipeline has all
the raw ingredients to compute a survivorship-bias-free, weight-adjusted
IVV/S&P 500 return series, but nothing currently combines them.

## The gap, confirmed by direct inspection

Two Gold tables exist today and are computed **independently of each
other**:

- `gold.equity_return_daily` / `gold.equity_total_return_index`
  (`writer/equity_views.py:92,200`) — one return series **per ticker**,
  built from that ticker's entire available Silver price history. There is
  no reference to index membership anywhere in either function: a ticker
  that rotated out of IVV gets a return computed from whatever price
  history it has, same as a current constituent.
- `gold.index_constituents` (`writer/equity_views.py:132`) — one row per
  ETF × constituent × **snapshot date**, with `weight_pct`, `weight_rank`,
  and an `is_latest_snapshot` flag. This one *is* point-in-time — multiple
  `observation_date` snapshots are retained, not just "today's list."

**Nothing joins the two.** There is no Gold table that says "the S&P 500's
return on date D, using only names that were actual members on date D,
weighted by their weight on date D." Confirmed via `grep` across
`writer/gold.py`, `writer/global_views.py`, and `io/local_store.py` — no
function references both `index_constituents` and `equity_return_daily`/
`equity_total_return_index` together.

If the mental model for "constituent returns" was an index-level
aggregate, that computation doesn't exist yet. What exists is (a)
survivorship-unfiltered per-ticker returns, and (b) a separate point-in-time
membership/weight table.

## Two things this build depends on — checked directly, not assumed

1. **Snapshot history is currently very thin.** `gold_index_constituents`
   for `IVV` only has **2 distinct snapshot dates** right now
   (2026-07-16, 2026-07-17) — the daily-cadence design intent
   (`manifests/etf_holdings.yml`'s `frequency: d`) hasn't accumulated real
   history yet; the `ishares` source has only actually been run twice.
   **This matters for scope**: a true historical backtest of this index
   return isn't possible yet — the computation can only be point-in-time
   correct *from whenever daily snapshotting starts running continuously,
   forward*. There is no way to reconstruct true historical S&P 500
   membership/weights before that without a paid iShares/vendor holdings-
   history subscription (iShares' public holdings download is
   current-day-only, by design) — worth being explicit about this
   limitation up front rather than promising a multi-year backtest.
2. **`IVV` itself isn't currently a priced ticker.** `equity_tiingo.yml`
   has no `IVV` entry — checked directly, zero matches. The natural
   validation check for this build (does the constructed weighted-
   constituent return track the real IVV ETF's own daily return, which it
   should almost exactly since IVV *is* a physical replication of the same
   index) isn't available until `IVV` is added as a priced ticker. **This
   is a one-line manifest addition and should be done first**, both for
   this validation and because it's a fairly glaring gap on its own (IVV is
   the exact ETF this whole constituent-pricing pipeline is built around,
   yet its own price series isn't ingested).

## Build

1. **Add `IVV` to `equity_tiingo.yml`** (trivial, do first — unblocks
   validation below).
2. **New pure engine function** in `writer/equity_views.py`, alongside the
   existing `compute_index_constituents`/`compute_equity_total_return_index`:
   `compute_index_weighted_return(constituent_rows, ticker_return_rows, *,
   index_etf) -> list[dict]`.
   - Grain: `index_etf · observation_date · price_return · total_return ·
     n_constituents_included · weight_coverage_pct`.
   - **Weighting convention**: use each ticker's weight as of the *prior*
     snapshot date applied to its return over the current date (standard
     beginning-of-period portfolio-return convention: a name's weight going
     into day D should come from D-1's snapshot, not D's — otherwise a
     stock's own return that day partially determines the weight used to
     measure it, which is circular). Needs an as-of/"most recent snapshot
     on or before D-1" lookup — the same shape as the `_last_on_or_before`
     helper already used in `writer/terminal_views.py` for FOMC meeting
     dates; reuse that pattern rather than re-deriving it.
   - **Coverage/normalization**: on any given day, not every constituent
     will have a fresh price (Tiingo quota throttling, a name mid-batch in
     `price_all_constituents.py`, etc.). Renormalize the included weights
     to sum to 1 before aggregating, and report `weight_coverage_pct` (sum
     of included weight ÷ sum of that day's total snapshot weight) so a
     consumer can tell a 99%-covered day from a 60%-covered one rather than
     silently averaging over whatever happened to be priced.
   - Sum weighted per-ticker `price_return`/`total_return` (from
     `compute_equity_return_daily`/`compute_equity_total_return_index`'s
     output, joined by ticker + date) to get the day's aggregate return.
3. **Wire into both backends** (`writer/gold.py` Spark path,
   `io/local_store.py` SQLite path) alongside the existing
   `index_constituents`/`equity_return_daily` build calls — same pattern,
   no new orchestration needed.
4. **`sql/50_gold.sql`** (+ `local_store.py` `_SCHEMA`): new
   `gold.index_weighted_return` table matching the grain above.
5. **`docs/dictionary/data_dictionary.md`** entry + a `POWERBI_CATALOG` row
   (`writer/global_views.py`'s `_entry(...)` helper), same as every other
   Gold table.
6. **Validation, once `IVV` is priced**: compare
   `gold.index_weighted_return`'s `total_return` for `IVV` against
   `gold.equity_total_return_index` where `ticker='IVV'` directly — they
   should track closely (small deltas from IVV's own expense ratio, cash
   drag, and sampling vs. full replication are expected; a large or
   systematic divergence would flag a bug in the weighting/join logic
   rather than a real economic difference). This is a much stronger check
   than a synthetic unit test alone, since it's validating against a real,
   independently-priced instrument tracking the same index.
7. **Tests**: a hand-computable weighted-average case (3 tickers, known
   weights and returns, known expected aggregate); a coverage/
   renormalization test when one constituent is missing a price that day;
   a prior-snapshot-weight test verifying day D's weighting comes from
   D-1's snapshot, not D's own.

## Notes / non-goals

- This does **not** need new data ingestion — everything it depends on
  (`gold_index_constituents`, `gold_equity_return_daily`,
  `gold_equity_total_return_index`) already exists and is already being
  populated by the running pipeline.
- Scope this to `IVV`/S&P 500 first (the only ETF with holdings ingestion
  wired up — `manifests/etf_holdings.yml` ships `SPY` inactive pending its
  XLSX-parser caveat). Generalizing to other index ETFs is a manifest
  activation + parser-path confirmation, not a new engine.
- Out of scope: reconstructing historical (pre-snapshot) membership. That
  would require a paid holdings-history vendor and is a separate, larger
  decision (arguably belongs alongside
  [`docs/handoffs/asset_class_expansion.md`](asset_class_expansion.md)'s
  paid-vendor-licensing discussion, not bundled into this build).

## Summary

| Step | Effort | Blocked on |
|---|---|---|
| Add `IVV` to `equity_tiingo.yml` | Trivial | Nothing — do first |
| `compute_index_weighted_return` engine + tests | S–M | Nothing |
| Wire into both Gold backends + DDL + dictionary/catalog entries | S | The engine function above |
| Validate against IVV's own realized return | Trivial once `IVV` is priced | `IVV` ticker having a few days of price history |
| True historical backtest (pre-2026-07-16) | Not currently possible | A paid holdings-history vendor — separate decision |

Recommended order: add `IVV` to the manifest now (costs nothing, unblocks
validation later), then build the engine function + wiring whenever this is
prioritized — the daily snapshot history will keep accumulating in the
background regardless of when the computation itself gets built.
