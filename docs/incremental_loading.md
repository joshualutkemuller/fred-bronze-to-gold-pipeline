# Incremental Loading

This document explains what a daily run actually fetches from FRED. The short
answer: **in steady state a daily run is incremental — it does not re-pull full
history for every series.** After a series' first run, each subsequent run
re-pulls only the last *N* observations and `MERGE`s them, which restates recent
revisions without re-downloading the whole series.

The authoritative implementation is
[`_plan_extract`](../src/fred_pipeline/pipeline.py) and
[`_extract`](../src/fred_pipeline/pipeline.py) in `pipeline.py`.

## How the load window is decided

For each series, `_plan_extract` returns `(observation_start, load_type)`:

```python
if force_full or spec.load_type == LoadType.FULL or self.warehouse is None:
    return None, "full"                       # observation_start=None => full history
n = spec.restate_records or self.config.restate_last_n
start = self.warehouse.restate_start(spec.series_id, n)
if start is None:
    return None, "full"                       # first load: series not in warehouse yet
return start, f"restate_last_{n}"             # only the last N observations
```

`restate_start(series_id, n)` reads the warehouse watermark and returns the
observation date of the *N*-th most recent observation. That date becomes the
`observation_start` passed to the FRED `series/observations` request, so only
observations on or after it are pulled and merged on the natural key
`(series_id, observation_date, realtime_start)`.

Example: a normal daily run for `DGS10` fetches roughly the last 90 daily
observations — not the full history back to 1962.

## Effective defaults

| Setting | Default | Source |
|---|---|---|
| `load_type` | `incremental` | `manifest.py` (`SeriesSpec`) |
| `vintage_enabled` | `true` | `manifest.py` (`SeriesSpec`) |
| `restate_last_n` | `90` | `config.py` (`PipelineConfig`) |
| `restate_records` (per-series override) | `None` → falls back to `restate_last_n` | manifest |
| `complete_vintage_history` | `false` | `config.py` (`PipelineConfig`) |

All series in the current manifests use `load_type: incremental`. Several set
tighter per-series overrides, e.g. GDP `restate_records: 24`, CPI and payrolls
`restate_records: 36`.

## The vintage nuance

For `vintage_enabled: true` series (the default, and most of the manifest),
`_extract` still requests the full real-time window
(`realtime_start=1776-07-04`, `realtime_end=9999-12-31`). So it pulls **all
revision vintages**, but only for the last-*N* observation *dates*. The request
is bounded on the observation-date axis and unbounded on the vintage axis. This
is the intended point-in-time (ALFRED) behavior and stays cheap because *N* is
small.

## Cases that DO pull full history

A run fetches full history for a series when any of the following holds:

1. **First run of a series** — the series is not yet in the warehouse, so
   `restate_start` returns `None` and the load falls back to full. This is
   one-time per series, not per run.
2. **`--force-full` / `force_full=True`** — an explicit full reload.
3. **No warehouse backend (dry run)** — with `warehouse is None` there is no
   watermark to read, so the load is always full.
4. **`load_type: full` series** — none exist in the current manifests, but such
   a series would pull full history on every run.
5. **`FRED_COMPLETE_VINTAGE_HISTORY=true`** — flips vintage-enabled series to
   `get_observations_all_vintages`, which enumerates every vintage date and
   batches under FRED's 2000-vintage-dates-per-request cap. This is much
   heavier. It is **off by default**, and even when on it still respects the
   last-*N* `observation_start` window — so it fetches the full *vintage*
   history for recent observations, not the full observation history.

## Bottom line

In its current state (all-incremental manifests, `complete_vintage_history`
off), a daily run is incremental: one full backfill the first time each series
is seen, then last-*N*-observation restatement thereafter. The main cost driver
to watch is that vintage-enabled series fetch the full revision history for
those recent observations — fine at the current scale, but worth monitoring if
you widen `restate_records` or enable `complete_vintage_history`.
