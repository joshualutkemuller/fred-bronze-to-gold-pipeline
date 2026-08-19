# Pipeline Benchmarking Guide

**Purpose:** Measure where time is spent during pipeline execution.

**Tool:** `scripts/benchmark_pipeline.py`

---

## Quick Start

**Default benchmark (6 series):**
```bash
FRED_API_KEY=your_key python scripts/benchmark_pipeline.py
```

**With custom series:**
```bash
FRED_API_KEY=your_key python scripts/benchmark_pipeline.py \
  --series GDPC1,UNRATE,CPIAUCSL,DGS10,SOFR,BAMLH0A0HYM2
```

**Dry-run (faster, no writes):**
```bash
FRED_API_KEY=your_key python scripts/benchmark_pipeline.py --dry-run
```

**Save results to JSON:**
```bash
FRED_API_KEY=your_key python scripts/benchmark_pipeline.py \
  --output /tmp/bench_results.json
```

---

## Example Output

```
Starting benchmark (1 iteration(s))...

======================================================================
Running with default benchmark set (6 series)
WITH WRITES to /tmp/benchmark.db
======================================================================

Pipeline completed: partial
Total elapsed: 45.23s
Series succeeded: 6, failed: 0

Stage breakdown:
  bronze                     23.45s ( 51.8%)
  silver                      8.12s ( 17.9%)
  quality                     5.34s ( 11.8%)
  gold                        2.89s (  6.4%)
  persist                     0.41s (  0.9%)
  release_calendar            4.02s ( 11.2%)

======================================================================
BENCHMARK SUMMARY
======================================================================
Iterations: 1/1
Series per run: 6
Average time: 45.23s
Per series scaling: ~7.54s per series
```

---

## Benchmark Modes

### 1. Default Benchmark (6 series)
**Best for:** Quick performance checks, regression testing
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py
```
- Covers all data types: macro (GDPC1), labor (UNRATE), inflation (CPIAUCSL), rates (DGS10), funding (SOFR), credit (BAMLH0A0HYM2)
- Runtime: ~40-60 seconds
- API calls: ~50-100

### 2. Small Series Set (3 series)
**Best for:** Very quick testing, CI pipelines
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py --series GDPC1,UNRATE,CPIAUCSL
```
- Runtime: ~15-30 seconds
- API calls: ~10-20

### 3. Medium Series Set (20+ series)
**Best for:** Real-world scaling measurements
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py \
  --series GDPC1,UNRATE,CPIAUCSL,DGS1,DGS2,DGS5,DGS10,DGS30,SOFR,EFFR,IORB,BAMLH0A0HYM2,BAMLC0A0CM,PAYEMS,ICSA,INDPRO,HOUST,M2SL,WALCL,WRESBAL
```
- Runtime: ~60-90 seconds
- API calls: ~200-400

### 4. Full Catalog (all active series)
**Best for:** Maximum scaling analysis, nightly benchmarks
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py --full-catalog
```
- Runtime: **3-15 minutes** (depends on # active series, rate limiting)
- API calls: **Thousands**
- Warning: This will make many API calls and respect FRED rate limits

### 5. Dry-Run (no database writes)
**Best for:** Isolating extract/transform costs
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py --dry-run
```
- Runs extract, transform, quality checks, but skips warehouse writes
- Faster than full run (no I/O)
- Useful for measuring API/compute overhead

---

## Running Multiple Iterations

**Test consistency:**
```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py --iterations 3
```

Shows:
- Average time across runs
- Min/max times
- Standard deviation
- Coefficient of variation (if unstable, check system load)

---

## Key Metrics Explained

| Metric | Meaning | Action |
|---|---|---|
| **Total elapsed** | Wall-clock time from start to finish | Baseline metric |
| **Per series** | Total time ÷ # series | Scaling metric (should be stable) |
| **Bronze %** | Extract + API calls | Usually 50-70% (network-bound) |
| **Silver %** | Transform + validation | Usually 10-20% (CPU-bound) |
| **Gold %** | Build analytical tables | Usually 5-10% (CPU + I/O) |
| **Stddev** | Variation across iterations | <5% is good; >10% means high variance |

---

## Interpreting Results

### Healthy Pipeline
```
Total: 45s for 6 series
Per series: 7.5s
Bronze: 50%, Silver: 20%, Gold: 10%
Stddev: 2%
```
→ Network dominates (as expected for FRED API). Consistent runs.

### Gold Layer Bottleneck
```
Total: 120s for 20 series
Gold: 45%
```
→ Consider Polars acceleration for correlation/transform functions

### Variable Results
```
Iteration 1: 40s
Iteration 2: 55s
Iteration 3: 48s
Stddev: 8%
```
→ Check system load, FRED rate limits, or network conditions

---

## JSON Output Format

```bash
FRED_API_KEY=... python scripts/benchmark_pipeline.py --output /tmp/results.json
```

Produces:
```json
{
  "summary": {
    "iterations": 3,
    "series_per_run": 6,
    "average_sec": 45.67,
    "min_sec": 42.1,
    "max_sec": 48.2,
    "stddev_sec": 2.89,
    "per_series_sec": 7.61
  },
  "runs": [
    {
      "timestamp": "2026-08-19T10:30:00",
      "series_count": 6,
      "dry_run": false,
      "total_elapsed_sec": 42.1
    },
    ...
  ]
}
```

Use for:
- Regression tracking (compare against baseline)
- CI/CD performance gates
- Scaling analysis (plot per_series_sec vs series_count)

---

## Performance Tuning Checklist

**If pipeline is slow:**

1. ✅ Run benchmark to isolate bottleneck (Bronze vs Silver vs Gold)
2. ✅ Check FRED API rate limits (should be 120/min by default)
3. ✅ Check network latency to FRED API
4. ✅ For Gold-heavy workloads, enable Polars: `pip install polars`
5. ✅ For large series counts, consider incremental upserts (see `docs/upserts.md`)

**If Gold layer is slow:**

6. ✅ Check which Gold functions dominate (profile the build_gold logs)
7. ✅ Consider Polars for series_correlation (N² operations)
8. ✅ Profile database I/O (`sqlite3 -profile fred.db`)

**If results vary:**

9. ✅ Run with `--iterations 3` to measure variance
10. ✅ Check system load (CPU, disk I/O, network)
11. ✅ Run at consistent time of day (FRED rate limits affect morning more)

---

## See Also

- `docs/upserts.md` — Whether to use upserts instead of truncate+reload
- `src/fred_pipeline/timing.py` — The `@timed` decorator that captures stage times
- `src/fred_pipeline/io/local_store.py` — Gold layer build implementation
