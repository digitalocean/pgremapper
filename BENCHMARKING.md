# Benchmarking

This repository includes benchmark scaffolding in `benchmark_test.go` so you can profile parsing and remap decision paths without a live Ceph cluster.

For general background on Go profiling, see the [runtime/pprof package documentation](https://pkg.go.dev/runtime/pprof).

## Scripts

[`scripts/bench-all.sh`](scripts/bench-all.sh) runs benchmarks with **CPU, heap, mutex, and block** profiles.

### Recommended: per-benchmark profiles

Runs each sub-benchmark in its own `go test` invocation. Text metrics go to `bench-results.txt`; profiles land in `profiles/` as separate files per benchmark (for example `profiles/BenchmarkCalcPgMappingsToUndoBackfill_large.cpu.prof`).

```
./scripts/bench-all.sh
```

Defaults: `-benchtime=15s`, `-count=6`, `-benchmem`, all four profile types.

Focus on production-scale fixtures only:

```
BENCH_FILTER='/(medium|large)$' ./scripts/bench-all.sh
```

Tune duration and repetitions:

```
BENCHTIME=30s COUNT=5 ./scripts/bench-all.sh
```

### Combined profiles (single run)

One `go test` pass; all benchmarks share merged profile files under `profiles/{cpu,mem,mutex,block}.prof`. Faster to kick off, harder to attribute hotspots to a single code path.

```
./scripts/bench-all.sh --combined
```

### Smoke check

One iteration per benchmark, no profiles:

```
./scripts/bench-all.sh --smoke
```

### Race detector

One pass over the benchmark suite with Go's race detector enabled. No profiles; intended to catch data races in code paths exercised by the benchmarks. This is significantly slower than `--smoke` — use `BENCH_FILTER` to narrow scope when needed.

```
./scripts/bench-all.sh --race
```

```
BENCH_FILTER='/(medium|large)$' ./scripts/bench-all.sh --race
```

### Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `BENCHTIME` | `15s` | Minimum time per sub-benchmark |
| `COUNT` | `6` | Timed repetitions (for `ns/op`, `B/op`, `allocs/op` spread; benchstat needs ≥4 for `%` lines) |
| `BENCH_FILTER` | `.` | Passed to `go test -bench` |
| `OUT_DIR` | `profiles` | Profile output directory |
| `RESULTS` | `bench-results.txt` | Text benchmark output |

`-benchtime` applies **per sub-benchmark**, not once for the whole suite. A full run over every `small`/`medium`/`large` case can take hours; use `BENCH_FILTER` to narrow scope when iterating.

## Fixture sizes

Benchmark fixture profiles used in `benchmark_test.go`:

* `small`: `pgCount=4096`, `osdCount=128`, and for upmap-heavy cases `upmapCount=300`.
* `medium`: `pgCount=16384`, `osdCount=512`, and for upmap-heavy cases `upmapCount=1200`.
* `large`: `pgCount=65536`, `osdCount=2048`, and for upmap-heavy cases `upmapCount=4800`.

Notes:

* Most benchmark families include `small`, `medium`, and `large` sub-benchmarks.
* Benchmark setup is included in timed sections, so wall-clock runtime more closely tracks the benchtime target.

## Manual commands

Run all benchmarks (no profiles):

```
go test -run '^$' -bench=. -benchmem ./...
```

Run a specific benchmark family:

```
go test -run '^$' -bench='^BenchmarkCalc' -benchtime=15s -benchmem ./...
```

`-benchmem` adds `B/op` and `allocs/op` columns alongside `ns/op`.

## Comparing results across commits

Save a baseline before changes:

```
./scripts/bench-all.sh
cp bench-results.txt baseline-bench-results.txt
```

After changes, rerun and compare with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```
go install golang.org/x/perf/cmd/benchstat@latest
benchstat baseline-bench-results.txt bench-results.txt
```

Run each pass at least six times (`COUNT=6` by default) and compare medians. Keep machine load low while benchmarking.

Save gate baselines under `baselines/` (for example `baselines/gate-0-scaffold.txt`) and compare stacked perf work:

```
benchstat baselines/gate-0-scaffold.txt bench-results.txt
```

### Compare profiles between gates

[`scripts/pprof-diff.sh`](scripts/pprof-diff.sh) diffs matching `*.prof` files in two directories (delta = NEW minus BASE; negative usually means less work in NEW):

```
./scripts/pprof-diff.sh 0 5 > pprof.results
```

Gate ids map to `profiles-gate-N/` when those directories exist. Use the same `BENCH_FILTER` on both runs so filenames overlap. Cumulative `0 → N` is a whole-stack story; adjacent gates (`1 → 2`) isolate a single PR when profiles were captured with the same filter.

## Inspect profiles with pprof

Per-benchmark CPU profile:

```
go tool pprof -top -cum profiles/BenchmarkMustGetCurrentMappingState_large.cpu.prof
```

Heap allocations (GC pressure sources):

```
go tool pprof -top -alloc_space profiles/BenchmarkMustGetCurrentMappingState_large.mem.prof
```

Mutex contention:

```
go tool pprof -top profiles/BenchmarkCalcPgMappingsToUndoBackfill_large.mutex.prof
```

Block profile:

```
go tool pprof -top profiles/BenchmarkCalcPgMappingsToUndoBackfill_large.block.prof
go tool pprof -top -sample_index=contentions profiles/BenchmarkCalcPgMappingsToUndoBackfill_large.block.prof
```

Combined-mode profiles:

```
go tool pprof -top -cum profiles/cpu.prof
go tool pprof -top -alloc_space profiles/mem.prof
```

Interactive web UI:

```
go tool pprof -http=:8080 profiles/BenchmarkMustGetCurrentMappingState_large.cpu.prof
```

Then open `http://localhost:8080` in a browser.

### Useful pprof commands inside interactive mode

* `top` - hottest functions by self time
* `top -cum` - hottest functions by cumulative time
* `list <function>` - annotated source for a function
* `web` - render the call graph (requires Graphviz installed)
