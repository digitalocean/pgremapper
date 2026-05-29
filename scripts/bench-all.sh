#!/usr/bin/env bash
# Copyright 2026 DigitalOcean
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run pgremapper benchmarks with CPU, heap, mutex, and block profiles.
#
# Default (per-benchmark): one go test invocation per sub-benchmark, writing
# separate .cpu.prof, .mem.prof, .mutex.prof, and .block.prof files under OUT_DIR.
#
# Usage:
#   ./scripts/bench-all.sh                    # per-benchmark profiles (recommended)
#   ./scripts/bench-all.sh --combined         # single run, merged profiles
#   ./scripts/bench-all.sh --smoke            # 1 iteration, no profiles
#   ./scripts/bench-all.sh --race             # 1 iteration with -race, no profiles
#   ./scripts/bench-all.sh --help
#
# Environment:
#   BENCHTIME   minimum time per benchmark (default: 15s)
#   COUNT       number of timed repetitions (default: 3)
#   BENCH_FILTER  go test -bench regex (default: .)
#   OUT_DIR     profile output directory (default: profiles)
#   RESULTS     text benchmark output file (default: bench-results.txt)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BENCHTIME="${BENCHTIME:-15s}"
COUNT="${COUNT:-3}"
BENCH_FILTER="${BENCH_FILTER:-.}"
OUT_DIR="${OUT_DIR:-profiles}"
RESULTS="${RESULTS:-bench-results.txt}"

usage() {
	cat <<'EOF'
Usage: bench-all.sh [MODE]

Modes:
  per-benchmark   Run each sub-benchmark separately with all four profiles (default).
  combined        Run all selected benchmarks in one go test; profiles are merged.
  smoke           One iteration per benchmark, benchmem only, no profiles.
  race            One iteration per benchmark with the race detector; no profiles.

Environment variables:
  BENCHTIME       Minimum duration per benchmark (default: 15s)
  COUNT           Repetitions for timed runs (default: 3)
  BENCH_FILTER    go test -bench regex (default: .)
  OUT_DIR         Directory for profile files (default: profiles)
  RESULTS         File for text benchmark output (default: bench-results.txt)

Examples:
  ./scripts/bench-all.sh
  BENCH_FILTER='/(medium|large)$' ./scripts/bench-all.sh
  BENCHTIME=30s COUNT=5 ./scripts/bench-all.sh --combined
  ./scripts/bench-all.sh --smoke
  ./scripts/bench-all.sh --race
EOF
}

list_benchmarks() {
	go test -run '^$' -bench="$BENCH_FILTER" -benchtime=1ns ./... 2>&1 \
		| awk '/^Benchmark/ {
			name = $1
			sub(/-[0-9]+$/, "", name)
			print name
		}'
}

run_smoke() {
	echo "Running smoke benchmarks (benchtime=1x, no profiles)..."
	go test -run '^$' -bench="$BENCH_FILTER" -benchtime=1x -benchmem ./...
}

run_race() {
	echo "Running race detector over benchmarks (benchtime=1x, no profiles)..."
	go test -race -run '^$' -bench="$BENCH_FILTER" -benchtime=1x ./...
}

run_combined() {
	mkdir -p "$OUT_DIR"
	echo "Running combined benchmark pass (benchtime=$BENCHTIME, count=$COUNT)..."
	echo "Profiles: $OUT_DIR/{cpu,mem,mutex,block}.prof"
	go test -run '^$' -bench="$BENCH_FILTER" \
		-benchtime="$BENCHTIME" -benchmem -count="$COUNT" \
		-cpuprofile="$OUT_DIR/cpu.prof" \
		-memprofile="$OUT_DIR/mem.prof" \
		-mutexprofile="$OUT_DIR/mutex.prof" \
		-blockprofile="$OUT_DIR/block.prof" \
		./... | tee "$RESULTS"
}

run_per_benchmark() {
	mkdir -p "$OUT_DIR"
	: >"$RESULTS"

	mapfile -t benches < <(list_benchmarks)
	if ((${#benches[@]} == 0)); then
		echo "No benchmarks matched BENCH_FILTER=$BENCH_FILTER" >&2
		exit 1
	fi

	echo "Running ${#benches[@]} benchmarks (benchtime=$BENCHTIME, count=$COUNT)..."
	echo "Text output: $RESULTS"
	echo "Profiles: $OUT_DIR/<benchmark>.{cpu,mem,mutex,block}.prof"

	for b in "${benches[@]}"; do
		safe="${b//\//_}"
		echo "=== $b ===" | tee -a "$RESULTS"
		go test -run '^$' -bench="^${b}\$" \
			-benchtime="$BENCHTIME" -benchmem -count="$COUNT" \
			-cpuprofile="$OUT_DIR/${safe}.cpu.prof" \
			-memprofile="$OUT_DIR/${safe}.mem.prof" \
			-mutexprofile="$OUT_DIR/${safe}.mutex.prof" \
			-blockprofile="$OUT_DIR/${safe}.block.prof" \
			./... 2>&1 | tee -a "$RESULTS"
	done
}

mode="${1:-per-benchmark}"
case "$mode" in
	per-benchmark | --per-benchmark) run_per_benchmark ;;
	combined | --combined) run_combined ;;
	smoke | --smoke) run_smoke ;;
	race | --race) run_race ;;
	-h | --help | help) usage ;;
	*)
		echo "Unknown mode: $mode" >&2
		usage >&2
		exit 1
		;;
esac
