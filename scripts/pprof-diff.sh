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
# Compare pprof profiles between two trees (e.g. profiles-gate-0 vs profiles-gate-1).
# Default: every *.prof file present in BOTH directories (delta = NEW minus BASE).
#
# Usage:
#   ./scripts/pprof-diff.sh BASE NEW
#   ./scripts/pprof-diff.sh 0 1
#   ./scripts/pprof-diff.sh BASE NEW BENCHMARK          # all kinds for one benchmark
#   ./scripts/pprof-diff.sh BASE NEW BENCHMARK KIND     # one profile file
#   ./scripts/pprof-diff.sh --list DIR|GATE
#
# Environment:
#   PROFILES_GATE_PREFIX   (default: profiles-gate)
#   PPROF                  (default: go tool pprof)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROFILES_GATE_PREFIX="${PROFILES_GATE_PREFIX:-profiles-gate}"
PPROF="${PPROF:-go tool pprof}"
NODECOUNT="${NODECOUNT:-5}"
KINDS_FILTER="${KINDS_FILTER:-cpu,mem,mutex,block}"

usage() {
	cat <<EOF
Usage: $(basename "$0") [OPTIONS] BASE NEW [BENCHMARK] [KIND]

Compare profiles where the same filename exists in BASE and NEW.
Delta = NEW minus BASE (negative flat/cum usually means less work in NEW).

Arguments:
  BASE, NEW     Directories or gate ids (0, 1, 2 -> profiles-gate-N)
  BENCHMARK     Optional: only files matching BENCHMARK.<kind>.prof
  KIND          Optional: cpu | mem | mutex | block (requires BENCHMARK)

Options:
  --list DIR|GATE       List *.prof files in one tree
  --kinds LIST          Comma-separated kinds to compare (default: cpu,mem,mutex,block)
  --nodecount N         Lines in each -top report (default: 5, env NODECOUNT)
  --http ADDR           Web UI for a single BENCHMARK [KIND] only (not --all mode)
  --list-fn REGEX       With single benchmark: also run -list=REGEX
  -h, --help

Examples:
  $(basename "$0") 0 1
  $(basename "$0") 0 1 --kinds mem
  $(basename "$0") profiles-gate-0 profiles-gate-1 BenchmarkHasRoomForRemap_large
  $(basename "$0") 0 1 BenchmarkAddReservations_large mem
  $(basename "$0") --list 1
EOF
}

resolve_dir() {
	local arg=$1
	if [[ "$arg" =~ ^[0-9]+$ ]]; then
		echo "${PROFILES_GATE_PREFIX}-${arg}"
		return
	fi
	echo "$arg"
}

list_profiles() {
	local dir
	dir=$(resolve_dir "$1")
	if [[ ! -d "$dir" ]]; then
		echo "No such directory: $dir" >&2
		exit 1
	fi
	echo "Profiles in $dir:"
	find "$dir" -maxdepth 1 -name '*.prof' -printf '  %f\n' 2>/dev/null | sort \
		|| find "$dir" -maxdepth 1 -name '*.prof' | sed 's|.*/|  |' | sort
}

parse_prof_name() {
	# Sets RE_STEM and RE_KIND from e.g. BenchmarkFoo_large.mem.prof
	local name=$1
	if [[ "$name" =~ ^(.+)\.(cpu|mem|mutex|block)\.prof$ ]]; then
		RE_STEM="${BASH_REMATCH[1]}"
		RE_KIND="${BASH_REMATCH[2]}"
		return 0
	fi
	return 1
}

kind_allowed() {
	local kind=$1
	[[ ",${KINDS_FILTER}," == *",${kind},"* ]]
}

pprof_top_flags() {
	local kind=$1
	case "$kind" in
		cpu)   echo -top -cum -nodecount="$NODECOUNT" ;;
		mem)   echo -top -alloc_space -nodecount="$NODECOUNT" ;;
		mutex) echo -top -nodecount="$NODECOUNT" ;;
		block) echo -top -nodecount="$NODECOUNT" ;;
		*)     return 1 ;;
	esac
}

run_diff_files() {
	local base_prof=$1 new_prof=$2 kind=$3
	local name
	name=$(basename "$base_prof")

	echo ""
	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	echo "  $name"
	echo "  delta (${kind}): $(basename "$(dirname "$new_prof")") minus $(basename "$(dirname "$base_prof")")"
	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

	local -a flags
	read -r -a flags <<<"$(pprof_top_flags "$kind")"

	if [[ -n "${HTTP_ADDR:-}" ]]; then
		local sample_flag=()
		[[ "$kind" == "mem" ]] && sample_flag=(-alloc_space)
		# shellcheck disable=SC2086
		$PPROF -http="$HTTP_ADDR" "${sample_flag[@]}" -base="$base_prof" "$new_prof"
		return
	fi

	# shellcheck disable=SC2086
	$PPROF "${flags[@]}" -base="$base_prof" "$new_prof" 2>&1 || true

	if [[ -n "${LIST_FN:-}" ]]; then
		echo ""
		echo "--- -list=${LIST_FN} ---"
		local list_sample=()
		[[ "$kind" == "mem" ]] && list_sample=(-alloc_space)
		# shellcheck disable=SC2086
		$PPROF "${list_sample[@]}" -base="$base_prof" "$new_prof" -list="$LIST_FN" 2>&1 || true
	fi
}

collect_pairs() {
	local base_dir=$1 new_dir=$2 bench_filter=${3:-} kind_filter=${4:-}

	BASE_DIR=$base_dir
	NEW_DIR=$new_dir
	PAIRS=()

	shopt -s nullglob
	for base_prof in "$base_dir"/*.prof; do
		local name stem kind
		name=$(basename "$base_prof")
		parse_prof_name "$name" || continue
		stem=$RE_STEM
		kind=$RE_KIND

		if [[ -n "$bench_filter" && "$stem" != "$bench_filter" ]]; then
			continue
		fi
		if [[ -n "$kind_filter" && "$kind" != "$kind_filter" ]]; then
			continue
		fi
		if ! kind_allowed "$kind"; then
			continue
		fi

		local new_prof="$new_dir/$name"
		[[ -f "$new_prof" ]] || continue

		PAIRS+=("$base_prof|$new_prof|$kind")
	done
	shopt -u nullglob
}

report_orphans() {
	local base_dir=$1 new_dir=$2
	local only_base=() only_new=()

	shopt -s nullglob
	for f in "$base_dir"/*.prof; do
		[[ -f "$new_dir/$(basename "$f")" ]] || only_base+=("$(basename "$f")")
	done
	for f in "$new_dir"/*.prof; do
		[[ -f "$base_dir/$(basename "$f")" ]] || only_new+=("$(basename "$f")")
	done
	shopt -u nullglob

	if ((${#only_base[@]} > 0)); then
		echo ""
		echo "Only in $(basename "$base_dir") (${#only_base[@]} files, not compared):"
		printf '  %s\n' "${only_base[@]}" | head -20
		((${#only_base[@]} > 20)) && echo "  ... and $((${#only_base[@]} - 20)) more"
	fi
	if ((${#only_new[@]} > 0)); then
		echo ""
		echo "Only in $(basename "$new_dir") (${#only_new[@]} files, not compared):"
		printf '  %s\n' "${only_new[@]}" | head -20
		((${#only_new[@]} > 20)) && echo "  ... and $((${#only_new[@]} - 20)) more"
	fi
}

run_all() {
	local base_dir=$1 new_dir=$2 bench_filter=${3:-} kind_filter=${4:-}

	if [[ ! -d "$base_dir" ]]; then
		echo "Missing directory: $base_dir" >&2
		exit 1
	fi
	if [[ ! -d "$new_dir" ]]; then
		echo "Missing directory: $new_dir" >&2
		exit 1
	fi

	if [[ -n "${HTTP_ADDR:-}" && -z "$bench_filter" ]]; then
		echo "--http requires BENCHMARK (and optional KIND); cannot use with compare-all mode" >&2
		exit 1
	fi

	collect_pairs "$base_dir" "$new_dir" "$bench_filter" "$kind_filter"

	echo "Comparing ${#PAIRS[@]} profile pair(s)"
	echo "  BASE: $(basename "$base_dir") -> $base_dir"
	echo "  NEW:  $(basename "$new_dir") -> $new_dir"
	echo "  kinds: ${KINDS_FILTER}"
	echo "  (delta = NEW minus BASE; negative = less in NEW)"

	if ((${#PAIRS[@]} == 0)); then
		echo ""
		echo "No matching profiles to compare." >&2
		report_orphans "$base_dir" "$new_dir"
		exit 1
	fi

	local entry base_prof new_prof kind
	for entry in "${PAIRS[@]}"; do
		IFS='|' read -r base_prof new_prof kind <<<"$entry"
		run_diff_files "$base_prof" "$new_prof" "$kind"
	done

	report_orphans "$base_dir" "$new_dir"
	echo ""
	echo "Done: ${#PAIRS[@]} comparison(s)."
}

HTTP_ADDR=""
LIST_FN=""
POSITIONAL=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		-h | --help)
			usage
			exit 0
			;;
		--list)
			shift
			[[ $# -ge 1 ]] || { echo "--list requires DIR or GATE id" >&2; exit 1; }
			list_profiles "$1"
			exit 0
			;;
		--kinds)
			shift
			[[ $# -ge 1 ]] || { echo "--kinds requires LIST" >&2; exit 1; }
			KINDS_FILTER=$1
			shift
			;;
		--nodecount)
			shift
			[[ $# -ge 1 ]] || { echo "--nodecount requires N" >&2; exit 1; }
			NODECOUNT=$1
			shift
			;;
		--http)
			shift
			[[ $# -ge 1 ]] || { echo "--http requires ADDR" >&2; exit 1; }
			HTTP_ADDR=$1
			shift
			;;
		--list-fn)
			shift
			[[ $# -ge 1 ]] || { echo "--list-fn requires REGEX" >&2; exit 1; }
			LIST_FN=$1
			shift
			;;
		--)
			shift
			POSITIONAL+=("$@")
			break
			;;
		-*)
			echo "Unknown option: $1" >&2
			usage >&2
			exit 1
			;;
		*)
			POSITIONAL+=("$1")
			shift
			;;
	esac
done

set -- "${POSITIONAL[@]}"
if [[ $# -lt 2 ]]; then
	usage >&2
	exit 1
fi

BASE_DIR=$(resolve_dir "$1")
NEW_DIR=$(resolve_dir "$2")
shift 2

BENCH_FILTER=""
KIND_FILTER=""

if [[ $# -ge 1 && "$1" != --* ]]; then
	BENCH_FILTER=$1
	shift
fi
if [[ $# -ge 1 && "$1" != --* ]]; then
	KIND_FILTER=$1
	KINDS_FILTER=$KIND_FILTER
	shift
fi

if [[ $# -gt 0 ]]; then
	echo "Unexpected arguments: $*" >&2
	usage >&2
	exit 1
fi

run_all "$BASE_DIR" "$NEW_DIR" "$BENCH_FILTER" "$KIND_FILTER"
