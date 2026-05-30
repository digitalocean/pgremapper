// Copyright 2026 DigitalOcean
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type benchmarkFixture struct {
	pgDump  string
	osdDump string
	osdTree string
	poolLs  string
}

func resetBenchmarkGlobals() {
	savedOsdDumpOut = nil
	savedOsdPoolsDetails = nil
	savedParsedOsdTree = nil
	savedPgDumpPgsBrief = nil
	savedPgUpmapItemMap = nil
	savedPgUpmapItemMapSource = nil

	M = nil
}

func installFixture(f benchmarkFixture) {
	runOsdDump = func() (string, error) { return f.osdDump, nil }
	runPgDumpPgsBrief = func() (string, error) { return f.pgDump, nil }
	runOsdTree = func() (string, error) { return f.osdTree, nil }
	runOsdPoolLs = func() (string, error) { return f.poolLs, nil }
	runCrushCmp = func(_ string) (string, error) {
		panic("runCrushCmp should not be called in this benchmark")
	}
	runPgQuery = func(_ string) (string, error) {
		panic("runPgQuery should not be called in benchmarks")
	}
}

func makePoolJSON() string {
	return `[
 { "pool_id": 1, "pool_name": "replicated", "erasure_code_profile": "" }
]`
}

func makeOsdDumpJSON(osdCount int) string {
	var b strings.Builder
	b.Grow(osdCount * 40)
	b.WriteString("{\n  \"osds\": [\n")
	for i := range osdCount {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, "    { \"osd\": %d, \"in\": 1, \"up\": 1 }", i)
	}
	b.WriteString("\n  ],\n  \"pg_upmap_items\": []\n}\n")
	return b.String()
}

func makeOsdDumpJSONWithUpmaps(osdCount int, upmapCount int) string {
	if upmapCount < 0 {
		upmapCount = 0
	}

	var b strings.Builder
	b.Grow(osdCount*40 + upmapCount*80)
	b.WriteString("{\n  \"osds\": [\n")
	for i := range osdCount {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, "    { \"osd\": %d, \"in\": 1, \"up\": 1 }", i)
	}

	b.WriteString("\n  ],\n  \"pg_upmap_items\": [\n")
	for i := 0; i < upmapCount; i++ {
		if i > 0 {
			b.WriteString(",\n")
		}
		from := (i + 13) % osdCount
		to := (from + 5) % osdCount
		if to == from {
			to = (to + 1) % osdCount
		}
		fmt.Fprintf(
			&b,
			"    { \"pgid\": \"1.%x\", \"mappings\": [ { \"from\": %d, \"to\": %d } ] }",
			i,
			from,
			to,
		)
	}
	b.WriteString("\n  ]\n}\n")
	return b.String()
}

func makeOsdTreeJSON(osdCount int, osdsPerHost int) string {
	if osdsPerHost <= 0 {
		osdsPerHost = 8
	}
	hostCount := (osdCount + osdsPerHost - 1) / osdsPerHost

	var b strings.Builder
	b.Grow(osdCount * 50)
	b.WriteString("{\n  \"nodes\": [\n")
	b.WriteString("    { \"id\": -1, \"name\": \"default\", \"type\": \"root\", \"children\": [")
	for h := range hostCount {
		if h > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%d", -1000-h)
	}
	b.WriteString("] },\n")

	for h := range hostCount {
		if h > 0 {
			b.WriteString(",\n")
		}
		start := h * osdsPerHost
		end := min(start+osdsPerHost, osdCount)
		fmt.Fprintf(&b, "    { \"id\": %d, \"name\": \"host%d\", \"type\": \"host\", \"children\": [", -1000-h, h)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "%d", i)
		}
		b.WriteString("] }")
	}

	for i := range osdCount {
		b.WriteString(",\n")
		fmt.Fprintf(&b, "    { \"id\": %d, \"name\": \"osd.%d\", \"type\": \"osd\", \"reweight\": 1.0 }", i, i)
	}
	b.WriteString("\n  ]\n}\n")
	return b.String()
}

func makeBenchmarkPgStats(pgCount int, osdCount int) []*pgBriefItem {
	pgStats := make([]*pgBriefItem, 0, pgCount)
	for i := range pgCount {
		a0 := i % osdCount
		a1 := (i + 7) % osdCount
		a2 := (i + 13) % osdCount

		u0 := a0
		u1 := a1
		u2 := a2
		state := "active+clean"
		if i%3 == 0 {
			u2 = (a2 + 5) % osdCount
			if u2 == a0 || u2 == a1 {
				u2 = (u2 + 11) % osdCount
			}
			state = "active+remapped+backfill_wait"
		}

		pgStats = append(pgStats, &pgBriefItem{
			PgID:   fmt.Sprintf("1.%x", i),
			State:  state,
			Up:     []int{u0, u1, u2},
			Acting: []int{a0, a1, a2},
		})
	}

	return pgStats
}

func makePgDumpJSON(pgCount int, osdCount int) string {
	pgStats := makeBenchmarkPgStats(pgCount, osdCount)

	out, err := json.Marshal(pgBriefNautilus{PgStats: pgStats})
	if err != nil {
		panic(err)
	}
	return string(out)
}

func makePgDumpJSONLegacy(pgCount int, osdCount int) string {
	pgStats := makeBenchmarkPgStats(pgCount, osdCount)

	out, err := json.Marshal(pgStats)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func makeBenchmarkFixture(pgCount int, osdCount int) benchmarkFixture {
	return benchmarkFixture{
		pgDump:  makePgDumpJSON(pgCount, osdCount),
		osdDump: makeOsdDumpJSON(osdCount),
		osdTree: makeOsdTreeJSON(osdCount, 8),
		poolLs:  makePoolJSON(),
	}
}

func makeBenchmarkFixtureLegacy(pgCount int, osdCount int) benchmarkFixture {
	f := makeBenchmarkFixture(pgCount, osdCount)
	f.pgDump = makePgDumpJSONLegacy(pgCount, osdCount)
	return f
}

func makeBenchmarkFixtureWithUpmaps(pgCount int, osdCount int, upmapCount int) benchmarkFixture {
	if upmapCount > pgCount {
		upmapCount = pgCount
	}

	return benchmarkFixture{
		pgDump:  makePgDumpJSON(pgCount, osdCount),
		osdDump: makeOsdDumpJSONWithUpmaps(osdCount, upmapCount),
		osdTree: makeOsdTreeJSON(osdCount, 8),
		poolLs:  makePoolJSON(),
	}
}

func makeSyntheticImportMappings(count int, osdCount int) []pgMapping {
	out := make([]pgMapping, count)
	for i := range count {
		from := (i + 13) % osdCount
		to := (from + 17) % osdCount
		if to == from {
			to = (to + 1) % osdCount
		}
		out[i] = pgMapping{
			PgID: fmt.Sprintf("1.%x", i),
			Mapping: mapping{
				From: from,
				To:   to,
			},
		}
	}
	return out
}

func makeSyntheticCrushDiffOutput(pgCount int, osdCount int) string {
	var b strings.Builder
	b.Grow(pgCount * 60)
	b.WriteString("# synthetic crushdiff output\n")
	for i := range pgCount {
		a0 := i % osdCount
		a1 := (i + 7) % osdCount
		a2 := (i + 13) % osdCount
		n2 := (a2 + 5) % osdCount
		if n2 == a0 || n2 == a1 {
			n2 = (n2 + 11) % osdCount
		}
		fmt.Fprintf(&b, "1.%x\t[%d, %d, %d] -> [%d, %d, %d]\n", i, a0, a1, a2, a0, a1, n2)
	}
	return b.String()
}

func BenchmarkMustGetCurrentMappingState(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)

				_ = mustGetCurrentMappingState()
			}
		})
	}
}

func BenchmarkMustGetCurrentMappingStateLegacyJSON(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixtureLegacy(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)

				_ = mustGetCurrentMappingState()
			}
		})
	}
}

func BenchmarkCalcPgMappingsToUndoBackfill(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()

			excludedOsds := map[int]struct{}{}
			includedOsds := map[int]struct{}{}
			excludedPools := map[int]struct{}{}
			includedPools := map[int]struct{}{}
			pgsIncludingOsds := map[int]struct{}{}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				calcPgMappingsToUndoBackfill(
					true,
					false,
					false,
					excludedOsds,
					includedOsds,
					excludedPools,
					includedPools,
					pgsIncludingOsds,
				)
			}
		})
	}
}

func BenchmarkCalcPgMappingsToDrainOsd(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		targets := make(map[int]struct{})
		for i := 1; i < tt.osdCount && i < 64; i++ {
			targets[i] = struct{}{}
		}

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()
				M.bs.maxBackfillsFrom = 4096

				calcPgMappingsToDrainOsd("", []int{0}, targets)
			}
		})
	}
}

func BenchmarkCalcPgMappingsToUndoUpmaps(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)

		osds := make([]int, 0, 16)
		for i := 0; i < 16 && i < tt.osdCount; i++ {
			osds = append(osds, i)
		}

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()
				M.bs.maxBackfillsFrom = 4096

				calcPgMappingsToUndoUpmaps(osds, false)
			}
		})
	}
}

func BenchmarkCalcPgMappingsToBalanceOsds(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		osds := make([]int, 0, 32)
		for i := 0; i < 32 && i < tt.osdCount; i++ {
			osds = append(osds, i)
		}

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				calcPgMappingsToBalanceOsds(osds, 64, 1)
			}
		})
	}
}

func BenchmarkImportMappingsLoop(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)
		imports := makeSyntheticImportMappings(tt.upmapCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				for _, m := range imports {
					pui := M.findOrMakeUpmapItem(m.PgID)
					found := false
					for _, puiM := range pui.Mappings {
						if puiM.From == m.Mapping.From {
							M.mustRemap(m.PgID, puiM.To, m.Mapping.To)
							found = true
							break
						}
					}
					if !found {
						M.mustRemap(m.PgID, m.Mapping.From, m.Mapping.To)
					}
				}
			}
		})
	}
}

func BenchmarkExportMappings(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				filters := make([]mappingFilter, 0, 16)
				for osd := 0; osd < 8 && osd < tt.osdCount; osd++ {
					filters = append(filters, withFrom(osd), withTo(osd))
				}

				mappings := M.getMappings(mfOr(filters...))

				wholePgFilters := make([]mappingFilter, 0, len(mappings))
				for _, mp := range mappings {
					wholePgFilters = append(wholePgFilters, withPgid(mp.PgID))
				}
				_ = M.getMappings(mfOr(wholePgFilters...))
			}
		})
	}
}

func BenchmarkRemapSingle(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				pgb, ok := M.bs.pgbs["1.0"]
				if !ok || len(pgb.Up) == 0 {
					b.Fatalf("missing benchmark PG 1.0 or empty up set")
				}

				from := pgb.Up[i%len(pgb.Up)]
				to := (from + 17) % tt.osdCount
				for _, upOsd := range pgb.Up {
					if to == upOsd {
						to = (to + 1) % tt.osdCount
					}
				}
				if to == from {
					to = (to + 1) % tt.osdCount
				}

				M.mustRemap("1.0", from, to)
			}
		})
	}
}

func BenchmarkGenerateCrushChangeMappings(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		out := makeSyntheticCrushDiffOutput(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				runCrushCmp = func(_ string) (string, error) { return out, nil }

				_, _ = crushCmp("synthetic")
			}
		})
	}
}

func BenchmarkAccountForRemap(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		resetBenchmarkGlobals()
		installFixture(fixture)
		bs := mustGetCurrentBackfillState()
		pgb := bs.pgbs["1.1"]
		if pgb == nil || len(pgb.Up) == 0 {
			b.Fatalf("missing benchmark PG 1.1 or empty up set")
		}

		from := pgb.Up[0]
		to := (from + 17) % tt.osdCount
		for _, osd := range pgb.Up {
			if to == osd {
				to = (to + 1) % tt.osdCount
			}
		}

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bs.accountForRemap("1.1", from, to)
				bs.accountForRemap("1.1", to, from)
			}
		})
	}
}

func BenchmarkHasRoomForRemap(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		fixture := makeBenchmarkFixture(tt.pgCount, tt.osdCount)

		resetBenchmarkGlobals()
		installFixture(fixture)
		bs := mustGetCurrentBackfillState()
		pgb := bs.pgbs["1.2"]
		if pgb == nil || len(pgb.Up) == 0 {
			b.Fatalf("missing benchmark PG 1.2 or empty up set")
		}

		from := pgb.Up[0]
		to := (from + 19) % tt.osdCount
		for _, osd := range pgb.Up {
			if to == osd {
				to = (to + 1) % tt.osdCount
			}
		}
		bs.maxBackfillsFrom = 1 << 30

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = bs.hasRoomForRemap("1.2", from, to)
			}
		})
	}
}

func BenchmarkAddReservations(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		pgStats := makeBenchmarkPgStats(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bs := makeBackfillState()
				for _, pgb := range pgStats {
					bs.addReservations(pgb)
				}
			}
		})
	}
}

func BenchmarkRemoveReservations(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		pgStats := makeBenchmarkPgStats(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				bs := makeBackfillState()
				for _, pgb := range pgStats {
					bs.addReservations(pgb)
				}
				b.StartTimer()

				for _, pgb := range pgStats {
					bs.removeReservations(pgb)
				}
			}
		})
	}
}

func BenchmarkComputeBackfillSrcsTgts(b *testing.B) {
	pgb := &pgBriefItem{
		PgID:   "1.bench",
		State:  "active+remapped+backfill_wait",
		Up:     []int{1, 2, 33, 4, 55, 6},
		Acting: []int{1, 22, 3, 4, 5, 66},
	}
	srcBuf := make([]int, 0, len(pgb.Acting))
	tgtBuf := make([]int, 0, len(pgb.Acting))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = computeBackfillSrcsTgts(pgb, srcBuf[:0], tgtBuf[:0])
	}
}

func BenchmarkSanitizeStaleUpmaps(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				sanitizeStaleUpmaps(osdDump().PgUpmapItems)
			}
		})
	}
}

func BenchmarkGetMappingsFilterCompositions(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				filters := make([]mappingFilter, 0, 16)
				for osd := range 8 {
					filters = append(filters, withFrom(osd), withTo(osd))
				}

				_ = M.getMappings(mfOr(filters...))
			}
		})
	}
}

func BenchmarkTryRemapConflictAndUpdatePaths(b *testing.B) {
	for _, tt := range []struct {
		name       string
		pgCount    int
		osdCount   int
		upmapCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128, upmapCount: 300},
		{name: "medium", pgCount: 16384, osdCount: 512, upmapCount: 1200},
		{name: "large", pgCount: 65536, osdCount: 2048, upmapCount: 4800},
	} {
		fixture := makeBenchmarkFixtureWithUpmaps(tt.pgCount, tt.osdCount, tt.upmapCount)

		b.Run("update-existing/"+tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resetBenchmarkGlobals()
				installFixture(fixture)
				M = mustGetCurrentMappingState()

				_ = M.tryRemap("1.0", 18, 25)
			}
		})
	}

	b.Run("conflict", func(b *testing.B) {
		runOsdPoolLs = func() (string, error) {
			return `[{"pool_id":1,"pool_name":"replicated","erasure_code_profile":""}]`, nil
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resetBenchmarkGlobals()
			runOsdDump = func() (string, error) {
				return `{
					"pg_upmap_items": [
						{"pgid":"1.1","mappings":[{"from":3,"to":4},{"from":5,"to":6}]}
					]
				}`, nil
			}
			runPgDumpPgsBrief = func() (string, error) {
				return `[
					{"pgid":"1.1","state":"active+remapped","up":[4,6],"acting":[3,5]}
				]`, nil
			}

			M = mustGetCurrentMappingState()
			_ = M.tryRemap("1.1", 7, 4)
		}
	})
}

func BenchmarkParseCrushDiff(b *testing.B) {
	for _, tt := range []struct {
		name     string
		pgCount  int
		osdCount int
	}{
		{name: "small", pgCount: 4096, osdCount: 128},
		{name: "medium", pgCount: 16384, osdCount: 512},
		{name: "large", pgCount: 65536, osdCount: 2048},
	} {
		out := makeSyntheticCrushDiffOutput(tt.pgCount, tt.osdCount)

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = parseCrushDiff(out)
			}
		})
	}
}

func BenchmarkParsePGRemapEntry(b *testing.B) {
	line := "1.abc\t[1, 22, 333] -> [1, 44, 333]"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parsePGRemapEntry(line)
	}
}

func BenchmarkHandleCephInf(b *testing.B) {
	buf := []byte(`{"a": inf, "b":inf, "nested":{"x":1}, "arr":[1,2,3]}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = handleCephInf(buf)
	}
}
