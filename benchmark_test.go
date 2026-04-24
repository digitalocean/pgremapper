package main

import (
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
	for i := 0; i < osdCount; i++ {
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
	for i := 0; i < osdCount; i++ {
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
	for h := 0; h < hostCount; h++ {
		if h > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%d", -1000-h)
	}
	b.WriteString("] },\n")

	for h := 0; h < hostCount; h++ {
		if h > 0 {
			b.WriteString(",\n")
		}
		start := h * osdsPerHost
		end := start + osdsPerHost
		if end > osdCount {
			end = osdCount
		}
		fmt.Fprintf(&b, "    { \"id\": %d, \"name\": \"host%d\", \"type\": \"host\", \"children\": [", -1000-h, h)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "%d", i)
		}
		b.WriteString("] }")
	}

	for i := 0; i < osdCount; i++ {
		b.WriteString(",\n")
		fmt.Fprintf(&b, "    { \"id\": %d, \"name\": \"osd.%d\", \"type\": \"osd\", \"reweight\": 1.0 }", i, i)
	}
	b.WriteString("\n  ]\n}\n")
	return b.String()
}

func makePgDumpJSON(pgCount int, osdCount int) string {
	var b strings.Builder
	b.Grow(pgCount * 130)
	b.WriteString("[\n")
	for i := 0; i < pgCount; i++ {
		if i > 0 {
			b.WriteString(",\n")
		}

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

		fmt.Fprintf(
			&b,
			" { \"pgid\": \"1.%x\", \"up\": [ %d, %d, %d ], \"acting\": [ %d, %d, %d ], \"state\": \"%s\" }",
			i,
			u0, u1, u2,
			a0, a1, a2,
			state,
		)
	}
	b.WriteString("\n]\n")
	return b.String()
}

func makeBenchmarkFixture(pgCount int, osdCount int) benchmarkFixture {
	return benchmarkFixture{
		pgDump:  makePgDumpJSON(pgCount, osdCount),
		osdDump: makeOsdDumpJSON(osdCount),
		osdTree: makeOsdTreeJSON(osdCount, 8),
		poolLs:  makePoolJSON(),
	}
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
	out := make([]pgMapping, 0, count)
	for i := 0; i < count; i++ {
		from := (i + 13) % osdCount
		to := (from + 17) % osdCount
		if to == from {
			to = (to + 1) % osdCount
		}
		out = append(out, pgMapping{
			PgID: fmt.Sprintf("1.%x", i),
			Mapping: mapping{
				From: from,
				To:   to,
			},
		})
	}
	return out
}

func makeSyntheticCrushDiffOutput(pgCount int, osdCount int) string {
	var b strings.Builder
	b.Grow(pgCount * 60)
	b.WriteString("# synthetic crushdiff output\n")
	for i := 0; i < pgCount; i++ {
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
