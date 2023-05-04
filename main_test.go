// Copyright 2021 DigitalOcean
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
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCalcPgMappingsToUndoBackfill(t *testing.T) {
	// Corner-case PG states included to ensure we handle them gracefully:
	// * 1.999[01]: up and acting sets have different lengths
	// * 1.999[23]: up/acting set has duplicate OSDs
	pgDumpOut := `
[
 { "pgid": "1.32", "up": [ 7, 5, 9], "acting": [ 7, 5, 9 ] },
 { "pgid": "1.33", "up": [ 6, 0, 10], "acting": [ 6, 10, 2], "state": "backfill_wait" },
 { "pgid": "1.45", "up": [ 6, 3, 2], "acting": [ 6, 3, 2], "state": "backfill_wait" },
 { "pgid": "1.46", "up": [ 4, 6, 0], "acting": [ 4, 6, 1], "state": "backfill_wait" },
 { "pgid": "1.47", "up": [ 0, 11, 4], "acting": [ 2, 11, 4], "state": "backfill_wait" },
 { "pgid": "1.89", "up": [ 10, 2, 8], "acting": [ 10, 2, 8] },
 { "pgid": "1.8a", "up": [ 3, 7, 0], "acting": [ 3, 7, 1], "state": "backfill_wait" },
 { "pgid": "1.8b", "up": [ 3, 6, 0], "acting": [ 3, 7, 1], "state": "backfill_wait" },
 { "pgid": "1.8c", "up": [ 3, 6, 0], "acting": [ 1, 2147483647, 3 ],
   "state": "active+undersized+degraded+remapped+backfill_wait" },
 { "pgid": "1.8d", "up": [ 3, 6, 0], "acting": [ 3, 7, 1 ],
   "state": "active+remapped+backfilling" },
 { "pgid": "1.8e", "up": [ 23, 26, 20], "acting": [ 23, 27, 21 ], "state": "backfill_wait" },
 { "pgid": "1.8f", "up": [ 33, 36, 30], "acting": [ 33, 37, 31 ], "state": "backfill_wait" },
 { "pgid": "1.90", "up": [ 33, 36, 30], "acting": [ 33, 37, 31 ], "state": "backfill_wait" },
 { "pgid": "1.91", "up": [ 33, 36, 30], "acting": [ 33, 37, 2147483647 ], "state": "backfill_wait" },
 { "pgid": "1.92", "up": [ 3, 6, 1], "acting": [ 1, 2147483647, 3 ], "state": "backfill_wait" },
 { "pgid": "1.93", "up": [ 1, 4, 5], "acting": [ 1, 2, 3 ], "state": "backfill_wait" },

 { "pgid": "1.9990", "up": [ 1 ], "acting": [ 1, 2, 3 ], "state": "backfill_wait" },
 { "pgid": "1.9991", "up": [ 1, 2, 3 ], "acting": [ 1 ], "state": "backfill_wait" },
 { "pgid": "1.9992", "up": [ 1, 2, 3 ], "acting": [ 1, 4, 4 ], "state": "backfill_wait" },
 { "pgid": "1.9993", "up": [ 1, 4, 4 ], "acting": [ 1, 2, 3 ], "state": "backfill_wait" }
]
`
	// PG 1.33 has a stale and invalid upmap entry like we've seen left
	// behind by Ceph sometimes - both its from and to are in the up set.
	osdDumpOut := `
{
  "pg_upmap_items": [
    { "pgid": "1.33", "mappings": [ { "from": 0, "to": 10 } ] },
    { "pgid": "1.8f", "mappings": [ { "from": 37, "to": 36 } ] },
    { "pgid": "1.90", "mappings": [ { "from": 37, "to": 36 }, { "from": 31, "to": 30 } ] },
    { "pgid": "1.93", "mappings": [ { "from": 3, "to": 4 }, { "from": 2, "to": 5 } ] }
  ]
}
`

	runOsdDump = func() (string, error) { return osdDumpOut, nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	runPgQuery = func(pgid string) (string, error) {
		switch pgid {
		case "1.8c":
			// Replicated case.
			return `
{
  "acting": [ 1, 2147483647, 3 ],
  "info": { "pgid": "1.8c" },
  "peer_info": [
    { "peer": "1",  "incomplete": 0 },
    { "peer": "3",  "incomplete": 0 },
    { "peer": "6",  "incomplete": 1 },
    { "peer": "10", "incomplete": 0 }
  ]
}
		    `, nil
		case "1.91":
			// EC case.
			return `
{
  "acting": [ 33, 37, 2147483647 ],
  "info": { "pgid": "1.91" },
  "peer_info": [
    { "peer": "37(1)", "incomplete": 0, "stats": {"last_epoch_clean": 101} },
    { "peer": "36(1)", "incomplete": 1, "stats": {"last_epoch_clean": 100} },
    { "peer": "33(0)", "incomplete": 0, "stats": {"last_epoch_clean": 100} },
    { "peer": "30(2)", "incomplete": 1, "stats": {"last_epoch_clean": 100} },
    { "peer": "38(2)", "incomplete": 0, "stats": {"last_epoch_clean": 101} },
    { "peer": "39(2)", "incomplete": 0, "stats": {"last_epoch_clean": 99} }
  ]
}
		    `, nil
		case "1.92":
			// We're missing a replica.
			return `
{
  "acting": [ 1, 2147483647, 3 ],
  "info": { "pgid": "1.92" },
  "peer_info": [
    { "peer": "1", "incomplete": 0 },
    { "peer": "3", "incomplete": 0 },
    { "peer": "6", "incomplete": 1 }
  ]
}
		    `, nil
		default:
			return "", fmt.Errorf("unhandled pgid %s", pgid)
		}
	}

	tests := []struct {
		name         string
		source       bool
		target       bool
		exclude      []int
		include      []int
		pgsIncluding []int
		expected     []expectedMapping
	}{
		{
			name:    "with exclude specified",
			source:  false,
			target:  false,
			exclude: []int{21, 26},
			include: []int{},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 6, To: 7, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 6, To: 10, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8f", Mappings: []mapping{{From: 30, To: 31, dirty: true}}},
				{ID: "1.90", Mappings: []mapping{}},
				{ID: "1.91", Mappings: []mapping{{From: 36, To: 37, dirty: true}, {From: 30, To: 38, dirty: true}}},
				{ID: "1.93", Mappings: []mapping{}},
			},
		},
		{
			name:    "with exclude, source, and target specified",
			source:  true,
			target:  true,
			exclude: []int{21, 26},
			include: []int{},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 6, To: 7, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 6, To: 10, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8f", Mappings: []mapping{{From: 30, To: 31, dirty: true}}},
				{ID: "1.90", Mappings: []mapping{}},
				{ID: "1.91", Mappings: []mapping{{From: 36, To: 37, dirty: true}, {From: 30, To: 38, dirty: true}}},
				{ID: "1.93", Mappings: []mapping{}},
			},
		},
		{
			name:    "with exclude and source specified",
			source:  true,
			target:  false,
			exclude: []int{21, 26},
			include: []int{},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 6, To: 7, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 6, To: 10, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8e", Mappings: []mapping{{From: 20, To: 21, dirty: true}}},
				{ID: "1.8f", Mappings: []mapping{{From: 30, To: 31, dirty: true}}},
				{ID: "1.90", Mappings: []mapping{}},
				{ID: "1.91", Mappings: []mapping{{From: 36, To: 37, dirty: true}, {From: 30, To: 38, dirty: true}}},
				{ID: "1.93", Mappings: []mapping{}},
			},
		},
		{
			name:    "with exclude and target specified",
			source:  false,
			target:  true,
			exclude: []int{21, 26},
			include: []int{},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 6, To: 7, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 6, To: 10, dirty: true}, {From: 0, To: 1, dirty: true}}},
				{ID: "1.8e", Mappings: []mapping{{From: 26, To: 27, dirty: true}}},
				{ID: "1.8f", Mappings: []mapping{{From: 30, To: 31, dirty: true}}},
				{ID: "1.90", Mappings: []mapping{}},
				{ID: "1.91", Mappings: []mapping{{From: 36, To: 37, dirty: true}, {From: 30, To: 38, dirty: true}}},
				{ID: "1.93", Mappings: []mapping{}},
			},
		},
		{
			name:    "with include specified",
			source:  false,
			target:  false,
			exclude: []int{},
			include: []int{0, 26},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8e", Mappings: []mapping{{From: 26, To: 27, dirty: true}}},
			},
		},
		{
			name:    "with include and target specified",
			source:  false,
			target:  true,
			exclude: []int{},
			include: []int{1, 26},
			expected: []expectedMapping{
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
			},
		},
		{
			name:    "with include and source specified",
			source:  true,
			target:  false,
			exclude: []int{},
			include: []int{0, 26},
			expected: []expectedMapping{
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.47", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8e", Mappings: []mapping{{From: 26, To: 27, dirty: true}}},
			},
		},
		{
			name:    "with exclude and include specified",
			source:  false,
			target:  false,
			exclude: []int{2},
			include: []int{0, 26},
			expected: []expectedMapping{
				{ID: "1.46", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8a", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8b", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8c", Mappings: []mapping{{From: 0, To: 1, dirty: true}}},
				{ID: "1.8e", Mappings: []mapping{{From: 26, To: 27, dirty: true}}},
			},
		},
		{
			name:         "with pgs-including specified",
			pgsIncluding: []int{26},
			expected: []expectedMapping{
				{ID: "1.8e", Mappings: []mapping{{From: 26, To: 27, dirty: true}, {From: 20, To: 21, dirty: true}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer teardownTest(t)
			M = mustGetCurrentMappingState()

			source := tt.source
			target := tt.target
			excludeOsds := make(map[int]struct{})
			includeOsds := make(map[int]struct{})
			pgsIncludingOsds := make(map[int]struct{})

			for _, v := range tt.exclude {
				excludeOsds[v] = struct{}{}
			}

			for _, v := range tt.include {
				includeOsds[v] = struct{}{}
			}

			for _, v := range tt.pgsIncluding {
				pgsIncludingOsds[v] = struct{}{}
			}

			calcPgMappingsToUndoBackfill(true, source, target, excludeOsds, includeOsds, pgsIncludingOsds)

			validateDirtyMappings(t, tt.expected)
		})
	}
}

func TestCountCurrentBackfills(t *testing.T) {
	defer teardownTest(t)
	out := `
[
 { "pgid": "1.32", "up": [ 7, 5, 9], "acting": [ 7, 5, 9 ] },
 { "pgid": "1.33", "up": [ 6, 0, 10], "acting": [ 6, 10, 2 ] },
 { "pgid": "1.45", "up": [ 6, 3, 2], "acting": [ 6, 3, 2 ] },
 { "pgid": "1.46", "up": [ 4, 6, 0], "acting": [ 4, 6, 1 ] },
 { "pgid": "1.47", "up": [ 0, 11, 4], "acting": [ 2, 11, 4 ] },
 { "pgid": "1.89", "up": [ 10, 2, 8], "acting": [ 10, 2, 8 ] },
 { "pgid": "1.8a", "up": [ 3, 7, 0], "acting": [ 3, 7, 1 ] },
 { "pgid": "1.8b", "up": [ 3, 6, 0], "acting": [ 3, 7, 1 ] },
 { "pgid": "1.8c", "up": [ 3, 6, 0], "acting": [ 3, 7, 1 ],
   "state": "active+recovery_wait+degraded+remapped" }
]
`

	runPgDumpPgsBrief = func() (string, error) { return out, nil }

	expectedSourceBackfillCounts := map[int]int{
		1: 4,
		2: 2,
		7: 2,
	}
	expectedTargetBackfillCounts := map[int]int{
		0: 6,
		6: 2,
	}

	sourceBackfillCounts, targetBackfillCounts := countCurrentBackfills()
	testEqual := func(expected, actual map[int]int) {
		require.Len(t, actual, len(expected))
		for k, v := range expected {
			require.Contains(t, actual, k)
			require.Equal(t, v, actual[k])
		}
	}
	testEqual(expectedSourceBackfillCounts, sourceBackfillCounts)
	testEqual(expectedTargetBackfillCounts, targetBackfillCounts)
}

func TestCalcPgMappingsToUndoUpmaps(t *testing.T) {
	// Need an entry for each PG that will have mappings affected. Other
	// than that, we want to fake backfills such that:
	// sourceBackfillCounts := map[int]int{
	// 	1: 4,
	// 	2: 1,
	// 	3: 1,
	// 	5: 2,
	// }
	// targetBackfillCounts := map[int]int{
	// 	0: 6,
	// 	6: 2,
	// 	8: 2,
	// }
	pgDumpOut := `
[
 { "pgid": "1.33", "up": [ 100, 2, 3 ], "acting": [ 100, 2, 3 ] },
 { "pgid": "1.34", "up": [ 100, 2, 3 ], "acting": [ 100, 2, 3 ] },
 { "pgid": "1.46", "up": [ 100, 1, 3 ], "acting": [ 100, 1, 3 ] },
 { "pgid": "1.47", "up": [ 100, 2, 3 ], "acting": [ 100, 2, 3 ] },
 { "pgid": "1.48", "up": [ 101, 2, 3 ], "acting": [ 101, 2, 3 ] },
 { "pgid": "1.8a", "up": [ 102, 7, 1 ], "acting": [ 102, 7, 1 ] },
 { "pgid": "1.8b", "up": [ 102, 7, 9 ], "acting": [ 102, 7, 9 ] },
 { "pgid": "1.8c", "up": [ 102, 7, 5 ], "acting": [ 102, 7, 5 ] },
 { "pgid": "1.8d", "up": [ 103, 2, 5 ], "acting": [ 103, 2, 5 ] },

 { "pgid": "1.100", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 1 ] },
 { "pgid": "1.101", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 1 ] },
 { "pgid": "1.102", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 1 ] },
 { "pgid": "1.103", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 1 ] },
 { "pgid": "1.104", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 2 ] },
 { "pgid": "1.105", "up": [ 998, 999, 0 ], "acting": [ 998, 999, 3 ] },
 { "pgid": "1.106", "up": [ 998, 999, 6 ], "acting": [ 998, 999, 5 ] },
 { "pgid": "1.107", "up": [ 998, 999, 6 ], "acting": [ 998, 999, 5 ] },
 { "pgid": "1.108", "up": [ 998, 999, 8 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.109", "up": [ 998, 999, 8 ], "acting": [ 998, 999, 1000 ] }
]
`
	osdDumpOut := `
		{
		  "pg_upmap_items": [
		    { "pgid": "1.33", "mappings": [ { "from": 0, "to": 2 } ] },
		    { "pgid": "1.34", "mappings": [ { "from": 0, "to": 3 } ] },
		    { "pgid": "1.46", "mappings": [ { "from": 0, "to": 1 } ] },
		    { "pgid": "1.47", "mappings": [ { "from": 0, "to": 2 } ] },
		    { "pgid": "1.48", "mappings": [ { "from": 6, "to": 2 } ] },
		    { "pgid": "1.8a", "mappings": [ { "from": 0, "to": 1 } ] },
		    { "pgid": "1.8b", "mappings": [ { "from": 1, "to": 7 }, { "from": 0, "to": 9 } ] },
		    { "pgid": "1.8c", "mappings": [ { "from": 6, "to": 5 } ] },
		    { "pgid": "1.8d", "mappings": [ { "from": 8, "to": 5 } ] }
		  ]
		}
		`

	runOsdDump = func() (string, error) { return osdDumpOut, nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	t.Run("source OSDs specified", func(t *testing.T) {
		defer teardownTest(t)
		maxSourceBackfills := 3
		sourceOsds := []int{1, 2, 5, 7}
		expected := []expectedMapping{
			{ID: "1.33", Mappings: nil},
			{ID: "1.48", Mappings: nil},
			{ID: "1.8b", Mappings: []mapping{{From: 0, To: 9}}},
			{ID: "1.8d", Mappings: nil},
		}

		M = mustGetCurrentMappingState()
		M.bs.maxBackfillsFrom = maxSourceBackfills
		calcPgMappingsToUndoUpmaps(sourceOsds, false)

		validateDirtyMappings(t, expected)
	})

	t.Run("target OSDs specified", func(t *testing.T) {
		defer teardownTest(t)
		maxSourceBackfills := 2
		targetOsds := []int{1, 6}
		expected := []expectedMapping{
			{ID: "1.48", Mappings: nil},
			{ID: "1.8b", Mappings: []mapping{{From: 0, To: 9}}},
		}

		M = mustGetCurrentMappingState()
		M.bs.maxBackfillsFrom = maxSourceBackfills
		calcPgMappingsToUndoUpmaps(targetOsds, true)

		validateDirtyMappings(t, expected)
	})

	t.Run("max-backfills specified", func(t *testing.T) {
		defer teardownTest(t)
		targetOsds := []int{0}
		expected := []expectedMapping{
			{ID: "1.33", Mappings: nil},
			{ID: "1.34", Mappings: nil},
			{ID: "1.8a", Mappings: nil},
		}

		M = mustGetCurrentMappingState()
		M.bs.maxBackfillReservations = 9
		M.bs.osd(100).maxBackfillReservations = 2
		calcPgMappingsToUndoUpmaps(targetOsds, true)

		validateDirtyMappings(t, expected)
	})
}

func TestCalcPgMappingsToBalanceHost(t *testing.T) {
	// Initial state:
	// 0: 1.1, 1.2, 1.3, 1.4 (-> 1), 1.5
	// 1: 1.6, 1.7, 1.8 (plus 1.4 coming from 0)
	// 2: 1.9, 1.10
	// 3: 1.11, 1.12, 1.13, 1.14
	// 4: 1.15
	pgDumpOut := `
[
 { "pgid": "1.1", "up": [ 0 ], "acting": [ 0 ] },
 { "pgid": "1.2", "up": [ 0 ], "acting": [ 0 ] },
 { "pgid": "1.3", "up": [ 0 ], "acting": [ 0 ] },
 { "pgid": "1.4", "up": [ 1 ], "acting": [ 0 ] },
 { "pgid": "1.5", "up": [ 0 ], "acting": [ 0 ] },
 { "pgid": "1.6", "up": [ 1 ], "acting": [ 1 ] },
 { "pgid": "1.7", "up": [ 1 ], "acting": [ 1 ] },
 { "pgid": "1.8", "up": [ 1 ], "acting": [ 1 ] },
 { "pgid": "1.9", "up": [ 2 ], "acting": [ 2 ] },
 { "pgid": "1.10", "up": [ 2 ], "acting": [ 2 ] },
 { "pgid": "1.11", "up": [ 3 ], "acting": [ 3 ] },
 { "pgid": "1.12", "up": [ 3 ], "acting": [ 3 ] },
 { "pgid": "1.13", "up": [ 3 ], "acting": [ 3 ] },
 { "pgid": "1.14", "up": [ 3 ], "acting": [ 3 ] },
 { "pgid": "1.15", "up": [ 4 ], "acting": [ 4 ] }
]
`

	osdDumpOut := `
{
  "osds": [
    { "osd": 0, "in": 1, "up": 1 },
    { "osd": 1, "in": 1, "up": 1 },
    { "osd": 2, "in": 1, "up": 1 },
    { "osd": 3, "in": 1, "up": 1 },
    { "osd": 4, "in": 1, "up": 1 },
    { "osd": 5, "in": 0, "up": 1 }
  ],
  "pg_upmap_items": [
    { "pgid": "1.4", "mappings": [ { "from": 0, "to": 1 } ] },
    { "pgid": "1.5", "mappings": [ { "from": 2, "to": 0 } ] }
  ]
}
`

	runOsdDump = func() (string, error) { return osdDumpOut, nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	tests := []struct {
		name         string
		maxBackfills int
		targetSpread int
		expected     []expectedMapping
	}{
		{
			name:         "fully balance",
			maxBackfills: 4,
			targetSpread: 0,
			expected: []expectedMapping{
				{ID: "1.14", Mappings: []mapping{{From: 3, To: 4, dirty: true}}},
				{ID: "1.5", Mappings: []mapping{{From: 2, To: 4, dirty: true}}},
				{ID: "1.8", Mappings: []mapping{{From: 1, To: 2, dirty: true}}},
			},
		},
		{
			name:         "no balance due to outstanding backfill",
			maxBackfills: 1,
			targetSpread: 0,
			expected:     []expectedMapping{},
		},
		{
			name:         "single movement",
			maxBackfills: 2,
			targetSpread: 0,
			expected: []expectedMapping{
				{ID: "1.5", Mappings: []mapping{{From: 2, To: 4, dirty: true}}},
			},
		},
		{
			name:         "increased target spread",
			maxBackfills: 4,
			targetSpread: 2,
			expected: []expectedMapping{
				{ID: "1.5", Mappings: []mapping{{From: 2, To: 4, dirty: true}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer teardownTest(t)
			M = mustGetCurrentMappingState()

			calcPgMappingsToBalanceOsds(
				[]int{0, 1, 2, 3, 4, 5},
				tt.maxBackfills,
				tt.targetSpread,
			)

			validateDirtyMappings(t, tt.expected)
		})
	}
}

func TestCalcPgMappingsToDrainOsd(t *testing.T) {
	osdDumpOut := `
{
  "pg_upmap_items": [
  ]
}
`

	// 3 racks, 2 hosts/rack, 4 osds/host
	osdTreeOut := `
{
  "nodes": [
    {
      "children": [ -1, -2, -3 ],
      "type": "root",
      "name": "root1",
      "id": -999
    },
    {
      "children": [ -4, -5 ],
      "type": "rack",
      "name": "rack1",
      "id": -1
    },
    {
      "children": [ 0, 1, 2, 3 ],
      "type": "host",
      "name": "host1",
      "id": -4
    },
    { "type": "osd", "name": "osd.0", "id": 0 },
    { "type": "osd", "name": "osd.1", "id": 1 },
    { "type": "osd", "name": "osd.2", "id": 2 },
    { "type": "osd", "name": "osd.3", "id": 3 },
    {
      "children": [ 4, 5, 6, 7 ],
      "type": "host",
      "name": "host2",
      "id": -5
    },
    { "type": "osd", "name": "osd.4", "id": 4 },
    { "type": "osd", "name": "osd.5", "id": 5 },
    { "type": "osd", "name": "osd.6", "id": 6 },
    { "type": "osd", "name": "osd.7", "id": 7 },
    {
      "children": [ -6, -7 ],
      "type": "rack",
      "name": "rack2",
      "id": -2
    },
    {
      "children": [ 8, 9, 10, 11 ],
      "type": "host",
      "name": "host3",
      "id": -6
    },
    { "type": "osd", "name": "osd.8", "id": 8 },
    { "type": "osd", "name": "osd.9", "id": 9 },
    { "type": "osd", "name": "osd.10", "id": 10 },
    { "type": "osd", "name": "osd.11", "id": 11 },
    {
      "children": [ 12, 13, 14, 15 ],
      "type": "host",
      "name": "host4",
      "id": -7
    },
    { "type": "osd", "name": "osd.12", "id": 12 },
    { "type": "osd", "name": "osd.13", "id": 13 },
    { "type": "osd", "name": "osd.14", "id": 14 },
    { "type": "osd", "name": "osd.15", "id": 15 },
    {
      "children": [ -8, -9 ],
      "type": "rack",
      "name": "rack3",
      "id": -3
    },
    {
      "children": [ 16, 17, 18, 19 ],
      "type": "host",
      "name": "host3",
      "id": -8
    },
    { "type": "osd", "name": "osd.16", "id": 16 },
    { "type": "osd", "name": "osd.17", "id": 17 },
    { "type": "osd", "name": "osd.18", "id": 18 },
    { "type": "osd", "name": "osd.19", "id": 19 },
    {
      "children": [ 20, 21, 22, 23 ],
      "type": "host",
      "name": "host4",
      "id": -9
    },
    { "type": "osd", "name": "osd.20", "id": 20 },
    { "type": "osd", "name": "osd.21", "id": 21 },
    { "type": "osd", "name": "osd.22", "id": 22 },
    { "type": "osd", "name": "osd.23", "id": 23 }
  ]
}
`

	// Need an entry for each PG that will have mappings affected. Other
	// than that, we want to fake backfills such that:
	// sourceBackfillCounts := map[int]int{
	// 	0: 2,
	// }
	// targetBackfillCounts := map[int]int{
	// 	1: 6,
	// 	2: 2,
	// 	3: 3,
	// 	5: 2,

	// 	// Leave these at 0 so that they would be
	// 	// preferred if allowMovementAcrossCrushType
	// 	// and targetOsds allows.
	// 	4:  0,
	// 	8:  0,
	// 	12: 0,
	// 	16: 0,
	// }
	pgDumpOut := `
[
 { "pgid": "1.32", "up": [ 0, 8, 16 ], "acting": [ 0, 8, 16 ] },
 { "pgid": "1.33", "up": [ 0, 5, 16 ], "acting": [ 0, 5, 16 ] },
 { "pgid": "1.34", "up": [ 0, 5, 16 ], "acting": [ 0, 5, 16 ] },
 { "pgid": "1.35", "up": [ 0, 8, 16 ], "acting": [ 0, 8, 16 ] },

 { "pgid": "1.100", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 0 ] },
 { "pgid": "1.101", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 0 ] },
 { "pgid": "1.102", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.103", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.104", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.105", "up": [ 998, 999, 1 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.106", "up": [ 998, 999, 2 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.107", "up": [ 998, 999, 2 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.108", "up": [ 998, 999, 3 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.109", "up": [ 998, 999, 3 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.110", "up": [ 998, 999, 3 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.111", "up": [ 998, 999, 5 ], "acting": [ 998, 999, 1000 ] },
 { "pgid": "1.112", "up": [ 998, 999, 5 ], "acting": [ 998, 999, 1000 ] }
]
`

	runOsdDump = func() (string, error) { return osdDumpOut, nil }
	runOsdTree = func() (string, error) { return osdTreeOut, nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	sourceOsd := 0
	maxSourceBackfills := 5

	tests := []struct {
		name                         string
		allowMovementAcrossCrushType string
		targetOsds                   []int
		expected                     []expectedMapping
	}{
		{
			name:                         "movements stay in host (default)",
			allowMovementAcrossCrushType: "",
			targetOsds:                   []int{1, 2, 3, 4, 8, 12, 16},
			expected: []expectedMapping{
				{ID: "1.32", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.34", Mappings: []mapping{{From: 0, To: 3, dirty: true}}},
			},
		},
		{
			name:                         "movement allowed across hosts",
			allowMovementAcrossCrushType: "host",
			targetOsds:                   []int{1, 2, 3, 5, 8, 12, 16},
			expected: []expectedMapping{
				{ID: "1.32", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.35", Mappings: []mapping{{From: 0, To: 5, dirty: true}}},
			},
		},
		// Movements allowed across racks - weird case enabled by PGs
		// 33 and 34 having two copies in the same rack currently.
		{
			name:                         "movement allowed across racks",
			allowMovementAcrossCrushType: "rack",
			targetOsds:                   []int{1, 2, 3, 5, 8, 12, 16},
			expected: []expectedMapping{
				{ID: "1.32", Mappings: []mapping{{From: 0, To: 2, dirty: true}}},
				{ID: "1.33", Mappings: []mapping{{From: 0, To: 8, dirty: true}}},
				{ID: "1.34", Mappings: []mapping{{From: 0, To: 12, dirty: true}}},
			},
		},

		{
			name:                         "movements stay in host - no candidates",
			allowMovementAcrossCrushType: "",
			targetOsds:                   []int{4, 8, 12, 16},
			expected:                     []expectedMapping{},
		},
		{
			name:                         "movement allowed across hosts - no candidates",
			allowMovementAcrossCrushType: "host",
			targetOsds:                   []int{8, 12, 16},
			expected:                     []expectedMapping{},
		},
		{
			name:                         "movement allowed across racks - no candidates",
			allowMovementAcrossCrushType: "rack",
			targetOsds:                   []int{16},
			expected:                     []expectedMapping{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer teardownTest(t)
			M = mustGetCurrentMappingState()
			M.bs.maxBackfillsFrom = maxSourceBackfills
			calcPgMappingsToDrainOsd(
				tt.allowMovementAcrossCrushType,
				sourceOsd,
				tt.targetOsds,
			)

			validateDirtyMappings(t, tt.expected)
		})
	}
}

type expectedMapping struct {
	ID       string
	Mappings []mapping
}

func validateDirtyMappings(t *testing.T, expected []expectedMapping) {
	puis := M.dirtyUpmapItems()
	require.Len(t, puis, len(expected))

	for i := range puis {
		require.Equal(t, expected[i].ID, puis[i].PgID)
		require.ElementsMatchf(t, expected[i].Mappings, puis[i].Mappings, "pg %s expected: %v got %v", expected[i].ID, expected[i].Mappings, puis[i].Mappings)
	}
}

func TestParseMaxBackfillReservations(t *testing.T) {
	defer teardownTest(t)
	osdTreeOut := `
{
  "nodes": [
    {
      "children": [ 0, 1, 2 ],
      "type": "host",
      "name": "host1",
      "id": -4
    },
    { "type": "osd", "name": "osd.0", "id": 0, "reweight": 0.123 },
    { "type": "osd", "name": "osd.1", "id": 1, "reweight": 1.00000 },
    { "type": "osd", "name": "osd.2", "id": 2, "reweight": 0 }
  ]
}
`
	runOsdTree = func() (string, error) { return osdTreeOut, nil }

	cmd := &cobra.Command{}
	cmd.Flags().StringSlice("max-backfill-reservations", []string{"4", "bucket:host1:10", "133:6"}, "")

	M = mustGetCurrentMappingState()
	mustParseMaxBackfillReservations(cmd)

	require.Equal(t, 10, M.bs.getMaxBackfillReservations(1))
	// 'out' OSDs are excluded from osdspecs.
	require.Equal(t, 4, M.bs.getMaxBackfillReservations(2))
	require.Equal(t, 6, M.bs.getMaxBackfillReservations(133))
}

func TestDeviceClassFilter(t *testing.T) {
	defer teardownTest(t)
	osdTreeOut := `
	{
		"nodes": [
		  { "id": -1, "name": "default", "type": "root", "children": [-4] },
		  { "id": -4, "name": "datacenter1", "type": "datacenter", "children": [-3] },
		  { "id": -3, "name": "rack1", "type": "rack", "children": [-31, -6, -5, -2] },
		  { "id": -2, "name": "host1", "type": "host", "children": [2, 1, 0] },
		  { "id": 0, "device_class": "green", "name": "osd.0", "type": "osd", "reweight": 1 },
		  { "id": 1, "device_class": "red", "name": "osd.1", "type": "osd", "reweight": 1 },
		  { "id": 2, "device_class": "blue", "name": "osd.2", "type": "osd", "reweight": 1 },
		  { "id": -5, "name": "host2", "type": "host", "children": [6, 4, 3] },
		  { "id": 3, "device_class": "green", "name": "osd.3", "type": "osd", "reweight": 1 },
		  { "id": 4, "device_class": "red", "name": "osd.4", "type": "osd", "reweight": 1 },
		  { "id": 6, "device_class": "blue", "name": "osd.6", "type": "osd", "reweight": 1 },
		  { "id": -6, "name": "host3", "type": "host", "children": [8, 7, 5] },
		  { "id": 5, "device_class": "green", "name": "osd.5", "type": "osd", "reweight": 1 },
		  { "id": 7, "device_class": "red", "name": "osd.7", "type": "osd", "reweight": 1 },
		  { "id": 8, "device_class": "blue", "name": "osd.8", "type": "osd", "reweight": 1 },
		  { "id": -31, "name": "host4", "type": "host", "children": [11, 10, 9] },
		  { "id": 9, "device_class": "green", "name": "osd.9", "type": "osd", "reweight": 1 },
		  { "id": 10, "device_class": "red", "name": "osd.10", "type": "osd", "reweight": 1 },
		  { "id": 11, "device_class": "blue", "name": "osd.11", "type": "osd", "reweight": 1 }
	  ]
	}
`
	runOsdTree = func() (string, error) { return osdTreeOut, nil }

	require.ElementsMatch(t, mustGetOsdsForBucket("rack1", "red"),
		[]int{1, 4, 7, 10})
	require.ElementsMatch(t, mustGetOsdsForBucket("rack1", "blue"),
		[]int{2, 6, 8, 11})
	require.ElementsMatch(t, mustGetOsdsForBucket("host1", "green"),
		[]int{0})
	require.ElementsMatch(t, mustGetOsdsForBucket("host4", ""),
		[]int{9, 10, 11})
}

func teardownTest(t *testing.T) {
	savedOsdDumpOut = nil
	savedOsdPoolsDetails = nil
	savedParsedOsdTree = nil
	savedPgDumpPgsBrief = nil
}
