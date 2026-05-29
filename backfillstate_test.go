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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackfillState(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })
	pgDumpOut := `
[
 { "pgid": "1.01", "up": [ 77, 1, 2 ], "acting": [ 77, 1, 2 ] },
 { "pgid": "1.02", "up": [ 77, 3, 4 ], "acting": [ 77, 3, 5 ] },
 { "pgid": "1.03", "up": [ 77, 5, 6 ], "acting": [ 3, 5, 7 ] },
 { "pgid": "1.04", "up": [ 8, 5, 6 ],  "acting": [ 77, 5, 7 ] }
]
`
	runOsdDump = func() (string, error) { return "{}", nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	bs := mustGetCurrentBackfillState()

	// Check initial state.
	require.Equal(t, 1, bs.osd(3).localReservations)
	require.Equal(t, 0, bs.osd(3).remoteReservations)
	require.Equal(t, 1, bs.osd(3).backfillsFrom)
	require.Equal(t, 0, bs.osd(4).localReservations)
	require.Equal(t, 1, bs.osd(4).remoteReservations)
	require.Equal(t, 0, bs.osd(4).backfillsFrom)
	require.Equal(t, 0, bs.osd(5).localReservations)
	require.Equal(t, 0, bs.osd(5).remoteReservations)
	require.Equal(t, 1, bs.osd(5).backfillsFrom)
	require.Equal(t, 0, bs.osd(6).localReservations)
	require.Equal(t, 2, bs.osd(6).remoteReservations)
	require.Equal(t, 0, bs.osd(6).backfillsFrom)
	require.Equal(t, 0, bs.osd(7).localReservations)
	require.Equal(t, 0, bs.osd(7).remoteReservations)
	require.Equal(t, 2, bs.osd(7).backfillsFrom)
	require.Equal(t, 2, bs.osd(77).localReservations)
	require.Equal(t, 1, bs.osd(77).remoteReservations)
	require.Equal(t, 1, bs.osd(77).backfillsFrom)

	// Put 1.01 into a backfill state.
	bs.accountForRemap("1.01", 1, 6)

	require.Equal(t, 1, bs.osd(1).backfillsFrom)
	require.Equal(t, 3, bs.osd(6).remoteReservations)
	require.Equal(t, 3, bs.osd(77).localReservations)

	// 1.02 already has 5 in acting, so this should have no effect on
	// reservations, but will change backfill sources.
	bs.accountForRemap("1.02", 3, 5)

	require.Equal(t, 2, bs.osd(3).backfillsFrom)
	require.Equal(t, 1, bs.osd(4).remoteReservations)
	require.Equal(t, 0, bs.osd(4).backfillsFrom)
	require.Equal(t, 0, bs.osd(5).remoteReservations)
	require.Equal(t, 0, bs.osd(5).backfillsFrom)
	require.Equal(t, 3, bs.osd(77).localReservations)

	// Take 1.02 out of a backfill state.
	bs.accountForRemap("1.02", 4, 3)

	require.Equal(t, 0, bs.osd(3).remoteReservations)
	require.Equal(t, 1, bs.osd(3).backfillsFrom)
	require.Equal(t, 0, bs.osd(4).remoteReservations)
	require.Equal(t, 0, bs.osd(5).remoteReservations)
	require.Equal(t, 2, bs.osd(77).localReservations)

	// Check final state.
	require.Equal(t, 1, bs.osd(3).localReservations)
	require.Equal(t, 0, bs.osd(3).remoteReservations)
	require.Equal(t, 1, bs.osd(3).backfillsFrom)
	require.Equal(t, 0, bs.osd(4).localReservations)
	require.Equal(t, 0, bs.osd(4).remoteReservations)
	require.Equal(t, 0, bs.osd(4).backfillsFrom)
	require.Equal(t, 0, bs.osd(5).localReservations)
	require.Equal(t, 0, bs.osd(5).remoteReservations)
	require.Equal(t, 0, bs.osd(5).backfillsFrom)
	require.Equal(t, 0, bs.osd(6).localReservations)
	require.Equal(t, 3, bs.osd(6).remoteReservations)
	require.Equal(t, 0, bs.osd(6).backfillsFrom)
	require.Equal(t, 0, bs.osd(7).localReservations)
	require.Equal(t, 0, bs.osd(7).remoteReservations)
	require.Equal(t, 2, bs.osd(7).backfillsFrom)
	require.Equal(t, 2, bs.osd(77).localReservations)
	require.Equal(t, 1, bs.osd(77).remoteReservations)
	require.Equal(t, 1, bs.osd(77).backfillsFrom)
}

func TestPgCountsByOsd(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })
	pgDumpOut := `
[
 { "pgid": "1.01", "up": [ 77, 1, 2 ], "acting": [ 77, 1, 2 ] },
 { "pgid": "1.02", "up": [ 77, 3, 4 ], "acting": [ 77, 3, 5 ] }
]
`
	runOsdDump = func() (string, error) { return "{}", nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	bs := mustGetCurrentBackfillState()
	c := bs.pgCountsByOsd()
	require.Equal(t, 2, c[77])
	require.Equal(t, 1, c[1])
	require.Equal(t, 1, c[2])
	require.Equal(t, 1, c[3])
	require.Equal(t, 1, c[4])
}

func TestHasRoomForRemapDoesNotMutateState(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })

	pgDumpOut := `
[
 { "pgid": "1.01", "up": [ 77, 1, 2 ], "acting": [ 77, 1, 2 ] },
 { "pgid": "1.02", "up": [ 77, 3, 4 ], "acting": [ 77, 3, 5 ] },
 { "pgid": "1.03", "up": [ 77, 5, 6 ], "acting": [ 3, 5, 7 ] }
]
`
	runOsdDump = func() (string, error) { return "{}", nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	bs := mustGetCurrentBackfillState()

	type osdCounters struct {
		local, remote, from, max int
	}
	snapshotOSDs := func() map[int]osdCounters {
		out := make(map[int]osdCounters, len(bs.osds))
		for osd, s := range bs.osds {
			out[osd] = osdCounters{
				local:  s.localReservations,
				remote: s.remoteReservations,
				from:   s.backfillsFrom,
				max:    s.maxBackfillReservations,
			}
		}
		return out
	}
	snapshotUp := func() map[string][]int {
		out := make(map[string][]int, len(bs.pgbs))
		for id, pgb := range bs.pgbs {
			dup := make([]int, len(pgb.Up))
			copy(dup, pgb.Up)
			out[id] = dup
		}
		return out
	}

	beforeOSDs := snapshotOSDs()
	beforeUp := snapshotUp()

	// Valid remap probe path; hasRoomForRemap applies and reverts internally.
	_ = bs.hasRoomForRemap("1.02", 4, 5)

	require.Equal(t, beforeOSDs, snapshotOSDs())
	require.Equal(t, beforeUp, snapshotUp())
}

func TestHasRoomForRemapRespectsSourceAndTargetLimits(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })

	pgDumpOut := `
[
 { "pgid": "1.01", "up": [ 77, 1, 2 ], "acting": [ 77, 1, 2 ] },
 { "pgid": "1.02", "up": [ 77, 3, 4 ], "acting": [ 77, 3, 5 ] },
 { "pgid": "1.03", "up": [ 77, 5, 6 ], "acting": [ 3, 5, 7 ] }
]
`
	runOsdDump = func() (string, error) { return "{}", nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	t.Run("source backfill limit", func(t *testing.T) {
		bs := mustGetCurrentBackfillState()
		// from=4 currently has no source backfills; cap at 0 blocks it.
		bs.maxBackfillsFrom = 0
		require.False(t, bs.hasRoomForRemap("1.02", 4, 5))
	})

	t.Run("target reservation limit", func(t *testing.T) {
		bs := mustGetCurrentBackfillState()
		// Make source limit permissive so target-side check is exercised.
		bs.maxBackfillsFrom = 1000

		// 1.01 is currently clean. Remapping 1->6 introduces backfill with target=6.
		// Cap target OSD 6 at zero reservations so this must be rejected.
		bs.osd(6).maxBackfillReservations = 0
		require.False(t, bs.hasRoomForRemap("1.01", 1, 6))
	})
}

func TestComputeBackfillSrcsTgts(t *testing.T) {
	t.Run("mismatches produce aligned src and tgt sets", func(t *testing.T) {
		pgb := &pgBriefItem{
			PgID:   "1.2",
			Up:     []int{1, 2, 33, 4, 55, 6},
			Acting: []int{1, 22, 3, 4, 5, 66},
		}

		srcs, tgts := computeBackfillSrcsTgts(pgb)

		require.Equal(t, []int{22, 3, 5, 66}, srcs)
		require.Equal(t, []int{2, 33, 55, 6}, tgts)
	})

	t.Run("no mismatches produce empty sets", func(t *testing.T) {
		pgb := &pgBriefItem{
			PgID:   "1.3",
			Up:     []int{1, 2, 3},
			Acting: []int{1, 2, 3},
		}

		srcs, tgts := computeBackfillSrcsTgts(pgb)

		require.Empty(t, srcs)
		require.Empty(t, tgts)
	})

	t.Run("supports entries larger than reusable buffer", func(t *testing.T) {
		up := make([]int, 10)
		acting := make([]int, 10)
		expectSrc := make([]int, 10)
		expectTgt := make([]int, 10)
		for i := range 10 {
			acting[i] = i
			up[i] = 100 + i
			expectSrc[i] = i
			expectTgt[i] = 100 + i
		}
		pgb := &pgBriefItem{PgID: "1.4", Up: up, Acting: acting}

		srcs, tgts := computeBackfillSrcsTgts(pgb)

		require.Equal(t, expectSrc, srcs)
		require.Equal(t, expectTgt, tgts)
	})
}
