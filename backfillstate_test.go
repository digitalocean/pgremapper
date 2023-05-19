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
	defer teardownTest(t)
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
