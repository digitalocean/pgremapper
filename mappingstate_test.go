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

func TestGetMappings(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })
	pgDumpOut := `
[
 { "pgid": "1.1", "up": [ 1, 2, 4 ], "acting": [ 1, 2, 3 ], "state": "backfill_wait" },
 { "pgid": "1.2", "up": [ 4, 5, 3 ], "acting": [ 1, 2, 3 ], "state": "backfill_wait" }
]
`

	osdDumpOut := `
{
  "pg_upmap_items": [
    { "pgid": "1.1", "mappings": [ { "from": 3, "to": 4 } ] },
    { "pgid": "1.2", "mappings": [ { "from": 1, "to": 4 }, { "from": 2, "to": 5 } ] }
  ]
}
`

	runOsdDump = func() (string, error) { return osdDumpOut, nil }
	runPgDumpPgsBrief = func() (string, error) { return pgDumpOut, nil }

	tests := []struct {
		name     string
		filter   mappingFilter
		expected []pgMapping
	}{
		{
			name:   "single PG",
			filter: withPgid("1.2"),
			expected: []pgMapping{
				{PgID: "1.2", Mapping: mapping{From: 1, To: 4}},
				{PgID: "1.2", Mapping: mapping{From: 2, To: 5}},
			},
		},
		{
			name:   "single OSD from",
			filter: withFrom(1),
			expected: []pgMapping{
				{PgID: "1.2", Mapping: mapping{From: 1, To: 4}},
			},
		},
		{
			name:   "single OSD to",
			filter: withTo(4),
			expected: []pgMapping{
				{PgID: "1.1", Mapping: mapping{From: 3, To: 4}},
				{PgID: "1.2", Mapping: mapping{From: 1, To: 4}},
			},
		},
		{
			name:   "and with results",
			filter: mfAnd(withFrom(1), withTo(4)),
			expected: []pgMapping{
				{PgID: "1.2", Mapping: mapping{From: 1, To: 4}},
			},
		},
		{
			name:     "and without results",
			filter:   mfAnd(withFrom(2), withTo(4)),
			expected: []pgMapping{},
		},
		{
			name:   "or",
			filter: mfOr(withFrom(3), withTo(5)),
			expected: []pgMapping{
				{PgID: "1.1", Mapping: mapping{From: 3, To: 4}},
				{PgID: "1.2", Mapping: mapping{From: 2, To: 5}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			M = mustGetCurrentMappingState()
			got := M.getMappings(tt.filter)
			require.ElementsMatch(t, tt.expected, got)
		})
	}
}

func TestTryRemapRemovesExactOppositeMapping(t *testing.T) {
	setupTest(t)
	t.Cleanup(func() { teardownTest(t) })

	runOsdDump = func() (string, error) {
		return `
{
  "osds": [],
  "pg_upmap_items": [
    { "pgid": "1.1", "mappings": [ { "from": 5, "to": 6 } ] }
  ]
}
`, nil
	}
	runPgDumpPgsBrief = func() (string, error) {
		return `
[
  { "pgid": "1.1", "up": [ 6 ], "acting": [ 5 ], "state": "active+remapped" }
]
`, nil
	}
	runOsdPoolLs = func() (string, error) {
		return `[{"pool_id":1,"pool_name":"replicated","erasure_code_profile":""}]`, nil
	}

	M = mustGetCurrentMappingState()
	err := M.tryRemap("1.1", 6, 5)
	require.NoError(t, err)

	require.Empty(t, M.getMappings(withPgid("1.1")))
	dirty := M.dirtyUpmapItems()
	require.Len(t, dirty, 1)
	require.Len(t, dirty[0].Mappings, 0)
	require.Len(t, dirty[0].removedMappings, 1)
	require.Equal(t, 5, dirty[0].removedMappings[0].From)
	require.Equal(t, 6, dirty[0].removedMappings[0].To)
}
