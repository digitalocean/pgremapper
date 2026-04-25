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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasDuplicateOSDID(t *testing.T) {
	for _, tt := range []struct {
		name   string
		osdids []int
		want   bool
	}{
		{
			name:   "nil slice",
			osdids: nil,
			want:   false,
		},
		{
			name:   "empty slice",
			osdids: []int{},
			want:   false,
		},
		{
			name:   "single element",
			osdids: []int{7},
			want:   false,
		},
		{
			name:   "non duplicate normal values",
			osdids: []int{1, 2, 3},
			want:   false,
		},
		{
			name:   "duplicate normal values",
			osdids: []int{1, 2, 1},
			want:   true,
		},
		{
			name:   "invalid sentinel only",
			osdids: []int{invalidOSD, invalidOSD, invalidOSD},
			want:   false,
		},
		{
			name:   "mixed invalid sentinel and duplicate value",
			osdids: []int{invalidOSD, 7, invalidOSD, 7},
			want:   true,
		},
		{
			name:   "negative values no duplicate",
			osdids: []int{-1, -2, -3},
			want:   false,
		},
		{
			name:   "negative values with duplicate",
			osdids: []int{-1, -2, -1},
			want:   true,
		},
		{
			name:   "large int values with duplicate",
			osdids: []int{math.MaxInt, 42, math.MaxInt},
			want:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, hasDuplicateOSDID(tt.osdids))
		})
	}
}

func TestParseCrushDiff(t *testing.T) {
	for _, tt := range []struct {
		name    string
		crushIn string
		items   []pgMapping
		errMsg  string
	}{
		{
			name: "valid case with 2 PGs remapped",
			crushIn: `
#osd	count	first	primary	c wt	wt
osd.0	79	30	30	0.00979614	1
osd.1	84	28	28	0.00979614	1
osd.2	57	20	20	0.00979614	1
osd.3	51	22	22	0.00979614	1
osd.4	50	13	13	0.00979614	1
osd.5	50	18	18	0.00979614	1
osd.6	54	18	18	0.00979614	1
osd.7	52	15	15	0.00979614	1
osd.8	54	13	13	0.00979614	1
 in 9
 avg 59 stddev 12.2656 (0.207891x) (expected 7.24185 0.122743x))
 min osd.4 50
 max osd.1 84
size 3	177
osdmaptool: writing epoch 847 to /tmp/tmp5ip_axby/osdmap
osdmaptool /tmp/tmp5ip_axby/osdmap --dump json > /tmp/tmp5ip_axby/osdmap.json
osdmaptool: osdmap file '/tmp/tmp5ip_axby/osdmap'
1.0	[3, 7, 8] -> [3, 7, 2]
2.0	[4, 5, 8] -> [3, 6, 0]
		`,
			items: []pgMapping{
				{
					PgID:    "1.0",
					Mapping: mapping{From: 8, To: 2},
				},
				{
					PgID:    "2.0",
					Mapping: mapping{From: 4, To: 3},
				},
				{
					PgID:    "2.0",
					Mapping: mapping{From: 5, To: 6},
				},
				{
					PgID:    "2.0",
					Mapping: mapping{From: 8, To: 0},
				},
			},
			errMsg: "",
		},
		{
			name: "invalid case with 1 PG with mismatched To set",
			crushIn: `
#osd	count	first	primary	c wt	wt
osd.0	79	30	30	0.00979614	1
osd.1	84	28	28	0.00979614	1
osd.2	57	20	20	0.00979614	1
osd.3	51	22	22	0.00979614	1
osd.4	50	13	13	0.00979614	1
osd.5	50	18	18	0.00979614	1
osd.6	54	18	18	0.00979614	1
osd.7	52	15	15	0.00979614	1
osd.8	54	13	13	0.00979614	1
 in 9
 avg 59 stddev 12.2656 (0.207891x) (expected 7.24185 0.122743x))
 min osd.4 50
 max osd.1 84
size 3	177
osdmaptool: writing epoch 847 to /tmp/tmp5ip_axby/osdmap
osdmaptool /tmp/tmp5ip_axby/osdmap --dump json > /tmp/tmp5ip_axby/osdmap.json
osdmaptool: osdmap file '/tmp/tmp5ip_axby/osdmap'
1.0	[3, 7, 8] -> [3, 7, 2]
2.0	[4, 5, 8] -> [3, 6]
		`,
			items:  nil,
			errMsg: "could not parse PG mapping entry: invalid PG mapping entry",
		},
		{
			name: "invalid case with 1 PG with mismatched From set",
			crushIn: `
#osd	count	first	primary	c wt	wt
osd.0	79	30	30	0.00979614	1
osd.1	84	28	28	0.00979614	1
osd.2	57	20	20	0.00979614	1
osd.3	51	22	22	0.00979614	1
osd.4	50	13	13	0.00979614	1
osd.5	50	18	18	0.00979614	1
osd.6	54	18	18	0.00979614	1
osd.7	52	15	15	0.00979614	1
osd.8	54	13	13	0.00979614	1
 in 9
 avg 59 stddev 12.2656 (0.207891x) (expected 7.24185 0.122743x))
 min osd.4 50
 max osd.1 84
size 3	177
osdmaptool: writing epoch 847 to /tmp/tmp5ip_axby/osdmap
osdmaptool /tmp/tmp5ip_axby/osdmap --dump json > /tmp/tmp5ip_axby/osdmap.json
osdmaptool: osdmap file '/tmp/tmp5ip_axby/osdmap'
1.0	[3, 7, 8] -> [3, 7, 2]
2.0	[4, 5] -> [3, 6, 0]
		`,
			items:  nil,
			errMsg: "could not parse PG mapping entry: invalid PG mapping entry",
		},
		{
			name: "invalid case with 1 PG with both mismatched sets",
			crushIn: `
#osd	count	first	primary	c wt	wt
osd.0	79	30	30	0.00979614	1
osd.1	84	28	28	0.00979614	1
osd.2	57	20	20	0.00979614	1
osd.3	51	22	22	0.00979614	1
osd.4	50	13	13	0.00979614	1
osd.5	50	18	18	0.00979614	1
osd.6	54	18	18	0.00979614	1
osd.7	52	15	15	0.00979614	1
osd.8	54	13	13	0.00979614	1
 in 9
 avg 59 stddev 12.2656 (0.207891x) (expected 7.24185 0.122743x))
 min osd.4 50
 max osd.1 84
size 3	177
osdmaptool: writing epoch 847 to /tmp/tmp5ip_axby/osdmap
osdmaptool /tmp/tmp5ip_axby/osdmap --dump json > /tmp/tmp5ip_axby/osdmap.json
osdmaptool: osdmap file '/tmp/tmp5ip_axby/osdmap'
1.0	[3, 7, 8] -> [3, 7, 2]
2.0	[4] -> [3, 6, 0]
		`,
			items:  nil,
			errMsg: "could not parse PG mapping entry: unequal count between existing and new OSD sets within mapping",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runCrushCmp = func(_ string) (string, error) {
				return tt.crushIn, nil
			}
			if tt.errMsg != "" {
				defer func() {
					msg := recover()
					require.NotNil(t, msg)

					e, ok := msg.(error)
					require.True(t, ok)
					require.Contains(t, e.Error(), tt.errMsg)
				}()
			}

			items, err := crushCmp("")
			require.NoError(t, err)
			require.Equal(t, tt.items, items)
		})
	}
}

func resetCephStateForCacheTest() {
	savedOsdDumpOut = nil
	savedOsdPoolsDetails = nil
	savedParsedOsdTree = nil
	savedPgDumpPgsBrief = nil
	savedPgUpmapItemMap = nil
	savedPgUpmapItemMapSource = nil
}

func TestPgUpmapItemMapCacheInvalidatesAfterOsdDumpReset(t *testing.T) {
	resetCephStateForCacheTest()
	defer teardownTest(t)

	variant := 1
	runOsdDump = func() (string, error) {
		if variant == 1 {
			return `{"pg_upmap_items":[{"pgid":"1.1","mappings":[{"from":10,"to":11}]}]}`, nil
		}
		return `{"pg_upmap_items":[{"pgid":"1.2","mappings":[{"from":20,"to":21}]}]}`, nil
	}

	first := pgUpmapItemMap()
	require.Contains(t, first, "1.1")
	require.NotContains(t, first, "1.2")

	variant = 2

	// Without resetting osdDump cache, map should still be the same cached view.
	second := pgUpmapItemMap()
	require.Equal(t, first, second)
	require.Contains(t, second, "1.1")
	require.NotContains(t, second, "1.2")

	// Resetting osdDump cache should force a fresh map build from new dump content.
	savedOsdDumpOut = nil
	third := pgUpmapItemMap()
	require.NotEqual(t, first, third)
	require.Contains(t, third, "1.2")
	require.NotContains(t, third, "1.1")
}

func TestPgDumpPgsBriefReordersUsingUpmapMappings(t *testing.T) {
	resetCephStateForCacheTest()
	defer teardownTest(t)

	runOsdPoolLs = func() (string, error) {
		return `[
			{"pool_id": 1, "pool_name": "replicated", "erasure_code_profile": ""}
		]`, nil
	}
	runOsdDump = func() (string, error) {
		return `{
			"pg_upmap_items": [
				{"pgid":"1.1","mappings":[{"from":4,"to":2}]},
				{"pgid":"1.2","mappings":[{"from":8,"to":6}]}
			]
		}`, nil
	}
	runPgDumpPgsBrief = func() (string, error) {
		return `[
			{"pgid":"1.1","state":"active","up":[1,3,2],"acting":[1,4,3]},
			{"pgid":"1.2","state":"active","up":[5,7,6],"acting":[5,8,7]}
		]`, nil
	}

	pgBriefs := pgDumpPgsBrief()
	require.Len(t, pgBriefs, 2)
	require.Equal(t, []int{1, 2, 3}, pgBriefs[0].Up)
	require.Equal(t, []int{5, 6, 7}, pgBriefs[1].Up)
}

func TestPgDumpPgsBriefDoesNotReorderECPools(t *testing.T) {
	resetCephStateForCacheTest()
	defer teardownTest(t)

	runOsdPoolLs = func() (string, error) {
		return `[
			{"pool_id": 2, "pool_name": "ec", "erasure_code_profile": "ec-profile"}
		]`, nil
	}
	runOsdDump = func() (string, error) {
		return `{
			"pg_upmap_items": [
				{"pgid":"2.1","mappings":[{"from":4,"to":2}]}
			]
		}`, nil
	}
	runPgDumpPgsBrief = func() (string, error) {
		return `[
			{"pgid":"2.1","state":"active","up":[1,3,2],"acting":[1,4,3]}
		]`, nil
	}

	pgBriefs := pgDumpPgsBrief()
	require.Len(t, pgBriefs, 1)
	// EC pool ordering must be preserved exactly.
	require.Equal(t, []int{1, 3, 2}, pgBriefs[0].Up)
}

func TestPgDumpPgsBriefParsesBothJSONShapes(t *testing.T) {
	tests := []struct {
		name string
		out  string
	}{
		{
			name: "array",
			out: `[
				{"pgid":"1.1","state":"active","up":[1,2,3],"acting":[1,2,3]},
				{"pgid":"1.2","state":"active","up":[4,5,6],"acting":[4,5,6]}
			]`,
		},
		{
			name: "nautilus+",
			out: `{
				"pg_stats": [
					{"pgid":"1.1","state":"active","up":[1,2,3],"acting":[1,2,3]},
					{"pgid":"1.2","state":"active","up":[4,5,6],"acting":[4,5,6]}
				]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetCephStateForCacheTest()
			defer teardownTest(t)

			runOsdPoolLs = func() (string, error) {
				return `[
					{"pool_id": 1, "pool_name": "replicated", "erasure_code_profile": ""}
				]`, nil
			}
			runOsdDump = func() (string, error) {
				return `{"pg_upmap_items": []}`, nil
			}
			runPgDumpPgsBrief = func() (string, error) {
				return tt.out, nil
			}

			pgBriefs := pgDumpPgsBrief()
			require.Len(t, pgBriefs, 2)
			require.Equal(t, "1.1", pgBriefs[0].PgID)
			require.Equal(t, "1.2", pgBriefs[1].PgID)
		})
	}
}
