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
	"errors"
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

func TestParsePGRemapEntry(t *testing.T) {
	for _, tt := range []struct {
		name    string
		entry   string
		want    *pgUpmapItem
		wantErr bool
	}{
		{
			name:  "3-OSD replicated, space-separated brackets",
			entry: "1.0\t[3, 7, 8] -> [3, 7, 2]",
			want: &pgUpmapItem{
				PgID: "1.0",
				Mappings: []mapping{
					{From: 3, To: 3},
					{From: 7, To: 7},
					{From: 8, To: 2},
				},
			},
		},
		{
			name:  "tab-separated brackets, multi-digit OSD IDs",
			entry: "1.abc\t[1, 22, 333] -> [1, 44, 333]",
			want: &pgUpmapItem{
				PgID: "1.abc",
				Mappings: []mapping{
					{From: 1, To: 1},
					{From: 22, To: 44},
					{From: 333, To: 333},
				},
			},
		},
		{
			name:  "single-OSD mapping",
			entry: "2.f [5] -> [9]",
			want: &pgUpmapItem{
				PgID: "2.f",
				Mappings: []mapping{
					{From: 5, To: 9},
				},
			},
		},
		{
			name:  "all same (no actual remap)",
			entry: "3.1 [1, 2] -> [1, 2]",
			want: &pgUpmapItem{
				PgID: "3.1",
				Mappings: []mapping{
					{From: 1, To: 1},
					{From: 2, To: 2},
				},
			},
		},
		{
			name:    "missing arrow / incomplete",
			entry:   "1.0 [3, 7, 8]",
			wantErr: true,
		},
		{
			name:    "no space after pgid",
			entry:   "nospace",
			wantErr: true,
		},
		{
			name:    "mismatched lhs/rhs count",
			entry:   "1.0 [3, 7] -> [3, 7, 2]",
			wantErr: true,
		},
		{
			name:    "empty lhs",
			entry:   "1.0 [] -> [3]",
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePGRemapEntry(tt.entry)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
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
			errMsg: "could not parse PG mapping entry: unequal count between existing and new OSD sets within mapping",
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
			errMsg: "could not parse PG mapping entry: unequal count between existing and new OSD sets within mapping",
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

func TestParseCrushDiffSortsAndKeepsLastDuplicate(t *testing.T) {
	input := `
2.0	[4, 5, 8] -> [3, 6, 0]
1.0	[3, 7, 8] -> [3, 7, 2]
2.0	[4, 5, 8] -> [4, 6, 0]
`

	puis, err := parseCrushDiff(input)
	require.NoError(t, err)
	require.Len(t, puis, 2)

	// Output should always be sorted by PG ID.
	require.Equal(t, "1.0", puis[0].PgID)
	require.Equal(t, []mapping{{From: 3, To: 3}, {From: 7, To: 7}, {From: 8, To: 2}}, puis[0].Mappings)

	require.Equal(t, "2.0", puis[1].PgID)
	// Duplicate PG ID should keep the last parsed mapping entry.
	require.Equal(t, []mapping{{From: 4, To: 4}, {From: 5, To: 6}, {From: 8, To: 0}}, puis[1].Mappings)
}

func TestParseCrushDiffMonotonicInputKeepsLastDuplicate(t *testing.T) {
	input := `
1.0	[3, 7, 8] -> [3, 7, 2]
1.0	[3, 7, 8] -> [3, 2, 8]
2.0	[4, 5, 8] -> [3, 6, 0]
`

	puis, err := parseCrushDiff(input)
	require.NoError(t, err)
	require.Len(t, puis, 2)

	require.Equal(t, "1.0", puis[0].PgID)
	// Duplicate PG ID should keep the last parsed mapping entry.
	require.Equal(t, []mapping{{From: 3, To: 3}, {From: 7, To: 2}, {From: 8, To: 8}}, puis[0].Mappings)

	require.Equal(t, "2.0", puis[1].PgID)
	require.Equal(t, []mapping{{From: 4, To: 3}, {From: 5, To: 6}, {From: 8, To: 0}}, puis[1].Mappings)
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

func TestParseCephCommandAndHandleCephInf(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		var out struct {
			X int `json:"x"`
		}
		err := parseCephCommand(`{"x":7}`, nil, &out)
		require.NoError(t, err)
		require.Equal(t, 7, out.X)
	})

	t.Run("run error returned", func(t *testing.T) {
		var out map[string]any
		err := parseCephCommand(`{"x":7}`, errors.New("boom"), &out)
		require.Error(t, err)
		require.Contains(t, err.Error(), "boom")
	})

	t.Run("inf replacement", func(t *testing.T) {
		in := []byte(`{"a": inf, "b":inf, "c": 1}`)
		got := handleCephInf(in)
		require.Equal(t, `{"a": null, "b":null, "c": 1}`, string(got))
	})

	t.Run("no inf", func(t *testing.T) {
		in := []byte(`{"a": 1, "b": 2}`)
		got := handleCephInf(in)
		require.Equal(t, string(in), string(got))
	})

	t.Run("does not replace non-matching inf text", func(t *testing.T) {
		in := []byte(`{"a":"infinite", "b":"prefix_inf_suffix"}`)
		got := handleCephInf(in)
		require.Equal(t, string(in), string(got))
	})

	t.Run("replaces multiple spaced and compact inf occurrences", func(t *testing.T) {
		in := []byte(`{"a": inf, "b":inf, "c": inf, "d":inf}`)
		got := handleCephInf(in)
		require.Equal(t, `{"a": null, "b":null, "c": null, "d":null}`, string(got))
	})
}

func TestGetOsdsForBucketNotFoundAndDeviceClassFilter(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		setupTest(t)
		t.Cleanup(func() { teardownTest(t) })

		runOsdTree = func() (string, error) {
			return `{"nodes":[]}`, nil
		}

		_, err := getOsdsForBucket("does-not-exist", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not a CRUSH bucket")
	})

	t.Run("device class filtering", func(t *testing.T) {
		setupTest(t)
		t.Cleanup(func() { teardownTest(t) })

		runOsdTree = func() (string, error) {
			return `{
				"nodes": [
					{"id":-1, "name":"root", "type":"root", "children":[-2]},
					{"id":-2, "name":"rack1", "type":"rack", "children":[0,1,2]},
					{"id":0, "name":"osd.0", "type":"osd", "device_class":"hdd", "reweight":1},
					{"id":1, "name":"osd.1", "type":"osd", "device_class":"ssd", "reweight":1},
					{"id":2, "name":"osd.2", "type":"osd", "device_class":"ssd", "reweight":0}
				]
			}`, nil
		}

		osds, err := getOsdsForBucket("rack1", "ssd")
		require.NoError(t, err)
		require.Equal(t, []int{1}, osds)
	})
}
