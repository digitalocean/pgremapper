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
			require.Nil(t, err)
			require.Equal(t, items, tt.items)
		})
	}
}
