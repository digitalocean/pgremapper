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
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/pkg/errors"
)

const (
	invalidOSD = math.MaxInt32
)

var (
	runOsdDump        = func() (string, error) { return run("ceph", "osd", "dump", "-f", "json") }
	runOsdTree        = func() (string, error) { return run("ceph", "osd", "tree", "-f", "json") }
	runPgDumpPgsBrief = func() (string, error) { return run("ceph", "pg", "dump", "pgs_brief", "-f", "json") }
	runPgQuery        = func(pgid string) (string, error) { return run("ceph", "pg", pgid, "query", "-f", "json") }

	pgQueryPeerRegexp = regexp.MustCompile(`(?P<osd>[0-9]+)(?:\((?P<index>[0-9]+)\))?`)
)

type pgUpmapItem struct {
	PgID     string    `json:"pgid"`
	Mappings []mapping `json:"mappings"`

	removedMappings []mapping
	dirty           bool
}

type osdDumpOut struct {
	Osds []struct {
		In  int `json:"in"`
		Up  int `json:"up"`
		Osd int `json:"osd"`
	} `json:"osds"`
	PgUpmapItems []*pgUpmapItem `json:"pg_upmap_items"`
}

type osdTreeOutNode struct {
	ID       int     `json:"id"`
	Type     string  `json:"type"`
	Name     string  `json:"name"`
	Reweight float64 `json:"reweight"`
	Children []int   `json:"children"`
}

type osdTreeOut struct {
	Nodes []*osdTreeOutNode `json:"nodes"`
}

type osdTreeNode struct {
	ID       int
	Name     string
	Type     string
	Reweight float64

	Parent   *osdTreeNode
	Children []*osdTreeNode
}

type parsedOsdTree struct {
	Root       *osdTreeNode
	IDToNode   map[int]*osdTreeNode
	NameToNode map[string]*osdTreeNode
}

type mapping struct {
	From int `json:"from"`
	To   int `json:"to"`

	dirty bool
}

type pgBriefItem struct {
	PgID   string `json:"pgid"`
	State  string `json:"state"`
	Up     []int  `json:"up"`
	Acting []int  `json:"acting"`
}

type pgBriefNautilus struct {
	PgStats []*pgBriefItem `json:"pg_stats"`
}

type pgQueryOut struct {
	Acting []int `json:"acting"`
	Info   struct {
		PgID string `json:"pgid"`
	} `json:"info"`
	PeerInfo []struct {
		Peer       string `json:"peer"`
		Incomplete int    `json:"incomplete"`
	} `json:"peer_info"`
}

func (pqo *pgQueryOut) getCompletePeers() []int {
	// Start with the acting set, since we know those are complete. We'll
	// then iterate the peers to find shards/replicas that are missing but
	// complete, as these need recovery before they're considered acting
	// again.
	peers := pqo.Acting

	for _, pi := range pqo.PeerInfo {
		if pi.Incomplete == 1 {
			continue
		}
		// For EC pools, Peer takes the form 'osdid(index)'. For replicated
		// pools, it's simply 'osdid'.
		m := pgQueryPeerRegexp.FindStringSubmatch(pi.Peer)
		if len(m) != 3 {
			panic(fmt.Sprintf("%s: can't interpret peer %q", pqo.Info.PgID, pi.Peer))
		}

		osd, err := strconv.Atoi(m[1])
		if err != nil {
			panic(fmt.Sprintf("%s: %s in peer ID %q is not a valid OSD ID", pqo.Info.PgID, m[1], pi.Peer))
		}

		if m[2] != "" {
			// EC pool case - we get the index from Peer.
			index, err := strconv.Atoi(m[2])
			if err != nil {
				panic(fmt.Sprintf("%s: %s in peer ID %q is not a valid index", pqo.Info.PgID, m[2], pi.Peer))
			}
			if peers[index] == osd {
				continue
			}
			if peers[index] != invalidOSD {
				panic(fmt.Sprintf("%s: multiple complete shards at index %d", pqo.Info.PgID, index))
			}
			peers[index] = osd
		} else {
			// Replicated pool case. Order doesn't matter; if this
			// OSD isn't already in the set, then put it at the
			// first missing slot.
			firstMissing := -1
			found := false
			for i, p := range peers {
				if firstMissing == -1 && p == invalidOSD {
					firstMissing = i
				}
				if p == osd {
					found = true
					break
				}
			}
			if found {
				continue
			}
			if firstMissing == -1 {
				panic(fmt.Sprintf("%s: too many complete replicas", pqo.Info.PgID))
			}
			peers[firstMissing] = osd
		}
	}
	return peers
}

func (pgb *pgBriefItem) primaryOsd() int {
	for _, osd := range pgb.Acting {
		if osd != invalidOSD {
			return osd
		}
	}
	panic(fmt.Sprintf("%s: no valid OSDs found in acting set", pgb.PgID))
}

type cephStatus struct {
	Health struct {
		Checks struct {
			OsdmapFlags struct {
				Severity string `json:"severity"`
				Summary  struct {
					Message string `json:"message"`
				} `json:"summary"`
			} `json:"OSDMAP_FLAGS"`
		} `json:"checks"`
	} `json:"health"`
}

func (otn *osdTreeNode) getNearestParentOfType(t string) *osdTreeNode {
	parent := otn.Parent
	for parent != nil {
		if parent.Type == t {
			break
		}
		parent = parent.Parent
	}
	return parent
}

func (otn *osdTreeNode) mustGetNearestParentOfType(t string) *osdTreeNode {
	parent := otn.getNearestParentOfType(t)
	if parent == nil {
		panic(fmt.Sprintf("node %s has no parent of type %s", otn.Name, t))
	}
	return parent
}

func (pui *pgUpmapItem) String() string {
	fmtMappingList := func(list []mapping, a color.Attribute) string {
		c := color.New(a).SprintFunc()
		strList := make([]string, len(list))
		for i, item := range list {
			s := item.String()
			if item.dirty {
				s = c(s)
			}
			strList[i] = s
		}

		return strings.Join(strList, ",")
	}

	str := fmt.Sprintf("pg %s: [", pui.PgID)
	if len(pui.Mappings) > 0 {
		str += fmtMappingList(pui.Mappings, color.FgGreen)
	}
	if len(pui.removedMappings) > 0 {
		if len(pui.Mappings) > 0 {
			str += ","
		}
		str += fmtMappingList(pui.removedMappings, color.FgRed)
	}
	str += "]"
	return str
}

func (pui *pgUpmapItem) do() {
	if len(pui.Mappings) == 0 {
		_ = runOrDie("ceph", "osd", "rm-pg-upmap-items", pui.PgID)
		return
	}

	cmd := []string{"ceph", "osd", "pg-upmap-items", pui.PgID}
	for _, m := range pui.Mappings {
		cmd = append(cmd, fmt.Sprintf("%d", m.From), fmt.Sprintf("%d", m.To))
	}
	_ = runOrDie(cmd...)
}

func (r mapping) String() string {
	return fmt.Sprintf("%d->%d", r.From, r.To)
}

func mustGetOsdsForBucket(bucket string) []int {
	osds, err := getOsdsForBucket(bucket)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return osds
}

func getOsdsForBucket(bucket string) ([]int, error) {
	tree := cachedOsdTree()

	bucketNode, ok := tree.NameToNode[bucket]
	if !ok {
		return nil, errors.Errorf("'%s' is not a CRUSH bucket known to this cluster", bucket)
	}

	osds := []int{}
	for _, c := range bucketNode.Children {
		if c.Type != "osd" {
			osds = append(osds, mustGetOsdsForBucket(c.Name)...)
			continue
		}
		if c.Reweight == 0 {
			// This OSD is 'out' - exclude it.
			continue
		}
		osds = append(osds, c.ID)
	}
	return osds, nil
}

func countCurrentBackfills() (map[int]int, map[int]int) {
	sourceBackfillCounts := make(map[int]int)
	targetBackfillCounts := make(map[int]int)
	pgBriefs := pgDumpPgsBrief()
	for _, pgb := range pgBriefs {
		up := pgb.Up
		acting := pgb.Acting

		for i := range acting {
			if up[i] != acting[i] {
				sourceBackfillCounts[acting[i]]++
				targetBackfillCounts[up[i]]++
			}
		}
	}
	return sourceBackfillCounts, targetBackfillCounts
}

func pgDumpPgsBrief() []*pgBriefItem {
	out, err := runPgDumpPgsBrief()
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}

	var pgBriefs []*pgBriefItem

	if err := json.Unmarshal([]byte(out), &pgBriefs); err != nil {
		// Newer versions of Ceph have a slightly different structure.
		var pgBriefNautilusOut pgBriefNautilus
		if err := json.Unmarshal([]byte(out), &pgBriefNautilusOut); err != nil {
			panic(errors.WithStack(err))
		}
		pgBriefs = pgBriefNautilusOut.PgStats
	}
	pgBriefs = sanitizePgBriefs(pgBriefs)

	puis := pgUpmapItemMap()
	for _, pgb := range pgBriefs {
		reorderUpToMatchActing(puis[pgb.PgID], pgb.Up, pgb.Acting)
	}

	return pgBriefs
}

func sanitizePgBriefs(pgBriefs []*pgBriefItem) []*pgBriefItem {
	duplicateMessage := "WARNING: PG %s's %s set has one or more duplicated OSD IDs; this PG will be excluded from operations and reservation calculations. Please check your CRUSH rules and map.\n"
	sanitized := make([]*pgBriefItem, 0, len(pgBriefs))

	for _, pgBrief := range pgBriefs {
		if len(pgBrief.Up) != len(pgBrief.Acting) {
			fmt.Printf("WARNING: PG %s's up and acting sets have mismatched lengths (%d vs. %d), perhaps due to a change in CRUSH rules; this PG will be excluded from operations and reservation calculations.\n", pgBrief.PgID, len(pgBrief.Up), len(pgBrief.Acting))
			continue
		}

		if hasDuplicateOSDID(pgBrief.Acting) {
			fmt.Printf(duplicateMessage, pgBrief.PgID, "acting")
			continue
		}

		if hasDuplicateOSDID(pgBrief.Up) {
			fmt.Printf(duplicateMessage, pgBrief.PgID, "up")
			continue
		}

		sanitized = append(sanitized, pgBrief)
	}

	return sanitized
}

func hasDuplicateOSDID(osdids []int) bool {
	for i, osdid := range osdids {
		for j, otherOSDID := range osdids {
			if i == j {
				continue
			}
			if osdid == otherOSDID {
				return true
			}
		}
	}
	return false
}

func reorderUpToMatchActing(pui *pgUpmapItem, up, acting []int) {
	if pui == nil {
		pui = &pgUpmapItem{}
	}

	// Re-order the up list so that any OSDs in it that are also in the
	// acting list are in the same place. We also need to take into account
	// upmap items which create relationships between the up and acting
	// OSDs. This should never do anything for EC pools, where the order
	// matters and won't change, but for replicated pools the order can
	// change and this doesn't imply data movement.
	for ai, osd := range acting {
		fromOsd := invalidOSD
		for ui := range up {
			// If this PG is in backfill, it could be because of an
			// upmap item. Find any such matching mapping and
			// consider its source OSD when matching against this
			// acting OSD.
			for _, mp := range pui.Mappings {
				if mp.To == up[ui] {
					fromOsd = mp.From
				}
			}
			if up[ui] != osd && (fromOsd == invalidOSD || fromOsd != osd) {
				continue
			}
			if ui == ai {
				// Indexes match; no change required.
				break
			}
			// Swap whatever's at the acting set index with
			// this OSD.
			tmp := up[ui]
			up[ui] = up[ai]
			up[ai] = tmp
			break
		}
	}
}

func osdDump() *osdDumpOut {
	var out osdDumpOut

	jsonOut, err := runOsdDump()
	mustParseCephCommand(jsonOut, err, &out)

	return &out
}

func pgUpmapItemMap() map[string]*pgUpmapItem {
	osdDumpOut := osdDump()

	puis := make(map[string]*pgUpmapItem)
	for _, pui := range osdDumpOut.PgUpmapItems {
		puis[pui.PgID] = pui
	}

	return puis
}

func osdTree() *parsedOsdTree {
	var out osdTreeOut

	jsonOut, err := runOsdTree()
	mustParseCephCommand(jsonOut, err, &out)

	tree := &parsedOsdTree{
		IDToNode:   make(map[int]*osdTreeNode),
		NameToNode: make(map[string]*osdTreeNode),
	}

	// First, build direct lookup mappings.
	for _, n := range out.Nodes {
		node := &osdTreeNode{
			ID:       n.ID,
			Name:     n.Name,
			Type:     n.Type,
			Reweight: n.Reweight,
		}
		tree.IDToNode[n.ID] = node
		tree.NameToNode[n.Name] = node
	}

	// Now, use the ID mapping from above to fill out parent/child links.
	for _, n := range out.Nodes {
		treeNode := tree.IDToNode[n.ID]
		for _, c := range n.Children {
			child := tree.IDToNode[c]

			child.Parent = treeNode
			treeNode.Children = append(treeNode.Children, child)
		}
	}

	return tree
}

var _cachedOsdTree *parsedOsdTree

func cachedOsdTree() *parsedOsdTree {
	if _cachedOsdTree == nil {
		_cachedOsdTree = osdTree()
	}
	return _cachedOsdTree
}

func pgQuery(pgid string) *pgQueryOut {
	var out pgQueryOut

	jsonOut, err := runPgQuery(pgid)
	mustParseCephCommand(jsonOut, err, &out)

	return &out
}

func mustParseCephCommand(out string, err error, v interface{}) {
	if err := parseCephCommand(out, err, v); err != nil {
		panic(errors.WithStack(err))
	}
}

func parseCephCommand(out string, err error, v interface{}) error {
	if err != nil {
		return err
	}

	if err := json.Unmarshal([]byte(out), v); err != nil {
		return err
	}

	return nil
}
