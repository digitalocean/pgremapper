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
	runOsdPoolLs      = func() (string, error) { return run("ceph", "osd", "pool", "ls", "detail", "-f", "json") }
	runPgDumpPgsBrief = func() (string, error) { return run("ceph", "pg", "dump", "pgs_brief", "-f", "json") }
	runPgQuery        = func(pgid string) (string, error) { return run("ceph", "pg", pgid, "query", "-f", "json") }

	pgQueryPeerRegexp = regexp.MustCompile(`(?P<osd>[0-9]+)(?:\((?P<index>[0-9]+)\))?`)
	pgIdRegexp        = regexp.MustCompile(`(?P<pool>[0-9]+)\.(?P<id>[0-9a-f]+)`)
)

type pgUpmapItem struct {
	PgID     string    `json:"pgid"`
	Mappings []mapping `json:"mappings"`

	removedMappings []mapping
	staleMappings   []mapping
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
	ID          int     `json:"id"`
	DeviceClass string  `json:"device_class"`
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Reweight    float64 `json:"reweight"`
	Children    []int   `json:"children"`
}

type osdTreeOut struct {
	Nodes []*osdTreeOutNode `json:"nodes"`
}

type osdTreeNode struct {
	ID          int
	DeviceClass string
	Type        string
	Name        string
	Reweight    float64

	Parent   *osdTreeNode
	Children []*osdTreeNode
}

type osdPoolDetail struct {
	ID        int    `json:"pool_id"`
	Name      string `json:"pool_name"`
	ECProfile string `json:"erasure_code_profile"`
}

type poolsDetails struct {
	Pools map[int]*osdPoolDetail
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
		Stats      struct {
			LastEpochClean int `json:"last_epoch_clean"`
		} `json:"stats"`
	} `json:"peer_info"`
}

// mappingsAsToFromMap returns the To/From OSD mapping pairs of an upmap item as a map
func (pui *pgUpmapItem) mappingsAsToFromMap() map[int]int {
	mappings := make(map[int]int)
	for _, mp := range pui.Mappings {
		mappings[mp.To] = mp.From
	}
	return mappings
}

func (pqo *pgQueryOut) getCompletePeers() []int {
	// Start with the acting set, since we know those are complete. We'll
	// then iterate the peers to find shards/replicas that are missing but
	// complete, as these need recovery before they're considered acting
	// again.
	peers := pqo.Acting
	osdEpochMap := make(map[int]int)

	for _, pi := range pqo.PeerInfo {

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

			// Save the last_epoch_clean for later comparison
			osdEpochMap[osd] = pi.Stats.LastEpochClean

			if peers[index] == osd {
				continue
			}
			if peers[index] != invalidOSD {
				// Choose the shard with the newest last_epoch_clean
				if osdEpochMap[peers[index]] > pi.Stats.LastEpochClean {
					continue
				}
			}
			peers[index] = osd
		} else {
			// For the replicated pool case we pick all of the complete peers
			// as the method for determining which ones should be replaced.
			if pi.Incomplete == 1 {
				continue
			}
			// Order doesn't matter; if this
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
	str := fmt.Sprintf("pg %s: [", pui.PgID)
	printedMappings := false

	fmtMappingList := func(list []mapping, prefix string, a color.Attribute) {
		if len(list) == 0 {
			return
		}

		if printedMappings {
			str += ","
		}

		c := color.New(a).SprintFunc()
		strList := make([]string, len(list))
		for i, item := range list {
			s := item.String()
			if item.dirty {
				s = c(prefix + s)
			}
			strList[i] = s
		}

		str += strings.Join(strList, ",")
		printedMappings = true
	}

	fmtMappingList(pui.Mappings, "+", color.FgGreen)
	fmtMappingList(pui.removedMappings, "-", color.FgRed)
	fmtMappingList(pui.staleMappings, "!", color.FgYellow)

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

// Detect whether a given PG belongs to an erasure-coded pool
func (pd *poolsDetails) PgUsesEC(pgid string) bool {
	m := pgIdRegexp.FindStringSubmatch(pgid)
	if len(m) != 3 {
		panic(fmt.Sprintf("can't parse PGID %s", pgid))
	}
	poolId, err := strconv.Atoi(m[1])
	if err != nil {
		panic(fmt.Sprintf("can't parse pool in PGID %s", pgid))
	}
	if pool, ok := pd.Pools[poolId]; ok {
		return pool.ECProfile != "replicated_rule"
	}
	panic(fmt.Sprintf("could not find pool data for PG %s", pgid))
}

func (r mapping) String() string {
	return fmt.Sprintf("%d->%d", r.From, r.To)
}

func mustGetOsdsForBucket(bucket string, deviceClass string) []int {
	osds, err := getOsdsForBucket(bucket, deviceClass)

	if err != nil {
		panic(errors.WithStack(err))
	}
	return osds
}

func getOsdsForBucket(bucket string, deviceClass string) ([]int, error) {
	tree := osdTree()

	bucketNode, ok := tree.NameToNode[bucket]
	if !ok {
		return nil, errors.Errorf("'%s' is not a CRUSH bucket known to this cluster", bucket)
	}

	osds := []int{}
	for _, c := range bucketNode.Children {
		if c.Type != "osd" {
			osds = append(osds, mustGetOsdsForBucket(c.Name, deviceClass)...)
			continue
		}
		if c.Reweight == 0 {
			// This OSD is 'out' - exclude it
			continue
		}
		// Perform device_class check only of device_class is defined
		if deviceClass != "" {
			if c.DeviceClass != deviceClass {
				// This OSD have another device_class - exclude it
				continue
			}
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

var savedPgDumpPgsBrief []*pgBriefItem

func pgDumpPgsBrief() []*pgBriefItem {
	if len(savedPgDumpPgsBrief) > 0 {
		return savedPgDumpPgsBrief
	}

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

	for _, pgb := range pgBriefs {
		reorderUpToMatchActing(pgb.PgID, pgb.Up, pgb.Acting, true)
	}

	savedPgDumpPgsBrief = pgBriefs
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
		if osdid == invalidOSD {
			continue
		}
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

func pgBriefMap() map[string]*pgBriefItem {
	pgBriefs := pgDumpPgsBrief()

	pgBriefMap := make(map[string]*pgBriefItem)
	for _, pgb := range pgBriefs {
		pgBriefMap[pgb.PgID] = pgb
	}

	return pgBriefMap
}

// Re-order the up list so that any OSDs in it that are also in the
// acting list are in the same place. We also need to take into account
// upmap items which create relationships between the up and acting
// OSDs. This should never do anything for EC pools, where the order
// matters and won't change, but for replicated pools the order can
// change and this doesn't imply data movement.
func reorderUpToMatchActing(pgid string, up, acting []int, useUpmap bool) {
	// Do not reorder if the PG belongs to an Erasure-Coded pool,
	// since order DOES matter and will trigger backfills.
	pools := osdPoolDetails()
	if pools.PgUsesEC(pgid) {
		return
	}

	mappings := make(map[int]int)
	if useUpmap {
		if pui, ok := pgUpmapItemMap()[pgid]; ok {
			mappings = pui.mappingsAsToFromMap()
		}
	}

	swapUp := func(i1 int, i2 int) {
		if i1 != i2 {
			tmp := up[i1]
			up[i1] = up[i2]
			up[i2] = tmp
		}
	}

	for ai, actOsd := range acting {
		for ui, upOsd := range up {
			if upOsd == actOsd {
				swapUp(ui, ai)
				break
			}
			// If this PG is in backfill, it could be because of an
			// upmap item. Find any such matching mapping and
			// consider its source OSD when matching against this
			// acting OSD.
			if from, ok := mappings[upOsd]; ok && from == actOsd {
				swapUp(ui, ai)
				break
			}
		}
	}
}

var savedOsdDumpOut *osdDumpOut

func osdDump() *osdDumpOut {
	if savedOsdDumpOut != nil {
		return savedOsdDumpOut
	}

	var out osdDumpOut

	jsonOut, err := runOsdDump()
	mustParseCephCommand(jsonOut, err, &out)

	savedOsdDumpOut = &out
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

var savedParsedOsdTree *parsedOsdTree

func osdTree() *parsedOsdTree {
	if savedParsedOsdTree != nil {
		return savedParsedOsdTree
	}

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
			ID:          n.ID,
			DeviceClass: n.DeviceClass,
			Name:        n.Name,
			Type:        n.Type,
			Reweight:    n.Reweight,
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

	savedParsedOsdTree = tree
	return tree
}

var savedOsdPoolsDetails *poolsDetails

// query and parse the full Ceph pool details
func osdPoolDetails() *poolsDetails {
	if savedOsdPoolsDetails != nil {
		return savedOsdPoolsDetails
	}

	var pools []*osdPoolDetail
	var poolsMap map[int]*osdPoolDetail

	jsonOut, err := runOsdPoolLs()
	mustParseCephCommand(jsonOut, err, &pools)

	poolsMap = make(map[int]*osdPoolDetail)
	for _, pool := range pools {
		poolsMap[pool.ID] = pool
	}

	savedOsdPoolsDetails = &poolsDetails{Pools: poolsMap}
	return savedOsdPoolsDetails
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
