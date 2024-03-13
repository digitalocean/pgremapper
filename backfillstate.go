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
	"math"
)

type osdBackfillState struct {
	// This mimics Ceph's internal naming for reservations. A local
	// backfill reservation is one taken on the primary OSD for a given PG.
	// A remote reservation is one taken on a backfill target.
	localReservations  int
	remoteReservations int

	// The configured max backfill reservations for this OSD. If -1, then
	// the default in backfillState is used.
	maxBackfillReservations int

	// The number of backfills in which this OSD is a source.
	// TODO: We don't account for degraded backfills today, where EC PGs
	// will use multiple OSDs to reconstruct the data instead of reading
	// the chunk from a single source OSD. There may be other cases where
	// this occurs as well (e.g., what happens when there are multiple
	// backfill targets?).
	backfillsFrom int
}

type backfillState struct {
	osds map[int]*osdBackfillState
	pgbs map[string]*pgBriefItem

	maxBackfillsFrom int
	// The configured default max backfill reservations when not specified
	// for an OSD.
	maxBackfillReservations int
}

func mustGetCurrentBackfillState() *backfillState {
	pgBriefs := pgDumpPgsBrief()
	bs := makeBackfillState()

	for _, pgb := range pgBriefs {
		bs.pgbs[pgb.PgID] = pgb
		bs.addReservations(pgb)
	}
	return bs
}

func makeBackfillState() *backfillState {
	return &backfillState{
		osds: make(map[int]*osdBackfillState),
		pgbs: make(map[string]*pgBriefItem),

		maxBackfillsFrom:        math.MaxInt32,
		maxBackfillReservations: math.MaxInt32,
	}
}

func (bs *backfillState) accountForRemap(pgid string, from, to int) {
	// find from in 'up' set, update it to 'to'; compare this to the acting set (may need to reorderUpToMatchActing()) before and after to see if it a) adds a backfill, b) removes a backfill, or c) makes no change to backfills.
	// maybe we remove the old backfills and recompute the backfill state for this PG from scratch?
	pgb, ok := bs.pgbs[pgid]
	if !ok {
		panic(fmt.Sprintf("%s: no such PG", pgid))
	}

	for i, osd := range pgb.Up {
		if osd == from {
			bs.removeReservations(pgb)
			pgb.Up[i] = to
			// Do not use the upmap here as we don't need to strictly re-order the
			// up set; it's sufficient to consider which OSDs are listed in up and
			// acting by themselves.
			reorderUpToMatchActing(pgid, pgb.Up, pgb.Acting, false)
			bs.addReservations(pgb)
		}
	}
	// We can get here if a remap has been requested where the 'from' OSD
	// is currently down. As noted in the osdBackfillState type TODO, we
	// don't handle degraded backfill today.
	fmt.Printf("pg %s: osd %d not in up set, unable to compute effect of remap on backfill state\n", pgid, from)
}

func (bs *backfillState) addReservations(pgb *pgBriefItem) {
	srcs, tgts := computeBackfillSrcsTgts(pgb)
	for _, osd := range srcs {
		bs.osd(osd).backfillsFrom++
	}
	for _, osd := range tgts {
		bs.osd(osd).remoteReservations++
	}
	if len(tgts) != 0 {
		bs.osd(pgb.primaryOsd()).localReservations++
	}
}

func (bs *backfillState) removeReservations(pgb *pgBriefItem) {
	srcs, tgts := computeBackfillSrcsTgts(pgb)
	for _, osd := range srcs {
		obs := bs.osd(osd)
		if obs.backfillsFrom == 0 {
			panic(fmt.Sprintf("no backfills from remaining on %d", osd))
		}
		obs.backfillsFrom--
	}
	for _, osd := range tgts {
		obs := bs.osd(osd)
		if obs.remoteReservations == 0 {
			panic(fmt.Sprintf("no remote reservations remaining on %d", osd))
		}
		obs.remoteReservations--
	}
	if len(tgts) != 0 {
		obs := bs.osd(pgb.primaryOsd())
		if obs.localReservations == 0 {
			panic(fmt.Sprintf("no local reservations remaining on %d", pgb.primaryOsd()))
		}
		obs.localReservations--
	}
}

func (bs *backfillState) osd(osd int) *osdBackfillState {
	if _, ok := bs.osds[osd]; !ok {
		bs.osds[osd] = &osdBackfillState{
			maxBackfillReservations: -1,
		}
	}
	return bs.osds[osd]
}

func (bs *backfillState) hasRoomForRemap(pgid string, from, to int) bool {
	// TODO: The computations below make the assumption that we're always
	// adding a reservation or source backfill. This will usually be true
	// for the cases where we call this function, but improvement may be
	// worthwhile at some point.

	if bs.osd(from).backfillsFrom >= bs.maxBackfillsFrom {
		return false
	}

	hasRoom := true

	// We apply the change then check to see if we've exceeded maximums
	// anywhere. This is a really cheesy algorithm, but since we're
	// single-threaded it's correct.
	bs.accountForRemap(pgid, from, to)

	pgb := bs.pgbs[pgid]
	primary := pgb.primaryOsd()
	if bs.osd(primary).localReservations > bs.getMaxBackfillReservations(primary) {
		hasRoom = false
	}

	_, tgts := computeBackfillSrcsTgts(pgb)
	for _, osd := range tgts {
		if bs.osd(osd).remoteReservations > bs.getMaxBackfillReservations(osd) {
			hasRoom = false
		}
	}

	bs.accountForRemap(pgid, to, from)

	return hasRoom
}

func (bs *backfillState) getMaxBackfillReservations(osd int) int {
	if obs, ok := bs.osds[osd]; ok && obs.maxBackfillReservations != -1 {
		return obs.maxBackfillReservations
	}
	return bs.maxBackfillReservations
}

func computeBackfillSrcsTgts(pgb *pgBriefItem) ([]int, []int) {
	srcs := []int{}
	tgts := []int{}
	up := pgb.Up
	acting := pgb.Acting

	for i := range acting {
		if up[i] != acting[i] {
			srcs = append(srcs, acting[i])
			tgts = append(tgts, up[i])
		}
	}
	return srcs, tgts
}
