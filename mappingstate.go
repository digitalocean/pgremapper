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
	"sort"
	"strings"
	"sync"

	"github.com/fatih/color"
)

// changeStateType determines if changes can and should happen
type changeStateType int

const (
	// NoChange -> no upmap changes
	NoChange changeStateType = iota
	// NoReservationAvailable -> upmap change should happen but no backfill reservation is available
	NoReservationAvailable
	// ChangesPending -> upmap changes are available
	ChangesPending
)

type mappingState struct {
	pgUpmapItems []*pgUpmapItem // This is always sorted for predictability and repeatability.
	bs           *backfillState
	changeState  changeStateType

	l sync.Mutex
}

func updateChangeState(wantedState changeStateType) changeStateType {
	if wantedState > M.changeState {
		return wantedState
	}
	return M.changeState
}

func mustGetCurrentMappingState() *mappingState {
	osdDumpOut := osdDump()
	items := osdDumpOut.PgUpmapItems
	sort.Slice(items, func(i, j int) bool { return items[i].PgID < items[j].PgID })
	return &mappingState{
		pgUpmapItems: osdDumpOut.PgUpmapItems,
		bs:           mustGetCurrentBackfillState(),
	}
}

func (m *mappingState) remap(pgid string, from, to int) {
	m.l.Lock()
	defer m.l.Unlock()

	pui := m.findOrMakeUpmapItem(pgid)
	for _, m := range pui.Mappings {
		if m.From == from && m.To == to {
			// Duplicate - ignore
			return
		}
	}

	m.bs.accountForRemap(pgid, from, to)

	pui.dirty = true
	m.changeState = ChangesPending

	for i, m := range pui.Mappings {
		if m.From == to && m.To == from {
			// This mapping is the exact opposite of what we want -
			// simply remove it.
			pui.Mappings[i].dirty = true
			pui.removedMappings = append(pui.removedMappings, pui.Mappings[i])
			pui.Mappings = append(pui.Mappings[0:i], pui.Mappings[i+1:]...)
			return
		}
		if m.To == from {
			// Modify this mapping to point to the new destination.
			pui.Mappings[i].dirty = true
			pui.removedMappings = append(pui.removedMappings, pui.Mappings[i])
			pui.Mappings[i].To = to
			return
		}
		if m.From == to || m.From == from || m.To == to {
			panic(fmt.Sprintf("pg %s: conflicting mapping(s) found when trying to map from %d to %d", pgid, from, to))
		}
	}

	// No existing mapping was found; add a new one.
	pui.Mappings = append(pui.Mappings, mapping{From: from, To: to, dirty: true})
}

func (m *mappingState) findOrMakeUpmapItem(pgid string) *pgUpmapItem {
	puis := m.pgUpmapItems
	i := sort.Search(len(puis), func(i int) bool { return m.pgUpmapItems[i].PgID >= pgid })
	if i < len(puis) && puis[i].PgID == pgid {
		return puis[i]
	}

	// Sorted insertion.
	pui := &pgUpmapItem{
		PgID: pgid,
	}
	puis = append(puis, &pgUpmapItem{})
	copy(puis[i+1:], puis[i:])
	puis[i] = pui
	m.pgUpmapItems = puis

	return pui
}

type mappingFilter func(*pgUpmapItem, mapping) bool

func withPgid(pgid string) mappingFilter {
	return func(pui *pgUpmapItem, _ mapping) bool {
		return pui.PgID == pgid
	}
}

func withFrom(from int) mappingFilter {
	return func(_ *pgUpmapItem, m mapping) bool {
		return m.From == from
	}
}

func withTo(to int) mappingFilter {
	return func(_ *pgUpmapItem, m mapping) bool {
		return m.To == to
	}
}

func mfAnd(filters ...mappingFilter) mappingFilter {
	return func(pui *pgUpmapItem, m mapping) bool {
		for _, f := range filters {
			if !f(pui, m) {
				return false
			}
		}
		return true
	}
}

func mfOr(filters ...mappingFilter) mappingFilter {
	return func(pui *pgUpmapItem, m mapping) bool {
		for _, f := range filters {
			if f(pui, m) {
				return true
			}
		}
		return false
	}
}

func (m *mappingState) iterateMappings(f func(pgid string, mp mapping), filter mappingFilter) {
	m.l.Lock()
	defer m.l.Unlock()

	for _, pui := range m.pgUpmapItems {
		for _, mp := range pui.Mappings {
			if filter(pui, mp) {
				f(pui.PgID, mp)
			}
		}
	}
}

type pgMapping struct {
	PgID    string  `json:"pgid"`
	Mapping mapping `json:"mapping"`
}

func (m *mappingState) getMappings(filter mappingFilter) []pgMapping {
	mappings := []pgMapping{}

	m.iterateMappings(func(pgid string, mp mapping) {
		mappings = append(mappings, pgMapping{
			PgID:    pgid,
			Mapping: mp,
		})
	},
		filter,
	)

	return mappings
}

func (m *mappingState) dirtyUpmapItems() []*pgUpmapItem {
	m.l.Lock()
	defer m.l.Unlock()

	items := []*pgUpmapItem{}

	for _, pui := range m.pgUpmapItems {
		if pui.dirty {
			items = append(items, pui)
		}
	}
	return items
}

func (m *mappingState) apply() {
	wg := sync.WaitGroup{}
	ch := make(chan *pgUpmapItem)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for pui := range ch {
				pui.do()
			}

			wg.Done()
		}()
	}

	for _, pui := range m.dirtyUpmapItems() {
		ch <- pui
	}
	close(ch)

	wg.Wait()
}

func (m *mappingState) String() string {
	strs := []string{}
	for _, pui := range m.dirtyUpmapItems() {
		strs = append(strs, pui.String())
	}
	if len(strs) > 0 {
		strs = append(strs,
			fmt.Sprintf("Color legend (for terminals that support it): %s - %s - %s",
				color.New(color.FgGreen).Sprint("new mapping"),
				color.New(color.FgRed).Sprint("removed mapping"),
				"kept mapping",
			),
		)
	}
	return strings.Join(strs, "\n")
}
