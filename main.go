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
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var gitCommit string

var (
	concurrency int
	yes         bool
	verbose     bool
	// M represents the state of upmap items, based on current state plus
	// whatever modifications have been made.
	M *mappingState

	rootCmd = &cobra.Command{
		Use:   "pgremapper",
		Short: "Use the upmap to manipulate PG mappings (and thus scheduled backfill)",
		Long: `Use the upmap to manipulate PG mappings (and thus scheduled backfill)

For any commands that take an osdspec, one of the following can be given:
* An OSD ID (e.g. '54').
* A CRUSH bucket (e.g. 'bucket:rack1' or 'bucket:host04').
`,
	}

	balanceBucketCmd = &cobra.Command{
		Use:   "balance-bucket <bucket>",
		Short: "Add/modify upmap entries to balance the PG count of OSDs in the given CRUSH bucket.",
		Long: `Add/modify upmap entries to balance the PG count of OSDs in the given CRUSH bucket.

This is essentially a small, targeted version of Ceph's own upmap balancer,
useful for cases where general enablement of the balancer either isn't possible
or is undesirable. The given CRUSH bucket must directly contain OSDs.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("a bucket must be specified")
			}

			if _, err := getOsdsForBucket(args[0], ""); err != nil {
				return errors.Wrapf(err, "error validating '%s' as a bucket containing OSDs", args[0])
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			M = mustGetCurrentMappingState()
			deviceClass := mustGetString(cmd, "device-class")

			osds := mustGetOsdsForBucket(args[0], deviceClass)

			maxBackfills := mustGetInt(cmd, "max-backfills")
			targetSpread := mustGetInt(cmd, "target-spread")

			calcPgMappingsToBalanceOsds(osds, maxBackfills, targetSpread)
			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	cancelBackfillCmd = &cobra.Command{
		Use:   "cancel-backfill",
		Short: "Add Ceph upmap entries to cancel out pending backfill",
		Long: `Add Ceph upmap entries to cancel out pending backfill.

This command iterates the list of PGs in a backfill state, creating, modifying,
or removing upmap exception table entries to point the PGs back to where they
are located now (i.e. makes the 'up' set the same as the 'acting' set). This
essentially reverts whatever decision led to this backfill (i.e. CRUSH change,
OSD reweight, or another upmap entry) and leaves the Ceph cluster with no (or
very little) remapped PGs (there are cases where Ceph disallows such remapping
due to violation of CRUSH rules).

Notably, 'pgremapper' knows how to reconstruct the acting set for a degraded
backfill (provided that complete copies exist for all indexes of that acting
set), which can allow one to convert a 'degraded+backfill{ing,_wait}' into
'degraded+recover{y,_wait}', at the cost of losing whatever backfill progress
has been made so far.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			excludeBackfilling, err := cmd.Flags().GetBool("exclude-backfilling")
			if err != nil {
				panic(errors.WithStack(err))
			}

			source, err := cmd.Flags().GetBool("source")
			if err != nil {
				panic(errors.WithStack(err))
			}

			target, err := cmd.Flags().GetBool("target")
			if err != nil {
				panic(errors.WithStack(err))
			}

			excludedOsds := mustGetOsdSpecSliceMap(cmd, "exclude-osds")
			includedOsds := mustGetOsdSpecSliceMap(cmd, "include-osds")
			pgsIncludingOsds := mustGetOsdSpecSliceMap(cmd, "pgs-including")

			M = mustGetCurrentMappingState()
			calcPgMappingsToUndoBackfill(excludeBackfilling, source, target, excludedOsds, includedOsds, pgsIncludingOsds)
			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	drainCmd = &cobra.Command{
		Use:   "drain <source osd ID>",
		Short: "Drain PGs from the given OSD to the target OSDs.",
		Long: `Drain PGs from the given OSD to the target OSDs.

Remap PGs off of the given source OSD, up to the given maximum number of
scheduled backfills. No attempt is made to balance the fullness of the target
OSDs; rather, the least busy target OSDs and PGs will be selected.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return errors.New("a source OSD must be specified")
			}

			if _, err := strconv.Atoi(args[0]); err != nil {
				return err
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			M = mustGetCurrentMappingState()

			sourceOsd, _ := strconv.Atoi(args[0])
			allowMovementAcrossCrushType := mustGetString(cmd, "allow-movement-across")
			mustParseMaxBackfillReservations(cmd)
			mustParseMaxSourceBackfills(cmd)
			targetOsds := mustGetOsdSpecSlice(cmd, "target-osds")

			tree := osdTree()
			sourceOsdNode, ok := tree.IDToNode[sourceOsd]
			if !ok || sourceOsdNode.Type != "osd" {
				panic(fmt.Errorf("source OSD %d doesn't exist", sourceOsd))
			}

			for _, targetOsd := range targetOsds {
				targetOsdNode, ok := tree.IDToNode[targetOsd]
				if !ok || targetOsdNode.Type != "osd" {
					panic(fmt.Errorf("target OSD %d doesn't exist", targetOsd))
				}
			}

			calcPgMappingsToDrainOsd(
				allowMovementAcrossCrushType,
				sourceOsd,
				targetOsds,
			)
			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	undoUpmapsCmd = &cobra.Command{
		Use:   "undo-upmaps [osd IDs...]",
		Short: "Undo upmap entries for the given source/target OSDs",
		Long: `Undo upmap entries for the given source/target OSDs.

Given a list of OSDs, remove (or modify) upmap items such that the OSDs become
the source (or target if --target is specified) of backfill operations (i.e.
they are currently the "To" ("From") of the upmap items) up to the backfill
limits specified. Backfill is spread across target and primary OSDs in a
best-effort manor.

This is useful for cases where the upmap rebalancer won't do this for us, e.g.,
performing a swap-bucket where we want the source OSDs to totally drain (vs.
balance with the rest of the cluster). It also achieves a much higher level of
concurrency than the balancer generally will.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("at least one OSD must be specified")
			}

			for _, arg := range args {
				if _, err := parseOsdSpec(arg); err != nil {
					return err
				}
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			M = mustGetCurrentMappingState()

			osds := make([]int, 0, len(args))
			for _, arg := range args {
				osdSpecOsds := mustParseOsdSpec(arg)
				osds = append(osds, osdSpecOsds...)
			}

			// Randomize OSD list for fairness across multiple
			// runs.
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(osds), func(i, j int) { osds[i], osds[j] = osds[j], osds[i] })

			target := mustGetBool(cmd, "target")
			mustParseMaxBackfillReservations(cmd)
			mustParseMaxSourceBackfills(cmd)

			calcPgMappingsToUndoUpmaps(osds, target)
			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	remapCmd = &cobra.Command{
		Use:   "remap <pg ID> <source osd ID> <target osd ID>",
		Short: "Remap the given PG from the source OSD to the target OSD.",
		Long: `Remap the given PG from the source OSD to the target OSD.

Modify the upmap exception table with the requested mapping. Like other
subcommands, this takes into account any existing mappings for this PG, and is
thus safer and more convenient to use than 'ceph osd pg-upmap-items' directly.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return errors.New("missing or extra args")
			}

			for i := 1; i < 3; i++ {
				if _, err := strconv.Atoi(args[i]); err != nil {
					return err
				}
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			M = mustGetCurrentMappingState()

			pgID := args[0]
			sourceOsd, _ := strconv.Atoi(args[1])
			targetOsd, _ := strconv.Atoi(args[2])

			M.mustRemap(pgID, sourceOsd, targetOsd)

			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	exportMappingsCommand = &cobra.Command{
		Use:   "export-mappings <osdspec> [<osdspec> ...]",
		Short: "Export the mappings from the given OSD spec(s).",
		Long: `Export the mappings from the given OSD spec(s).

Export all upmaps for the given OSD spec(s) in a json format usable by
import-mappings. Useful for keeping the state of existing mappings to restore
after destroying a number of OSDs, or any other CRUSH change that will cause
upmap items to be cleaned up by the mons.

Note that the mappings exported will be just the portions of the upmap items
pertaining to the selected OSDs (i.e. if a given OSD is the From or To of the
mapping), unless --whole-pg is specified.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("at least one OSD must be specified")
			}

			for _, arg := range args {
				if _, err := parseOsdSpec(arg); err != nil {
					return err
				}
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			var writer io.Writer
			output := mustGetString(cmd, "output")
			if output == "" {
				writer = os.Stdout
			} else {
				f, err := os.Create(output)
				if err != nil {
					panic(err)
				}

				defer f.Close()
				writer = f
			}

			var filters []mappingFilter
			for _, arg := range args {
				osds := mustParseOsdSpec(arg)
				for _, osd := range osds {
					filters = append(filters, withFrom(osd), withTo(osd))
				}
			}

			M = mustGetCurrentMappingState()
			mappings := M.getMappings(mfOr(filters...))

			if mustGetBool(cmd, "whole-pg") {
				// Using the list of mappings from above, query
				// again, this time getting all mappings with
				// the PG IDs from the first query.
				var filters []mappingFilter
				for _, mapping := range mappings {
					filters = append(filters, withPgid(mapping.PgID))
				}
				mappings = M.getMappings(mfOr(filters...))
			}

			if err := json.NewEncoder(writer).Encode(mappings); err != nil {
				panic(err)
			}
		},
	}

	generateCrushMappingsCommand = &cobra.Command{
		Use:   "generate-crush-change-mappings",
		Short: "Export the mappings incurred from making a CRUSHmap change.",
		Long: `Export the mappings incurred from making a CRUSHmap change.

Export all upmaps for a given CRUSHmap change in a json format usable by
import-mappings. Useful for keeping the state of existing mappings to restore
after destroying a number of OSDs, or any other CRUSH change that will cause
upmap items to be cleaned up by the mons.

A typical use-case could be changing a given CRUSH rule to switch chooseleaf
from "osd" to "host", or from "host" to "rack". Once this new CRUSHmap is
injected into the existing OSDMap, a large number of PGs will be subject to
backfill -- that cannot be cancelled. Using this subcommand, we can pregenerate
a list of upmap mappings, that we can gradually import, in order to move PGs
such that they are already conform to the expected spread. Injecting a new
CRUSHmap after this process completes, should largely be a no-op (unless the
cluster undergoes some other major changes).
`,
		Run: func(cmd *cobra.Command, args []string) {
			var writer io.Writer
			cm := mustGetString(cmd, "crushmap-text")
			output := mustGetString(cmd, "output")
			if output == "" {
				writer = os.Stdout
			} else {
				f, err := os.Create(output)
				if err != nil {
					panic(err)
				}
				defer f.Close()

				writer = f
			}

			mappings, err := crushCmp(cm)
			if err != nil {
				panic(err)
			}

			if err := json.NewEncoder(writer).Encode(mappings); err != nil {
				panic(err)
			}
		},
	}

	importMappingsCommand = &cobra.Command{
		Use:   "import-mappings [<file>]",
		Short: "Import and apply mappings.",
		Long: `Import and apply mappings.

Import all upmaps from the given JSON input (probably from export-mappings) to the
cluster. Input is stdin unless a file path is provided.

JSON format example, remapping PG 1.1 from OSD 100 to OSD 42:
[
  {
    "pgid": "1.1",
    "mapping": {
      "from": 100,
      "to": 42,
    }
  }
]
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return errors.New("extra args")
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			// Read in either the file or from stdin
			var reader io.Reader
			if len(args) == 0 {
				reader = os.Stdin
			} else {
				f, err := os.Open(args[0])
				if err != nil {
					panic(err)
				}

				defer f.Close()
				reader = f
			}

			M = mustGetCurrentMappingState()

			var mappings []pgMapping
			if err := json.NewDecoder(reader).Decode(&mappings); err != nil {
				panic(err)
			}

			for _, m := range mappings {
				// There are two cases to consider:
				// 1. The mapping we want to create is simply
				//    gone - in this case, we can re-issue the
				//    remap in its original form.
				// 2. There is now a different upmap item from
				//    the source OSD. We need to find this one
				//    and modify it.
				//
				// Look for case 2 first, falling back to case
				// 1 if we don't find anything.
				pui := M.findOrMakeUpmapItem(m.PgID)
				found := false
				for _, puiM := range pui.Mappings {
					if puiM.From == m.Mapping.From {
						M.mustRemap(m.PgID, puiM.To, m.Mapping.To)
						found = true
						break
					}
				}
				if !found {
					M.mustRemap(m.PgID, m.Mapping.From, m.Mapping.To)
				}
			}

			if !confirmProceed() {
				return
			}

			M.apply()
		},
	}

	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long:  "Print version information",

		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("git sha %s\n", gitCommit)
		},
	}
)

func mustGetBool(cmd *cobra.Command, arg string) bool {
	ret, err := cmd.Flags().GetBool(arg)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return ret
}

func mustGetInt(cmd *cobra.Command, arg string) int {
	ret, err := cmd.Flags().GetInt(arg)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return ret
}

func mustGetString(cmd *cobra.Command, arg string) string {
	ret, err := cmd.Flags().GetString(arg)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return ret
}

func mustGetStringSlice(cmd *cobra.Command, arg string) []string {
	ret, err := cmd.Flags().GetStringSlice(arg)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return ret
}

func mustGetOsdSpecSlice(cmd *cobra.Command, arg string) []int {
	strings := mustGetStringSlice(cmd, arg)

	var osds []int
	for _, s := range strings {
		osdSpecOsds := mustParseOsdSpec(s)
		osds = append(osds, osdSpecOsds...)
	}
	return osds
}

func mustGetOsdSpecSliceMap(cmd *cobra.Command, arg string) map[int]struct{} {
	list := mustGetOsdSpecSlice(cmd, arg)

	ret := make(map[int]struct{})
	for _, v := range list {
		ret[v] = struct{}{}
	}

	return ret
}

func mustParseOsdSpec(s string) []int {
	osds, err := parseOsdSpec(s)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return osds
}

func parseOsdSpec(s string) ([]int, error) {
	errResponse := func(s string) ([]int, error) {
		return nil, errors.New(fmt.Sprintf("'%s' is not a valid osdspec - see root command --help", s))
	}

	osd, err := strconv.Atoi(s)
	if err == nil {
		return []int{osd}, nil
	}

	spl := strings.SplitN(s, ":", 2)
	if len(spl) != 2 {
		return errResponse(s)
	}

	if spl[0] != "bucket" {
		return errResponse(s)
	}

	osds, err := getOsdsForBucket(spl[1], "")
	if err != nil {
		return nil, err
	}

	return osds, nil
}

func mustParseMaxSourceBackfills(cmd *cobra.Command) {
	max := mustGetInt(cmd, "max-source-backfills")
	M.bs.maxBackfillsFrom = max
}

func mustParseMaxBackfillReservations(cmd *cobra.Command) {
	strs := mustGetStringSlice(cmd, "max-backfill-reservations")

	if len(strs) >= 1 {
		max, err := strconv.Atoi(strs[0])
		if err != nil {
			panic(errors.WithStack(err))
		}
		M.bs.maxBackfillReservations = max

		for _, s := range strs[1:] {
			spl := strings.Split(s, ":")
			if len(spl) < 2 {
				panic(errors.WithStack(errors.New(fmt.Sprintf("'%s' is not a valid max-backfill-reservation specifier", s))))
			}

			max, err := strconv.Atoi(spl[len(spl)-1])
			if err != nil {
				panic(errors.WithStack(err))
			}

			osds := mustParseOsdSpec(s[0:strings.LastIndex(s, ":")])
			for _, osd := range osds {
				M.bs.osd(osd).maxBackfillReservations = max
			}
		}
	}
}

func init() {
	rootCmd.PersistentFlags().IntVar(&concurrency, "concurrency", 5, "number of commands to issue in parallel")
	rootCmd.PersistentFlags().BoolVar(&yes, "yes", false, "skip confirmations and dry-run output")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "display Ceph commands being run")

	balanceBucketCmd.Flags().Int("max-backfills", 5, "max number of backfills to schedule for this bucket, including pre-existing ones")
	balanceBucketCmd.Flags().Int("target-spread", 1, "target difference between the fullest and emptiest OSD in the bucket")
	balanceBucketCmd.Flags().String("device-class", "", "device class filter, balance only OSDs with this device class")

	rootCmd.AddCommand(balanceBucketCmd)

	cancelBackfillCmd.Flags().Bool("exclude-backfilling", false, "don't interrupt already-started backfills")
	cancelBackfillCmd.Flags().Bool("source", false, "selects only osds that are backfill sources")
	cancelBackfillCmd.Flags().Bool("target", false, "selects only osds that are backfill targets")
	cancelBackfillCmd.Flags().StringSlice("exclude-osds", []string{}, "list of osdspecs that are backfill sources or targets which will be excluded from backfill cancellation")
	cancelBackfillCmd.Flags().StringSlice("include-osds", []string{}, "list of osdspecs that are backfill sources or targets which will be included in backfill cancellation")
	cancelBackfillCmd.Flags().StringSlice("pgs-including", []string{}, "only PGs that include the given OSDs in their up or acting set will have their backfill canceled, whether or not the given OSDs are backfill sources or targets in those PGs")
	rootCmd.AddCommand(cancelBackfillCmd)

	drainCmd.Flags().String("allow-movement-across", "", "the lowest CRUSH bucket type across which shards/replicas of a PG may move; '' (empty) means that shards/replicas must stay within their current direct bucket (IMPORTANT: this is not validated against your CRUSH rules, so make sure you set it and the target OSDs correctly!)")
	drainCmd.Flags().StringSlice("max-backfill-reservations", []string{}, "limit number of backfill reservations made; format: \"default max[,osdspec:max]\", e.g., \"5,bucket:data10:10\"")
	drainCmd.Flags().Int("max-source-backfills", 1, "max number of backfills to schedule per source OSD, including pre-existing ones")
	drainCmd.Flags().StringSlice("target-osds", []string{}, "list of OSDs that will be used as the target of remappings")
	rootCmd.AddCommand(drainCmd)

	undoUpmapsCmd.Flags().StringSlice("max-backfill-reservations", []string{}, "limit number of backfill reservations made; format: \"default max[,osdspec:max]\", e.g., \"5,bucket:data10:10\"")
	undoUpmapsCmd.Flags().Int("max-source-backfills", 1, "max number of backfills to schedule per source OSD, including pre-existing ones")
	undoUpmapsCmd.Flags().Bool("target", false, "the given OSDs are backfill targets rather than sources")
	rootCmd.AddCommand(undoUpmapsCmd)

	rootCmd.AddCommand(remapCmd)

	exportMappingsCommand.Flags().String("output", "", "write output to the given file path instead of stdout")
	exportMappingsCommand.Flags().Bool("whole-pg", false, "export all mappings for any PGs that include the given OSD(s), not just the portions pertaining to those OSDs")
	rootCmd.AddCommand(exportMappingsCommand)

	generateCrushMappingsCommand.Flags().String("crushmap-text", "", "CRUSHmap, with changes, provided in the text format")
	generateCrushMappingsCommand.Flags().String("output", "", "write output to the given file path instead of stdout")
	rootCmd.AddCommand(generateCrushMappingsCommand)

	rootCmd.AddCommand(importMappingsCommand)

	rootCmd.AddCommand(versionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

func calcPgMappingsToUndoBackfill(excludeBackfilling, source, target bool, excludedOsds, includedOsds, pgsIncludingOsds map[int]struct{}) {
	pgBriefs := pgDumpPgsBrief()

	excluded := func(osd int) bool {
		_, ok := excludedOsds[osd]
		return ok
	}

	// Included is true if the flag isn't supplied
	// or if it is supplied and the OSD is in it
	included := func(osd int) bool {
		_, ok := includedOsds[osd]
		return len(includedOsds) == 0 || ok
	}

	// Run these concurrently in case they need to go to pgQuery, which is
	// quite slow.
	wg := sync.WaitGroup{}
	ch := make(chan *pgBriefItem)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for pgb := range ch {
				id := pgb.PgID
				up := pgb.Up
				acting := pgb.Acting

				if !strings.Contains(pgb.State, "backfill") {
					continue
				}
				if excludeBackfilling && strings.Contains(pgb.State, "backfilling") {
					continue
				}
				if len(up) != len(acting) {
					continue
				}

				// Check if we need to reconstruct the original
				// acting set in the case of a degraded PG.
				for _, osd := range acting {
					if osd == invalidOSD {
						// Reconstruct the original
						// acting set via a PG query.
						pqo := pgQuery(id)
						acting = pqo.getCompletePeers()
						reorderUpToMatchActing(pgb.PgID, up, acting, true)
						break
					}
				}

				if len(pgsIncludingOsds) > 0 {
					include := false
					for _, osd := range append(acting, up...) {
						if _, ok := pgsIncludingOsds[osd]; ok {
							include = true
							break
						}
					}
					if !include {
						continue
					}
				}

				// Calculate acting set difference and remap to
				// avoid any ensuing backfill.
				for i := range acting {
					if up[i] != acting[i] {
						if up[i] == invalidOSD || acting[i] == invalidOSD {
							continue
						}

						if target == source {
							// We'll allow this OSD to be
							// acted on if this PG is the
							// source _or_ target
							if excluded(up[i]) || excluded(acting[i]) {
								continue
							}

							if !(included(up[i]) || included(acting[i])) {
								continue
							}
						} else {
							// If source/target flag is set we will act
							// if the PG is in the source/target
							if source && excluded(up[i]) || target && excluded(acting[i]) {
								continue
							}

							if !(source && included(up[i]) || target && included(acting[i])) {
								continue
							}
						}

						// It is possible that our
						// remap attempt will fail in
						// complex cases where:
						// * An upmap item already
						//   exists for one of the
						//   OSDs.
						// * At least one of the OSDs
						//   appears in both the up and
						//   acting sets.
						// This is a somewhat-common
						// occurrence in EC systems
						// after a CRUSH change has
						// been made at the host, rack,
						// etc. level, and in many of
						// these cases we can't
						// actually use the upmap
						// exception table to cancel
						// the backfill.
						err := M.tryRemap(id, up[i], acting[i])
						if err != nil {
							fmt.Printf("WARNING: %v\n", err)
						}
					}
				}
			}

			wg.Done()
		}()
	}

	for _, pgb := range pgBriefs {
		ch <- pgb
	}

	close(ch)
	wg.Wait()
}

func calcPgMappingsToDrainOsd(
	allowMovementAcrossCrushType string,
	sourceOsd int,
	targetOsds []int,
) {
	candidateMappings := getCandidateMappings(
		allowMovementAcrossCrushType,
		sourceOsd,
		targetOsds,
	)

	for len(candidateMappings) > 0 {
		pgid, ok := remapLeastBusyPg(candidateMappings)
		if !ok {
			break
		}

		// Since this PG has now been remapped, remove it from the candidates.
		newCandidates := []pgMapping{}
		for _, m := range candidateMappings {
			if m.PgID == pgid {
				continue
			}
			newCandidates = append(newCandidates, m)
		}
		candidateMappings = newCandidates
	}
}

func getCandidateMappings(
	allowMovementAcrossCrushType string,
	sourceOsd int,
	targetOsds []int,
) []pgMapping {
	pgs := getUpPGsForOsds([]int{sourceOsd})
	candidateMappings := []pgMapping{}
	for _, pg := range pgs[sourceOsd] {
		for _, targetOsd := range targetOsds {
			if !isCandidateMapping(
				allowMovementAcrossCrushType,
				sourceOsd,
				targetOsd,
				pg,
			) {
				continue
			}
			candidateMappings = append(candidateMappings, pgMapping{
				PgID: pg.PgID,
				Mapping: mapping{
					From: sourceOsd,
					To:   targetOsd,
				},
			})
		}
	}
	return candidateMappings
}

func isCandidateMapping(
	allowMovementAcrossCrushType string,
	sourceOsd int,
	targetOsd int,
	pg *pgBriefItem,
) bool {
	if targetOsd == sourceOsd {
		return false
	}

	tree := osdTree()
	sourceOsdNode := tree.IDToNode[sourceOsd]
	targetOsdNode := tree.IDToNode[targetOsd]

	if allowMovementAcrossCrushType == "" {
		// Data movements must stay within the source's direct CRUSH
		// bucket.
		return targetOsdNode.Parent == sourceOsdNode.Parent
	}

	// Data movements are allowed between buckets of type
	// allowMovementAcrossCrushType as long as they share the next level up
	// in the hierarchy. However, no other OSDs in this PG may be in the
	// same bucket of the target's crushShardBucket.
	sourceCrushParentBucket := sourceOsdNode.mustGetNearestParentOfType(allowMovementAcrossCrushType)
	targetCrushParentBucket := targetOsdNode.mustGetNearestParentOfType(allowMovementAcrossCrushType)
	if sourceCrushParentBucket.Parent != targetCrushParentBucket.Parent {
		return false
	}
	for _, pgUpOsd := range pg.Up {
		if pgUpOsd == sourceOsd {
			continue
		}

		pgUpOsdNode := tree.IDToNode[pgUpOsd]
		pgUpOsdCrushParentBucket := pgUpOsdNode.mustGetNearestParentOfType(allowMovementAcrossCrushType)
		if targetCrushParentBucket == pgUpOsdCrushParentBucket {
			// Moving to this target would put multiple shards in
			// the same bucket, which isn't valid.
			return false
		}
	}
	return true
}

func calcPgMappingsToUndoUpmaps(osds []int, osdsAreTargets bool) {
	// For fairness, iterate the osds, adding one backfill at a time to
	// each candidate, until we don't add any new backfills.
	somethingChanged := true
	for somethingChanged {
		somethingChanged = false

		for _, osd := range osds {
			var candidateMappings []pgMapping
			if osdsAreTargets {
				candidateMappings = M.getMappings(withFrom(osd))
			} else {
				candidateMappings = M.getMappings(withTo(osd))
			}

			// Since we pass these mappings in as candidates for
			// action, reverse the From and To (since we want to
			// undo the associated upmap).
			for i := range candidateMappings {
				mp := &candidateMappings[i].Mapping
				mp.From, mp.To = mp.To, mp.From
			}

			_, ok := remapLeastBusyPg(candidateMappings)
			if !ok {
				continue
			}
			somethingChanged = true
		}
	}
}

func remapLeastBusyPg(candidateMappings []pgMapping) (string, bool) {
	var (
		found       bool
		bestScore   = int(math.MaxInt32)
		bestMapping pgMapping
	)
	// Look for a candidate OSD to remap to that has the lowest reservation
	// score. We consider the remote reservation count (the count of
	// backfills in which this OSD is the target) to be more important than
	// the local reservation count (the count of backfills for which this
	// OSD is primary), and thus apply a weight to it.
	for _, m := range candidateMappings {
		if !M.bs.hasRoomForRemap(m.PgID, m.Mapping.From, m.Mapping.To) {
			M.changeState = updateChangeState(NoReservationAvailable)
			continue
		}

		obs := M.bs.osd(m.Mapping.To)
		score := obs.remoteReservations*10 + obs.localReservations
		if score < bestScore {
			found = true
			bestScore = score
			bestMapping = m
		}
	}
	if !found {
		return "", false
	}

	M.mustRemap(bestMapping.PgID, bestMapping.Mapping.From, bestMapping.Mapping.To)

	return bestMapping.PgID, true
}

func calcPgMappingsToBalanceOsds(osds []int, maxBackfills, targetSpread int) {
	sort.Slice(osds, func(i, j int) bool { return osds[i] < osds[j] })

	osdUpPGs := getUpPGsForOsds(osds)

	osdDumpOut := osdDump()
	for _, o := range osdDumpOut.Osds {
		if pgs, ok := osdUpPGs[o.Osd]; ok && o.In == 0 {
			if len(pgs) != 0 {
				panic(fmt.Sprintf("osd %d is 'out' but has PGs in up set", o.Osd))
			}
			// This OSD is 'out' - exclude it from consideration.
			delete(osdUpPGs, o.Osd)
			continue
		}
	}

	backfillsInSet := 0
	for _, osd := range osds {
		backfillsInSet += M.bs.osd(osd).backfillsFrom
	}

	for backfillsInSet < maxBackfills {
		var (
			lowestOsd, highestOsd int
			lowestLen, highestLen int
		)
		// Get the first 'in' osd.
		for _, osd := range osds {
			if _, ok := osdUpPGs[osd]; !ok {
				continue
			}
			lowestOsd = osd
			lowestLen = len(osdUpPGs[osd])
			highestOsd = osd
			highestLen = len(osdUpPGs[osd])
			break
		}
		for _, osd := range osds {
			pgs, ok := osdUpPGs[osd]
			if !ok {
				continue
			}
			thisLen := len(pgs)
			if thisLen < lowestLen {
				lowestOsd = osd
				lowestLen = thisLen
			}
			if thisLen > highestLen {
				highestOsd = osd
				highestLen = thisLen
			}
		}
		if highestLen-lowestLen <= targetSpread {
			// Balanced enough - all done.
			return
		}

		pg := osdUpPGs[highestOsd][highestLen-1]
		M.mustRemap(pg.PgID, highestOsd, lowestOsd)
		osdUpPGs[lowestOsd] = append(osdUpPGs[lowestOsd], pg)
		osdUpPGs[highestOsd] = osdUpPGs[highestOsd][:highestLen-1]
		backfillsInSet++
	}
}

func getUpPGsForOsds(osds []int) map[int][]*pgBriefItem {
	osdPGs := make(map[int][]*pgBriefItem)
	for _, osd := range osds {
		osdPGs[osd] = nil
	}

	pgBriefs := pgDumpPgsBrief()
	for _, pgBrief := range pgBriefs {
		for _, osd := range pgBrief.Up {
			if _, ok := osdPGs[osd]; ok {
				osdPGs[osd] = append(osdPGs[osd], pgBrief)
				break
			}
		}
	}
	return osdPGs
}

func run(command ...string) (string, error) {
	if verbose {
		fmt.Fprintf(os.Stderr, "** executing: %s\n", strings.Join(command, " "))
	}

	cmd := exec.Command(command[0], command[1:]...)
	stdout, err := cmd.Output()

	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = fmt.Sprintf("\nstderr:\n%s", ee.Stderr)
		}
		return "", errors.Wrapf(err, "failed to execute command: %s%s",
			strings.Join(command, " "), stderr)
	}

	return string(stdout), nil
}

func runCombined(command ...string) (string, error) {
	if verbose {
		fmt.Fprintf(os.Stderr, "** executing: %s\n", strings.Join(command, " "))
	}

	cmd := exec.Command(command[0], command[1:]...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		return "", errors.Wrapf(err, "failed to execute command: %q",
			strings.Join(command, " "))
	}

	return string(out), nil
}

func runOrDie(command ...string) string {
	stdout, err := run(command...)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return stdout
}

func confirmProceed() bool {
	switch M.changeState {
	case NoChange:
		fmt.Fprintf(os.Stderr, "nothing to do\n")
		return false
	case NoReservationAvailable:
		fmt.Fprintf(os.Stderr, "change possible but no backfill reservation available, try later\n")
		return false
	}

	if yes {
		return true
	}

	fmt.Println("The following changes would be made to the upmap exception table:")
	fmt.Println(M.String())
	fmt.Println()
	fmt.Println("No changes made - use --yes to apply changes.")

	return false
}
