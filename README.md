# pgremapper
![Build](https://github.com/digitalocean/pgremapper/workflows/Go/badge.svg?branch=main) ![License](https://github.com/digitalocean/pgremapper/workflows/License/badge.svg?branch=main) [![Go Report Card](https://goreportcard.com/badge/github.com/digitalocean/pgremapper)](https://goreportcard.com/report/github.com/digitalocean/pgremapper) [![Apache License](https://img.shields.io/hexpm/l/plug)](LICENSE)

When working with Ceph clusters, there are actions that cause backfill (CRUSH map changes) and cases where you want to cause backfill (moving data between OSDs or hosts). Trying to manage backfill via CRUSH is difficult because changes to the CRUSH map cause many ancillary data movements that can be wasteful.

Additionally, controlling the amount of in-progress backfill is difficult, and having PGs in `backfill_wait` state has consequences:
* Any PG performing recovery or backfill must obtain [local and remote reservations](https://docs.ceph.com/en/latest/dev/osd_internals/backfill_reservation/).
* A PG in a wait state may hold some of its necessary reservations, but not all. This may, in turn, block other recoveries or backfills that could otherwise make independent progress.
* For EC pools, the source of a backfill read is likely not the primary, and this is not considered as a part of the reservation scheme. A single OSD could have any number of backfills reading from it; no knobs outside of recovery sleep can be used to mitigate this. Pacific's [mclock scheduler](
https://docs.ceph.com/en/latest/rados/configuration/mclock-config-ref/) should theoretically improve this situation.
* There are no reservation slots held for recoveries, meaning that a recovery could be waiting behind another backfill (or several backfills if they stack in a wait state).

The primary control knob for backfills, `osd-max-backfills`, sets the number of local and remote reservations available on a given OSD. Given the above, this knob is not sufficient given the way that backfill can pile up in the face of a large-scale change; one sometimes has to set it unacceptably high to achieve backfill concurrency across many OSDs.

This tool, `pgremapper`, is intended to aid with all of the above usecases and problems. It operates by manipulating the [pg-upmap](https://docs.ceph.com/en/latest//rados/operations/upmap/) exception table available in Luminous+ to override CRUSH decisions based on a number of algorithms exposed as commands, outlined below. Many of these commands are intended to be run in a loop in order to achieve some target state.

### Acknowledgments

The initial version of this tool, which became the `cancel-backfill` command below, was heavily inspired by [techniques developed by CERN IT](https://www.slideshare.net/Inktank_Ceph/ceph-day-berlin-mastering-ceph-operations-upmap-and-the-mgr-balancer).

### Requirements

As mentioned above, the upmap exception table was introduced in Luminous, and this is a hard requirement for pgremapper. However, there were significant improvements to the upmap code throughout the Luminous series. When working with upmaps, it's recommended that you are running Luminous v12.2.13 (the last release), Mimic v13.2.7+, Nautilus v14.2.5+, or newer, at least on the mons/mgrs.

We've used `pgremapper` on a variety of versions of Luminous and Nautilus.

### Caveats

* When running older versions of Luminous or Mimic, it's possible for stale upmap entries that have no effect to accumulate. `pgremapper` can become confused by these stale entries and fail. See the Requirements section above for recommended versions.
* If the system is still processing osdmaps and peering, `pgremapper` can become confused and fail for pretty much the same reason as above (upmap entries at the mon layer may not yet be reflected in current PG state).
* Given a recent enough Ceph version, CRUSH cannot be violated by an upmap entry. This is good, but it can make certain manipulations impossible; consider a case where a backfill is swapping EC chunks between two racks. To the best of our knowledge today, no upmap entry can be created to counteract such a backfill, as Ceph will evaluate the correctness of the upmap entry in parts, rather than as a whole. (If you have evidence to the contrary or this is actually possible in newer versions of Ceph, let us know!)

### Bug Reports

If you find a situation where `pgremapper` isn't working right, please file a report with a clear description of how pgremapper was invoked and any of its output, what the system was doing at the time, and output from the following Ceph commands:
* `ceph osd dump -f json`
* `ceph osd tree -f json`
* `ceph pg dump pgs_brief -f json`
* If a specific PG is named in `pgremapper` error output, then `ceph pg <pgid> query -f json`

## Building

If you have a Go environment configured, you can use `go get`:
```
go get github.com/digitalocean/pgremapper
```

Otherwise, clone this repository and use a golang Docker container to build:
```
docker run --rm -v $(pwd):/pgremapper -w /pgremapper golang:1.17.2 go build -o pgremapper .
```

## Usage

`pgremapper` makes no changes by default and has some global options:

```
$ ./pgremapper [--concurrency <n>] [--yes] [--verbose] <command>
```

* `--concurrency`: For commands that can be issued in parallel, this controls the concurrency. This is set at a reasonable default that generally doesn't lead to too much concurrent peering in the cluster when manipulating the `pg-upmap` table.
* `--yes`: Apply changes instead of emitting the diff output that would show which changes would be applied.
* `--verbose`: Display Ceph commands being run, for debugging purposes.

### osdspec

For commands or options that take a list of OSDs, `pgremapper` uses the concept of an `osdspec` (inspired by Git's `refspec`) to simplify the command line. An `osdspec` can either be an OSD ID (e.g. `42`) or a CRUSH bucket prefixed by `bucket:` (e.g. `bucket:rack1` or `bucket:host4`). In the latter case, all OSDs found under that CRUSH bucket are included.

### balance-bucket

This is essentially a small, targeted version of Ceph's own upmap balancer (though not as sophisticated - it doesn't prioritize undoing existing `pg-upmap` entries, for example), useful for cases where general enablement of the balancer either isn't possible or is undesirable. The given CRUSH bucket must directly contain OSDs.

```
$ ./pgremapper balance-bucket <bucket> [--max-backfills <n>] [--target-spread <n>]
```

* `<bucket>`: A CRUSH bucket that directly contains OSDs.
* `--max-backfills`: The total number of backfills that should be allowed to be scheduled that affect this CRUSH bucket. This takes pre-existing backfills into account.
* `--target-spread`: The goal state in terms of the maximum difference in PG counts across OSDs in this bucket.

#### Example

Schedule 10 backfills on the host named `data11`, trying to achieve a maximum PG spread of 3 between the fullest and emptiest OSDs (in terms of PG counts) within that host:
```
$ ./pgremapper balance-bucket data11 --max-backfills 10 --target-spread 3
```

### cancel-backfill

This command iterates the list of PGs in a backfill state, creating, modifying, or removing upmap exception table entries to point the PGs back to where they are located now (i.e. makes the `up` set the same as the `acting` set). This essentially reverts whatever decision led to this backfill (i.e. CRUSH change, OSD reweight, or another upmap entry) and leaves the Ceph cluster with no (or very little) remapped PGs (there are cases where Ceph disallows such remapping due to violation of CRUSH rules).

Notably, `pgremapper` knows how to reconstruct the acting set for a degraded backfill (provided that complete copies exist for all indexes of that acting set), which can allow one to convert a `degraded+backfill{ing,_wait}` into `degraded+recover{y,_wait}`, at the cost of losing whatever backfill progress has been made so far.

```
$ ./pgremapper cancel-backfill [--exclude-backfilling] [--include-osds <osdspec>,...] [--exclude-osds <osdspec>,...] [--pgs-including <osdspec>,...]
```

* `--exclude-backfilling`: Constrain cancellation to PGs that are in a `backfill_wait` state, ignoring those in a `backfilling` state.
* `--include-osds`: Cancel backfills containing one of the given OSDs as a backfill source or target only.
* `--exclude-osds`: The inverse of `--include-osds` - cancel backfills that do not contain one of the given OSDs as a backfill source or target.
* `--pgs-including`: Cancel backfills for PGs that include the given OSDs in their up or acting set, whether or not the given OSDs are backfill sources or targets in those PGs.

#### Example - Cancel all backfill in the system as a part of an augment

This is useful during augment scenarios, if you want to control PG movement to the new nodes via the upmap balancer (a technique based on [this CERN talk](https://www.slideshare.net/Inktank_Ceph/ceph-day-berlin-mastering-ceph-operations-upmap-and-the-mgr-balancer).

```
# Make sure no data movement occurs when manipulating the CRUSH map.
$ ceph osd set nobackfill
$ ceph osd set norebalance

<perform augment CRUSH changes>

$ ./pgremapper cancel-backfill --yes

$ ceph osd unset norebalance
$ ceph osd unset nobackfill

<enable the upmap balancer to begin gentle data movements>
```

#### Example - Cancel backfill that has a CRUSH bucket as a source or target, but not backfill including specified OSDs

You may want to reduce backfill load on a given host so that only a few OSDs on that host make progress. This will cancel backfill where host `data04` is a source or target, but not if OSD `21` or `34` is the source or target.
```
$ ./pgremapper cancel-backfill --include-osds bucket:data04 --exclude-osds 21,34
```

#### Example - Cancel backfill for any PGs that include a given host

Due to a failure, we know that `data10` is going to need a bunch of recovery, so let's make sure that the recovery can happen without any backfills entering a degraded state:
```
$ ./pgremapper cancel-backfill --pgs-including bucket:data10
```

### drain

Remap PGs off of the given source OSD, up to the given maximum number of scheduled backfills. No attempt is made to balance the fullness of the target OSDs; rather, the least busy target OSDs and PGs will be selected.

```
$ ./pgremapper drain <source OSD> --target-osds <osdspec>[,<osdspec>] [--allow-movement-across <bucket type>] [--max-backfill-reservations default_max[,osdspec:max]] [--max-source-backfills <n>]
```

* `<source OSD>`: The OSD that will become the backfill source. 
* `--target-osds`: The OSD(s) that will become the backfill target(s).
* `--allow-movement-across`: Constrain which type of data movements will be considered if target OSDs are given outside of the CRUSH bucket that contains the source OSD. For example, if your OSDs all live in a CRUSH bucket of type `host`, passing `host` here will allow remappings across hosts as long as the source and target host live within the same CRUSH bucket themselves. Target CRUSH buckets will be not be considered for a given PG if they already contain replicas/chunks of that PG. By default, if this option isn't given, data movements are allowed only within the direct CRUSH bucket containing the source OSD.
* `--max-backfill-reservations`: Consume only the given reservation maximums for backfill. You'll commonly want to set this below your `osd-max-backfills` setting so that any scheduled recoveries may clear without waiting for a backfill to complete. A default value is specified first, and then per-`osdspec` values for cases where you want to allow more backfill or have non-uniform `osd-max-backfills` settings.
* `--max-source-backfills`: Allow the source OSD to have this maximum number of backfills scheduled. TODO: This option works for EC systems, where the given OSD truly will be the backfill source; in replicated systems, the primary OSD is the source and thus source concurrency must be controlled via `--max-backfill-reservations`.

#### Example - Offload some PGs from one OSD to another

Schedule backfills to move 5 PGs from OSD 4 to OSD 21:
```
$ ./pgremapper drain 4 --target-osds 21 --max-source-backfills 5
```

#### Example - Move PGs off-host

Schedule backfills to move 8 PGs from OSD 15 to any combination of OSDs on host data12, ensuring we don't exceed 2 backfill reservations anywhere:
```
$ ./pgremapper drain 15 --target-osds bucket:data12 --allow-movement-across host --max-backfill-reservations 2 --max-source-backfills 8
```

### export-mappings

Export all upmaps for the given OSD spec(s) in a json format usable by import-mappings. Useful for keeping the state of existing mappings to restore after destroying a number of OSDs, or any other CRUSH change that will cause upmap items to be cleaned up by the mons.

Note that the mappings exported will be just the portions of the upmap items pertaining to the selected OSDs (i.e. if a given OSD is the From or To of the mapping), unless `--whole-pg` is specified.

```
$ ./pgremapper export-mappings <osdspec> ... [--output <file>] [--whole-pg]
```

* `<osdspec> ...`: The OSDs for which mappings will be exported.
* `--output`: Write output to the given file path instead of `stdout`.
* `--whole-pg`: Export all mappings for any PGs that include the given OSD(s), not just the portions pertaining to those OSDs.

### import-mappings

Import all upmaps from the given JSON input (probably from export-mappings) to the cluster. Input is `stdin` unless a file path is provided.

JSON format example, remapping PG 1.1 from OSD 100 to OSD 42:
```
[
  {
    "pgid": "1.1",
    "mapping": {
      "from": 100,
      "to": 42,
    }
  }
]
```

```
$ ./pgremapper import-mappings [<file>]
```

* `<file>`: Read from the given file path instead of `stdin`.

### remap

Modify the upmap exception table with the requested mapping. Like other subcommands, this takes into account any existing mappings for this PG, and is thus safer and more convenient to use than 'ceph osd pg-upmap-items' directly.

```
$ ./pgremapper remap <pg ID> <source osd ID> <target osd ID>
```

### undo-upmaps

Given a list of OSDs, remove (or modify) upmap items such that the OSDs become the source (or target if `--target` is specified) of backfill operations (i.e.  they are currently the "To" ("From") of the upmap items) up to the backfill limits specified. Backfill is spread across target and primary OSDs in a best-effort manner.

This is useful for cases where the upmap rebalancer won't do this for us, e.g., performing a swap-bucket where we want the source OSDs to totally drain (vs. balance with the rest of the cluster). It also achieves a much higher level of concurrency than the balancer generally will.

```
$ ./pgremapper undo-upmaps <osdspec>[,<osdspec>] [--max-backfill-reservations default_max[,osdspec:max]] [--max-source-backfills <n>] [--target]
```

* `--max-backfill-reservations`: Consume only the given reservation maximums for backfill. You'll commonly want to set this below your `osd-max-backfills` setting so that any scheduled recoveries may clear without waiting for a backfill to complete. A default value is specified first, and then per-`osdspec` values for cases where you want to allow more backfill or have non-uniform `osd-max-backfills` settings.
* `--max-source-backfills`: Allow a given source OSD to have this maximum number of backfills scheduled. TODO: This option works for EC systems, where the given OSD truly will be the backfill source; in replicated systems, the primary OSD is the source and thus source concurrency must be controlled via `--max-backfill-reservations`.
* `--target`: The given list of OSDs should serve as backfill targets, rather than the default of backfill sources.

#### Example - Move PGs back after an OSD recreate

A common usecase is reformatting an OSD - we want to move all data off of that OSD to another, recreate the first OSD in the new format, and then move the data back. There was an example above of using `drain` to move data from OSD 4 to OSD 21; now let's start moving it back:
```
$ ./pgremapper undo-upmaps 21 --max-source-backfills 5
```
Or:
```
$ ./pgremapper undo-upmaps 4 --target --max-source-backfills 5
```
(Note that `drain` could be used for this as well, since it will happily remove upmap entries as needed.)

#### Example - Move PGs off of a host after a swap-bucket

Let's say you swapped data01 and data04, where data04 is an empty replacement for data01. You use `cancel-backfill` to revert the swap, and then can start scheduling backfill in controlled batches - 2 per source OSD, not exceeding 2 backfill reservations except for data04 where 3 backfill reservations are allowed (more target concurrency):
```
$ ./pgremapper undo-upmaps bucket:data01 --max-backfill-reservations 2,bucket:data04:3 --max-source-backfills 2
```

# Development

## Testing

Because pgremapper is stateless and should largely make the same decisions each run (modulo some randomization that occurs in remapping commands to ensure a level of fairness), the majority of testing can be done in unit tests that simulate Ceph responses. If you're trying to accomplish something specific while a cluster is in a certain state, the best option is to put a Ceph cluster in that state and capture relevant output from it for unit tests.
