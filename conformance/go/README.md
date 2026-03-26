# Go Conformance Suite

This package contains the extracted engine-level conformance suite for `latticedb-go`.

It is intended to be run against adapter implementations supplied by this repo:

- a `Driver` for the public database surface
- an `Exporter` for dump/export behavior
- a `RecoveryHarness` for crash/recovery simulation

Until those adapters are wired up, the suite compiles and skips.

## Current Coverage

The suite covers:

- persistence across reopen
- stable monotonic edge identity across rollback and reopen
- nested value round-trips
- missing versus stored `NULL` semantics in direct property APIs
- read-only rejection, own-write visibility, commit visibility to new transactions, and rollback cleanup
- query mutation atomicity
- query `SET ... = null` removal semantics
- parallel-edge targeting and single-edge deletion via stable edge ID
- direct vector search and full-text search
- vector and full-text query operators preserving additional `MATCH` bindings
- query cache management behavior
- crash recovery of committed graph state and committed node-property updates
- export and dump invariants

## Running

```bash
cd conformance/go
go test ./...
```

When no adapters are configured, the suite will skip.
