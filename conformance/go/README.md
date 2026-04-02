# Go Conformance Suite

This package contains the extracted engine-level conformance suite for `latticedb-go`.

It runs against adapter implementations supplied by this repo:

- a `Driver` for the public database surface
- an `Exporter` for dump/export behavior
- a `RecoveryHarness` for crash/recovery simulation

## Current Coverage

The suite covers:

- persistence across reopen
- label insertion-order round-trip in direct APIs, multi-label query order independence, and unlabeled nodes
- stable monotonic edge identity across rollback and reopen
- nested value round-trips
- query projection and `UNWIND` preserving bytes, vectors, and nested values
- missing versus stored `NULL` semantics in direct property APIs
- `IS NULL` / `IS NOT NULL` semantics over query property access results
- directional traversal and empty results for unknown relationship types
- read-only rejection, own-write visibility, commit visibility to newly started transactions, and rollback cleanup
- query mutation atomicity
- query property-map replacement, property-map merge, and `SET ... = null` / `REMOVE` semantics
- parallel-edge targeting and single-edge deletion via stable edge ID
- direct vector search and full-text search, including fuzzy-search permissiveness
- vector and full-text query operators preserving additional `MATCH` bindings, row multiplicity, and `AND` filters
- query cache management behavior
- crash recovery of committed graph state, secondary labels, and committed node/edge-property updates
- canonical dump/export invariants

The suite intentionally does not freeze overlapping live writer behavior. Portable callers should serialize write transactions that may touch the same logical record, and future engines are free to provide stronger conflict detection or isolation than the current reference engine exposes publicly.

## Running

```bash
cd conformance/go
go test ./...
```

## Future Direction

The current adapters live in:

- `latticedb_test.go` for the public database, export, and recovery surfaces
- `exporter_test.go` for the export adapter interface
- `recovery_test.go` for the crash harness interface

Future alternative implementations should be able to run this same suite by satisfying the same local interfaces.
