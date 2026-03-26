# LatticeDB Go

Pure Go engine work for LatticeDB.

This repo is being bootstrapped contract-first:

- the engine conformance spec in [docs/engine_conformance.md](docs/engine_conformance.md)
- the logical value model in [docs/value_model.md](docs/value_model.md)
- the extracted conformance suite in [conformance/go](conformance/go)
- adapter boundaries for driver, export, and recovery behavior

The cgo-backed Go client remains in the main `latticedb` repo and serves as the reference behavior while this engine is built out.
