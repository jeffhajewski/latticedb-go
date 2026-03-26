# Value Model

`latticedb-go` targets the same logical value model as the reference engine:

- `NULL`
- `BOOL`
- `INT`
- `FLOAT`
- `STRING`
- `BYTES`
- `VECTOR`
- `LIST`
- `MAP`

Required semantics:

- lists are ordered and heterogeneous
- maps are string-keyed nested values
- nested values may contain other nested values recursively
- duplicate map keys are invalid input at public API boundaries
- bytes remain bytes
- vectors remain vectors
- query materialization must not coerce bytes into strings
- `UNWIND` and query projection must preserve nested values and vectors
