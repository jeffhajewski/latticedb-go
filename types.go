package latticedb

type Value = any

type OpenOptions struct {
	Create           bool
	ReadOnly         bool
	CacheSizeMB      uint32
	PageSize         uint32
	EnableVector     bool
	VectorDimensions uint16
}

type CreateNodeOptions struct {
	Labels     []string
	Properties map[string]Value
}

type CreateEdgeOptions struct {
	Properties map[string]Value
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]Value
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]Value
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]Value
}

type VectorSearchOptions struct {
	K        uint32
	EfSearch uint16
}

type FTSSearchOptions struct {
	Limit         uint32
	MaxDistance   uint32
	MinTermLength uint32
}

type QueryCacheStats struct {
	Entries uint32
	Hits    uint64
	Misses  uint64
}

type VectorSearchResult struct {
	NodeID   uint64
	Distance float32
}

type FTSSearchResult struct {
	NodeID uint64
	Score  float32
}
