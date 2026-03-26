package latticedb

import "github.com/jeffhajewski/latticedb-go/internal/engine"

var ErrReadOnly = engine.ErrReadOnly

type DB struct {
	inner *engine.DB
}

type Tx struct {
	inner *engine.Tx
}

func Open(path string, opts OpenOptions) (*DB, error) {
	inner, err := engine.Open(path, engine.OpenOptions{
		Create:           opts.Create,
		ReadOnly:         opts.ReadOnly,
		CacheSizeMB:      opts.CacheSizeMB,
		PageSize:         opts.PageSize,
		EnableVector:     opts.EnableVector,
		VectorDimensions: opts.VectorDimensions,
	})
	if err != nil {
		return nil, err
	}
	return &DB{inner: inner}, nil
}

func (db *DB) Close() error {
	return db.inner.Close()
}

func (db *DB) Begin(readOnly bool) (*Tx, error) {
	tx, err := db.inner.Begin(readOnly)
	if err != nil {
		return nil, err
	}
	return &Tx{inner: tx}, nil
}

func (db *DB) View(fn func(*Tx) error) error {
	return db.inner.View(func(tx *engine.Tx) error {
		return fn(&Tx{inner: tx})
	})
}

func (db *DB) Update(fn func(*Tx) error) error {
	return db.inner.Update(func(tx *engine.Tx) error {
		return fn(&Tx{inner: tx})
	})
}

func (db *DB) Query(query string, params map[string]Value) (QueryResult, error) {
	result, err := db.inner.Query(query, params)
	if err != nil {
		return QueryResult{}, err
	}
	return convertQueryResult(result), nil
}

func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	results, err := db.inner.VectorSearch(vector, engine.VectorSearchOptions{
		K:        opts.K,
		EfSearch: opts.EfSearch,
	})
	if err != nil {
		return nil, err
	}
	out := make([]VectorSearchResult, len(results))
	for i, result := range results {
		out[i] = VectorSearchResult{
			NodeID:   result.NodeID,
			Distance: result.Distance,
		}
	}
	return out, nil
}

func (db *DB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	results, err := db.inner.FTSSearch(query, engine.FTSSearchOptions{
		Limit:         opts.Limit,
		MaxDistance:   opts.MaxDistance,
		MinTermLength: opts.MinTermLength,
	})
	if err != nil {
		return nil, err
	}
	out := make([]FTSSearchResult, len(results))
	for i, result := range results {
		out[i] = FTSSearchResult{
			NodeID: result.NodeID,
			Score:  result.Score,
		}
	}
	return out, nil
}

func (db *DB) CacheClear() error {
	return db.inner.CacheClear()
}

func (db *DB) CacheStats() (QueryCacheStats, error) {
	stats, err := db.inner.CacheStats()
	if err != nil {
		return QueryCacheStats{}, err
	}
	return QueryCacheStats{
		Entries: stats.Entries,
		Hits:    stats.Hits,
		Misses:  stats.Misses,
	}, nil
}

func (tx *Tx) Commit() error {
	return tx.inner.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.inner.Rollback()
}

func (tx *Tx) CreateNode(opts CreateNodeOptions) (Node, error) {
	node, err := tx.inner.CreateNode(engine.CreateNodeOptions{
		Labels:     cloneStrings(opts.Labels),
		Properties: opts.Properties,
	})
	if err != nil {
		return Node{}, err
	}
	return convertNode(node), nil
}

func (tx *Tx) DeleteNode(nodeID uint64) error {
	return tx.inner.DeleteNode(nodeID)
}

func (tx *Tx) NodeExists(nodeID uint64) (bool, error) {
	return tx.inner.NodeExists(nodeID)
}

func (tx *Tx) GetNode(nodeID uint64) (*Node, error) {
	node, err := tx.inner.GetNode(nodeID)
	if err != nil || node == nil {
		return nil, err
	}
	converted := convertNode(*node)
	return &converted, nil
}

func (tx *Tx) SetProperty(nodeID uint64, key string, value Value) error {
	return tx.inner.SetProperty(nodeID, key, value)
}

func (tx *Tx) GetProperty(nodeID uint64, key string) (Value, bool, error) {
	return tx.inner.GetProperty(nodeID, key)
}

func (tx *Tx) SetVector(nodeID uint64, key string, vector []float32) error {
	return tx.inner.SetVector(nodeID, key, vector)
}

func (tx *Tx) FTSIndex(nodeID uint64, text string) error {
	return tx.inner.FTSIndex(nodeID, text)
}

func (tx *Tx) CreateEdge(sourceID uint64, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	edge, err := tx.inner.CreateEdge(sourceID, targetID, edgeType, engine.CreateEdgeOptions{
		Properties: opts.Properties,
	})
	if err != nil {
		return Edge{}, err
	}
	return convertEdge(edge), nil
}

func (tx *Tx) GetEdgeProperty(edgeID uint64, key string) (Value, bool, error) {
	return tx.inner.GetEdgeProperty(edgeID, key)
}

func (tx *Tx) SetEdgeProperty(edgeID uint64, key string, value Value) error {
	return tx.inner.SetEdgeProperty(edgeID, key, value)
}

func (tx *Tx) RemoveEdgeProperty(edgeID uint64, key string) error {
	return tx.inner.RemoveEdgeProperty(edgeID, key)
}

func (tx *Tx) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	edges, err := tx.inner.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

func convertNode(node engine.Node) Node {
	return Node{
		ID:         node.ID,
		Labels:     cloneStrings(node.Labels),
		Properties: cloneValueMap(node.Properties),
	}
}

func convertEdge(edge engine.Edge) Edge {
	return Edge{
		ID:         edge.ID,
		SourceID:   edge.SourceID,
		TargetID:   edge.TargetID,
		Type:       edge.Type,
		Properties: cloneValueMap(edge.Properties),
	}
}

func convertQueryResult(result engine.QueryResult) QueryResult {
	rows := make([]map[string]Value, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = cloneValueMap(row)
	}
	return QueryResult{
		Columns: cloneStrings(result.Columns),
		Rows:    rows,
	}
}

func cloneValueMap(in map[string]any) map[string]Value {
	if len(in) == 0 {
		return map[string]Value{}
	}
	out := make(map[string]Value, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
