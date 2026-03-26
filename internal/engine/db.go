package engine

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/jeffhajewski/latticedb-go/internal/search"
	"github.com/jeffhajewski/latticedb-go/internal/store"
)

var ErrReadOnly = errors.New("database is read-only")

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
	Properties map[string]any
}

type CreateEdgeOptions struct {
	Properties map[string]any
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]any
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]any
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]any
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

type DB struct {
	mu               sync.RWMutex
	path             string
	graph            *store.GraphState
	nextNodeID       uint64
	nextEdgeID       uint64
	readOnly         bool
	enableVector     bool
	vectorDimensions uint16
	queryCache       map[string]struct{}
	cacheHits        uint64
	cacheMisses      uint64
	closed           bool
}

type Tx struct {
	db       *DB
	readOnly bool
	graph    *store.GraphState
	closed   bool
}

func Open(path string, opts OpenOptions) (*DB, error) {
	graph, nextNodeID, nextEdgeID, err := store.LoadGraphState(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if !opts.Create {
			return nil, err
		}
		graph = store.NewGraphState()
		nextNodeID = 1
		nextEdgeID = 1
		if err := store.PersistGraphState(path, graph, nextNodeID, nextEdgeID); err != nil {
			return nil, err
		}
	}

	return &DB{
		path:             path,
		graph:            graph,
		nextNodeID:       nextNodeID,
		nextEdgeID:       nextEdgeID,
		readOnly:         opts.ReadOnly,
		enableVector:     opts.EnableVector,
		vectorDimensions: opts.VectorDimensions,
		queryCache:       map[string]struct{}{},
	}, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	if !db.readOnly {
		if err := store.PersistGraphState(db.path, db.graph, db.nextNodeID, db.nextEdgeID); err != nil {
			return err
		}
	}
	db.closed = true
	return nil
}

func (db *DB) Begin(readOnly bool) (*Tx, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, errors.New("database is closed")
	}
	if !readOnly && db.readOnly {
		return nil, ErrReadOnly
	}

	return &Tx{
		db:       db,
		readOnly: readOnly,
		graph:    store.CloneGraphState(db.graph),
	}, nil
}

func (db *DB) View(fn func(*Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

func (db *DB) Update(fn func(*Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) Query(query string, params map[string]any) (QueryResult, error) {
	db.touchQueryCache(query)

	plan, err := parseQuery(query)
	if err != nil {
		return QueryResult{}, err
	}

	if plan.mutates() {
		var result QueryResult
		err := db.Update(func(tx *Tx) error {
			var execErr error
			result, execErr = plan.execute(tx, params)
			return execErr
		})
		return result, err
	}

	var result QueryResult
	err = db.View(func(tx *Tx) error {
		var execErr error
		result, execErr = plan.execute(tx, params)
		return execErr
	})
	return result, err
}

func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	queryVector := append([]float32(nil), vector...)
	results := make([]VectorSearchResult, 0)

	err := db.View(func(tx *Tx) error {
		for _, nodeID := range store.SortedNodeIDs(tx.graph) {
			node := tx.graph.Nodes[nodeID]
			vectorValue, ok := search.FirstVectorProperty(node.Properties)
			if !ok {
				continue
			}
			distance, err := search.VectorDistance(vectorValue, queryVector)
			if err != nil {
				return err
			}
			results = append(results, VectorSearchResult{NodeID: node.ID, Distance: distance})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(results, func(a VectorSearchResult, b VectorSearchResult) int {
		if a.Distance < b.Distance {
			return -1
		}
		if a.Distance > b.Distance {
			return 1
		}
		if a.NodeID < b.NodeID {
			return -1
		}
		if a.NodeID > b.NodeID {
			return 1
		}
		return 0
	})

	if opts.K > 0 && len(results) > int(opts.K) {
		results = results[:opts.K]
	}
	return results, nil
}

func (db *DB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	results := make([]FTSSearchResult, 0)
	terms := search.Tokenize(query)

	err := db.View(func(tx *Tx) error {
		for _, nodeID := range store.SortedNodeIDs(tx.graph) {
			node := tx.graph.Nodes[nodeID]
			text, ok := node.Properties[store.FTSTextKey].(string)
			if !ok {
				continue
			}
			score := search.FTSScore(text, terms)
			if score <= 0 {
				continue
			}
			results = append(results, FTSSearchResult{NodeID: node.ID, Score: score})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	slices.SortFunc(results, func(a FTSSearchResult, b FTSSearchResult) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		if a.NodeID < b.NodeID {
			return -1
		}
		if a.NodeID > b.NodeID {
			return 1
		}
		return 0
	})

	limit := opts.Limit
	if limit == 0 {
		return results, nil
	}
	if len(results) > int(limit) {
		results = results[:limit]
	}
	return results, nil
}

func (db *DB) CacheClear() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.queryCache = map[string]struct{}{}
	db.cacheHits = 0
	db.cacheMisses = 0
	return nil
}

func (db *DB) CacheStats() (QueryCacheStats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return QueryCacheStats{
		Entries: uint32(len(db.queryCache)),
		Hits:    db.cacheHits,
		Misses:  db.cacheMisses,
	}, nil
}

func (db *DB) touchQueryCache(query string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.queryCache[query]; ok {
		db.cacheHits++
		return
	}
	db.queryCache[query] = struct{}{}
	db.cacheMisses++
}

func (tx *Tx) Commit() error {
	if tx.closed {
		return nil
	}
	if tx.readOnly {
		tx.closed = true
		return nil
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.db.closed {
		return errors.New("database is closed")
	}
	if tx.db.readOnly {
		return ErrReadOnly
	}

	tx.db.graph = tx.graph
	if err := store.PersistGraphState(tx.db.path, tx.db.graph, tx.db.nextNodeID, tx.db.nextEdgeID); err != nil {
		return err
	}
	tx.closed = true
	return nil
}

func (tx *Tx) Rollback() error {
	tx.closed = true
	return nil
}

func (tx *Tx) CreateNode(opts CreateNodeOptions) (Node, error) {
	if err := tx.ensureWritable(); err != nil {
		return Node{}, err
	}
	if err := store.ValidateCreateLabels(opts.Labels); err != nil {
		return Node{}, err
	}

	props, err := store.NormalizeProperties(opts.Properties)
	if err != nil {
		return Node{}, err
	}

	id := tx.db.allocateNodeID()
	record := &store.NodeRecord{
		ID:         id,
		Labels:     slices.Clone(opts.Labels),
		Properties: props,
	}
	tx.graph.Nodes[id] = record
	return publicNode(record), nil
}

func (tx *Tx) DeleteNode(nodeID uint64) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	delete(tx.graph.Nodes, nodeID)
	for edgeID, edge := range tx.graph.Edges {
		if edge.SourceID == nodeID || edge.TargetID == nodeID {
			delete(tx.graph.Edges, edgeID)
		}
	}
	return nil
}

func (tx *Tx) NodeExists(nodeID uint64) (bool, error) {
	_, ok := tx.graph.Nodes[nodeID]
	return ok, nil
}

func (tx *Tx) GetNode(nodeID uint64) (*Node, error) {
	node, ok := tx.graph.Nodes[nodeID]
	if !ok {
		return nil, nil
	}
	public := publicNode(node)
	return &public, nil
}

func (tx *Tx) SetProperty(nodeID uint64, key string, value any) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return err
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return err
	}
	node.Properties[key] = normalized
	return nil
}

func (tx *Tx) GetProperty(nodeID uint64, key string) (any, bool, error) {
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return nil, false, err
	}
	value, ok := node.Properties[key]
	if !ok {
		return nil, false, nil
	}
	return store.CloneValue(value), true, nil
}

func (tx *Tx) SetVector(nodeID uint64, key string, vector []float32) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if tx.db.vectorDimensions > 0 && len(vector) != int(tx.db.vectorDimensions) {
		return fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), tx.db.vectorDimensions)
	}
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return err
	}
	node.Properties[key] = append([]float32(nil), vector...)
	return nil
}

func (tx *Tx) FTSIndex(nodeID uint64, text string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	node, err := tx.requireNode(nodeID)
	if err != nil {
		return err
	}
	node.Properties[store.FTSTextKey] = text
	return nil
}

func (tx *Tx) CreateEdge(sourceID uint64, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	if err := tx.ensureWritable(); err != nil {
		return Edge{}, err
	}
	if _, err := tx.requireNode(sourceID); err != nil {
		return Edge{}, err
	}
	if _, err := tx.requireNode(targetID); err != nil {
		return Edge{}, err
	}

	props, err := store.NormalizeProperties(opts.Properties)
	if err != nil {
		return Edge{}, err
	}

	id := tx.db.allocateEdgeID()
	record := &store.EdgeRecord{
		ID:         id,
		SourceID:   sourceID,
		TargetID:   targetID,
		Type:       edgeType,
		Properties: props,
	}
	tx.graph.Edges[id] = record
	return publicEdge(record), nil
}

func (tx *Tx) GetEdgeProperty(edgeID uint64, key string) (any, bool, error) {
	edge, err := tx.requireEdge(edgeID)
	if err != nil {
		return nil, false, err
	}
	value, ok := edge.Properties[key]
	if !ok {
		return nil, false, nil
	}
	return store.CloneValue(value), true, nil
}

func (tx *Tx) SetEdgeProperty(edgeID uint64, key string, value any) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	edge, err := tx.requireEdge(edgeID)
	if err != nil {
		return err
	}
	normalized, err := store.NormalizeValue(value)
	if err != nil {
		return err
	}
	edge.Properties[key] = normalized
	return nil
}

func (tx *Tx) RemoveEdgeProperty(edgeID uint64, key string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	edge, err := tx.requireEdge(edgeID)
	if err != nil {
		return err
	}
	delete(edge.Properties, key)
	return nil
}

func (tx *Tx) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	if _, err := tx.requireNode(nodeID); err != nil {
		return nil, err
	}
	results := make([]Edge, 0)
	for _, edgeID := range store.SortedEdgeIDs(tx.graph) {
		edge := tx.graph.Edges[edgeID]
		if edge.SourceID != nodeID {
			continue
		}
		results = append(results, publicEdge(edge))
	}
	return results, nil
}

func (tx *Tx) ensureWritable() error {
	if tx.closed {
		return errors.New("transaction is closed")
	}
	if tx.readOnly {
		return ErrReadOnly
	}
	if tx.db.readOnly {
		return ErrReadOnly
	}
	return nil
}

func (tx *Tx) requireNode(nodeID uint64) (*store.NodeRecord, error) {
	if tx.closed {
		return nil, errors.New("transaction is closed")
	}
	node, ok := tx.graph.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	return node, nil
}

func (tx *Tx) requireEdge(edgeID uint64) (*store.EdgeRecord, error) {
	if tx.closed {
		return nil, errors.New("transaction is closed")
	}
	edge, ok := tx.graph.Edges[edgeID]
	if !ok {
		return nil, fmt.Errorf("edge %d not found", edgeID)
	}
	return edge, nil
}

func (db *DB) allocateNodeID() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	id := db.nextNodeID
	db.nextNodeID++
	return id
}

func (db *DB) allocateEdgeID() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	id := db.nextEdgeID
	db.nextEdgeID++
	return id
}

func publicNode(node *store.NodeRecord) Node {
	return Node{
		ID:         node.ID,
		Labels:     slices.Clone(node.Labels),
		Properties: store.ClonePropertyMap(node.Properties),
	}
}

func publicEdge(edge *store.EdgeRecord) Edge {
	return Edge{
		ID:         edge.ID,
		SourceID:   edge.SourceID,
		TargetID:   edge.TargetID,
		Type:       edge.Type,
		Properties: store.ClonePropertyMap(edge.Properties),
	}
}
