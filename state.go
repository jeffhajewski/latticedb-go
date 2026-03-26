package latticedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
)

const (
	stateFileName = "state.json"
	ftsTextKey    = "text"
)

type graphState struct {
	Nodes map[uint64]*nodeRecord
	Edges map[uint64]*edgeRecord
}

type nodeRecord struct {
	ID         uint64
	Labels     []string
	Properties map[string]Value
}

type edgeRecord struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]Value
}

type persistedState struct {
	NextNodeID uint64          `json:"next_node_id"`
	NextEdgeID uint64          `json:"next_edge_id"`
	Nodes      []persistedNode `json:"nodes"`
	Edges      []persistedEdge `json:"edges"`
}

type persistedNode struct {
	ID         uint64                    `json:"id"`
	Labels     []string                  `json:"labels"`
	Properties map[string]persistedValue `json:"properties"`
}

type persistedEdge struct {
	ID         uint64                    `json:"id"`
	SourceID   uint64                    `json:"source_id"`
	TargetID   uint64                    `json:"target_id"`
	Type       string                    `json:"type"`
	Properties map[string]persistedValue `json:"properties"`
}

type persistedValue struct {
	Kind   string                    `json:"kind"`
	Bool   bool                      `json:"bool,omitempty"`
	Int    int64                     `json:"int,omitempty"`
	Float  float64                   `json:"float,omitempty"`
	String string                    `json:"string,omitempty"`
	Bytes  []byte                    `json:"bytes,omitempty"`
	Vector []float32                 `json:"vector,omitempty"`
	List   []persistedValue          `json:"list,omitempty"`
	Map    map[string]persistedValue `json:"map,omitempty"`
}

func newGraphState() *graphState {
	return &graphState{
		Nodes: map[uint64]*nodeRecord{},
		Edges: map[uint64]*edgeRecord{},
	}
}

func stateFilePath(dbPath string) string {
	return filepath.Join(dbPath, stateFileName)
}

func loadGraphState(dbPath string) (*graphState, uint64, uint64, error) {
	data, err := os.ReadFile(stateFilePath(dbPath))
	if err != nil {
		return nil, 0, 0, err
	}

	var snapshot persistedState
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, 0, 0, fmt.Errorf("decode state: %w", err)
	}

	graph := newGraphState()
	for _, storedNode := range snapshot.Nodes {
		props, err := decodePropertyMap(storedNode.Properties)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("decode node %d properties: %w", storedNode.ID, err)
		}
		graph.Nodes[storedNode.ID] = &nodeRecord{
			ID:         storedNode.ID,
			Labels:     slices.Clone(storedNode.Labels),
			Properties: props,
		}
	}
	for _, storedEdge := range snapshot.Edges {
		props, err := decodePropertyMap(storedEdge.Properties)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("decode edge %d properties: %w", storedEdge.ID, err)
		}
		graph.Edges[storedEdge.ID] = &edgeRecord{
			ID:         storedEdge.ID,
			SourceID:   storedEdge.SourceID,
			TargetID:   storedEdge.TargetID,
			Type:       storedEdge.Type,
			Properties: props,
		}
	}

	nextNodeID := snapshot.NextNodeID
	if nextNodeID == 0 {
		nextNodeID = 1
	}
	nextEdgeID := snapshot.NextEdgeID
	if nextEdgeID == 0 {
		nextEdgeID = 1
	}
	return graph, nextNodeID, nextEdgeID, nil
}

func persistGraphState(dbPath string, graph *graphState, nextNodeID uint64, nextEdgeID uint64) error {
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	snapshot := persistedState{
		NextNodeID: nextNodeID,
		NextEdgeID: nextEdgeID,
		Nodes:      make([]persistedNode, 0, len(graph.Nodes)),
		Edges:      make([]persistedEdge, 0, len(graph.Edges)),
	}

	for _, nodeID := range sortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		props, err := encodePropertyMap(node.Properties)
		if err != nil {
			return fmt.Errorf("encode node %d properties: %w", nodeID, err)
		}
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{
			ID:         node.ID,
			Labels:     slices.Clone(node.Labels),
			Properties: props,
		})
	}
	for _, edgeID := range sortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		props, err := encodePropertyMap(edge.Properties)
		if err != nil {
			return fmt.Errorf("encode edge %d properties: %w", edgeID, err)
		}
		snapshot.Edges = append(snapshot.Edges, persistedEdge{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: props,
		})
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}

	tempPath := filepath.Join(dbPath, ".state.tmp")
	if err := os.WriteFile(tempPath, data, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tempPath, stateFilePath(dbPath)); err != nil {
		return fmt.Errorf("rename temp state: %w", err)
	}
	return nil
}

func cloneGraphState(graph *graphState) *graphState {
	cloned := newGraphState()
	for id, node := range graph.Nodes {
		cloned.Nodes[id] = &nodeRecord{
			ID:         node.ID,
			Labels:     slices.Clone(node.Labels),
			Properties: clonePropertyMap(node.Properties),
		}
	}
	for id, edge := range graph.Edges {
		cloned.Edges[id] = &edgeRecord{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: clonePropertyMap(edge.Properties),
		}
	}
	return cloned
}

func clonePropertyMap(in map[string]Value) map[string]Value {
	if len(in) == 0 {
		return map[string]Value{}
	}
	out := make(map[string]Value, len(in))
	for key, value := range in {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value Value) Value {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []Value:
		cloned := make([]Value, len(v))
		for i, item := range v {
			cloned[i] = cloneValue(item)
		}
		return cloned
	case map[string]Value:
		return clonePropertyMap(v)
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			list := make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				list[i] = cloneValue(rv.Index(i).Interface())
			}
			return list
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return value
			}
			out := make(map[string]Value, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				out[iter.Key().String()] = cloneValue(iter.Value().Interface())
			}
			return out
		default:
			return value
		}
	}
}

func normalizeValue(value Value) (Value, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return nil, fmt.Errorf("uint64 value %d exceeds int64 range", v)
		}
		return int64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return v, nil
	case []byte:
		return append([]byte(nil), v...), nil
	case []float32:
		return append([]float32(nil), v...), nil
	case []Value:
		list := make([]Value, len(v))
		for i, item := range v {
			normalized, err := normalizeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = normalized
		}
		return list, nil
	case map[string]Value:
		out := make(map[string]Value, len(v))
		for key, item := range v {
			normalized, err := normalizeValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Invalid:
		return nil, nil
	case reflect.Slice, reflect.Array:
		list := make([]Value, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			normalized, err := normalizeValue(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			list[i] = normalized
		}
		return list, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key type %s is not supported", rv.Type().Key())
		}
		out := make(map[string]Value, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			normalized, err := normalizeValue(iter.Value().Interface())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = normalized
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}

func encodePropertyMap(in map[string]Value) (map[string]persistedValue, error) {
	if len(in) == 0 {
		return map[string]persistedValue{}, nil
	}
	out := make(map[string]persistedValue, len(in))
	for key, value := range in {
		encoded, err := encodeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = encoded
	}
	return out, nil
}

func decodePropertyMap(in map[string]persistedValue) (map[string]Value, error) {
	if len(in) == 0 {
		return map[string]Value{}, nil
	}
	out := make(map[string]Value, len(in))
	for key, value := range in {
		decoded, err := decodeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = decoded
	}
	return out, nil
}

func encodeValue(value Value) (persistedValue, error) {
	switch v := value.(type) {
	case nil:
		return persistedValue{Kind: "null"}, nil
	case bool:
		return persistedValue{Kind: "bool", Bool: v}, nil
	case int64:
		return persistedValue{Kind: "int", Int: v}, nil
	case float64:
		return persistedValue{Kind: "float", Float: v}, nil
	case string:
		return persistedValue{Kind: "string", String: v}, nil
	case []byte:
		return persistedValue{Kind: "bytes", Bytes: append([]byte(nil), v...)}, nil
	case []float32:
		return persistedValue{Kind: "vector", Vector: append([]float32(nil), v...)}, nil
	case []Value:
		list := make([]persistedValue, len(v))
		for i, item := range v {
			encoded, err := encodeValue(item)
			if err != nil {
				return persistedValue{}, err
			}
			list[i] = encoded
		}
		return persistedValue{Kind: "list", List: list}, nil
	case map[string]Value:
		mapped, err := encodePropertyMap(v)
		if err != nil {
			return persistedValue{}, err
		}
		return persistedValue{Kind: "map", Map: mapped}, nil
	default:
		normalized, err := normalizeValue(value)
		if err != nil {
			return persistedValue{}, err
		}
		if reflect.TypeOf(normalized) == reflect.TypeOf(value) {
			return persistedValue{}, fmt.Errorf("unsupported normalized value type %T", value)
		}
		return encodeValue(normalized)
	}
}

func decodeValue(value persistedValue) (Value, error) {
	switch value.Kind {
	case "null":
		return nil, nil
	case "bool":
		return value.Bool, nil
	case "int":
		return value.Int, nil
	case "float":
		return value.Float, nil
	case "string":
		return value.String, nil
	case "bytes":
		return append([]byte(nil), value.Bytes...), nil
	case "vector":
		return append([]float32(nil), value.Vector...), nil
	case "list":
		list := make([]Value, len(value.List))
		for i, item := range value.List {
			decoded, err := decodeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = decoded
		}
		return list, nil
	case "map":
		return decodePropertyMap(value.Map)
	default:
		return nil, fmt.Errorf("unknown stored value kind %q", value.Kind)
	}
}

func sortedNodeIDs(graph *graphState) []uint64 {
	ids := make([]uint64, 0, len(graph.Nodes))
	for id := range graph.Nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func sortedEdgeIDs(graph *graphState) []uint64 {
	ids := make([]uint64, 0, len(graph.Edges))
	for id := range graph.Edges {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func labelsMatch(node *nodeRecord, required []string) bool {
	for _, label := range required {
		if !slices.Contains(node.Labels, label) {
			return false
		}
	}
	return true
}

func propertiesMatch(actual map[string]Value, required map[string]Value) bool {
	for key, want := range required {
		got, ok := actual[key]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(got, want) {
			return false
		}
	}
	return true
}

func validateCreateLabels(labels []string) error {
	for _, label := range labels {
		if label == "" {
			return errors.New("labels must be non-empty")
		}
	}
	return nil
}
