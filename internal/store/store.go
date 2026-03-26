package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
)

const (
	stateFileName = "state.json"
	walFileName   = "wal.log"
	FTSTextKey    = "text"
)

type GraphState struct {
	Nodes map[uint64]*NodeRecord
	Edges map[uint64]*EdgeRecord
}

type NodeRecord struct {
	ID         uint64
	Labels     []string
	Properties map[string]any
}

type EdgeRecord struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]any
}

type persistedState struct {
	CommitID   uint64          `json:"commit_id"`
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

func NewGraphState() *GraphState {
	return &GraphState{
		Nodes: map[uint64]*NodeRecord{},
		Edges: map[uint64]*EdgeRecord{},
	}
}

func CloneGraphState(graph *GraphState) *GraphState {
	cloned := NewGraphState()
	for id, node := range graph.Nodes {
		cloned.Nodes[id] = &NodeRecord{
			ID:         node.ID,
			Labels:     slices.Clone(node.Labels),
			Properties: ClonePropertyMap(node.Properties),
		}
	}
	for id, edge := range graph.Edges {
		cloned.Edges[id] = &EdgeRecord{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: ClonePropertyMap(edge.Properties),
		}
	}
	return cloned
}

func ClonePropertyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = CloneValue(value)
	}
	return out
}

func CloneValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []any:
		cloned := make([]any, len(v))
		for i, item := range v {
			cloned[i] = CloneValue(item)
		}
		return cloned
	case map[string]any:
		return ClonePropertyMap(v)
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			list := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				list[i] = CloneValue(rv.Index(i).Interface())
			}
			return list
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return value
			}
			out := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				out[iter.Key().String()] = CloneValue(iter.Value().Interface())
			}
			return out
		default:
			return value
		}
	}
}

func NormalizeValue(value any) (any, error) {
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
	case []any:
		list := make([]any, len(v))
		for i, item := range v {
			normalized, err := NormalizeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = normalized
		}
		return list, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			normalized, err := NormalizeValue(item)
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
		list := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			normalized, err := NormalizeValue(rv.Index(i).Interface())
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
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			normalized, err := NormalizeValue(iter.Value().Interface())
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

func NormalizeProperties(in map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		normalized, err := NormalizeValue(value)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		out[key] = normalized
	}
	return out, nil
}

func SortedNodeIDs(graph *GraphState) []uint64 {
	ids := make([]uint64, 0, len(graph.Nodes))
	for id := range graph.Nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func SortedEdgeIDs(graph *GraphState) []uint64 {
	ids := make([]uint64, 0, len(graph.Edges))
	for id := range graph.Edges {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func LabelsMatch(node *NodeRecord, required []string) bool {
	for _, label := range required {
		if !slices.Contains(node.Labels, label) {
			return false
		}
	}
	return true
}

func PropertiesMatch(actual map[string]any, required map[string]any) bool {
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

func ValidateCreateLabels(labels []string) error {
	for _, label := range labels {
		if label == "" {
			return errors.New("labels must be non-empty")
		}
	}
	return nil
}

func stateFilePath(dbPath string) string {
	return filepath.Join(dbPath, stateFileName)
}

func walFilePath(dbPath string) string {
	return filepath.Join(dbPath, walFileName)
}

func encodePropertyMap(in map[string]any) (map[string]persistedValue, error) {
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

func decodePropertyMap(in map[string]persistedValue) (map[string]any, error) {
	if len(in) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		decoded, err := decodeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = decoded
	}
	return out, nil
}

func encodeValue(value any) (persistedValue, error) {
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
	case []any:
		list := make([]persistedValue, len(v))
		for i, item := range v {
			encoded, err := encodeValue(item)
			if err != nil {
				return persistedValue{}, err
			}
			list[i] = encoded
		}
		return persistedValue{Kind: "list", List: list}, nil
	case map[string]any:
		mapped, err := encodePropertyMap(v)
		if err != nil {
			return persistedValue{}, err
		}
		return persistedValue{Kind: "map", Map: mapped}, nil
	default:
		normalized, err := NormalizeValue(value)
		if err != nil {
			return persistedValue{}, err
		}
		if reflect.TypeOf(normalized) == reflect.TypeOf(value) {
			return persistedValue{}, fmt.Errorf("unsupported normalized value type %T", value)
		}
		return encodeValue(normalized)
	}
}

func decodeValue(value persistedValue) (any, error) {
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
		list := make([]any, len(value.List))
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
