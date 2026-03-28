package exporter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/jeffhajewski/latticedb-go/internal/store"
)

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

type exportedGraph struct {
	Nodes []exportedNode `json:"nodes"`
	Edges []exportedEdge `json:"edges"`
}

type exportedNode struct {
	ID         string         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
}

type exportedEdge struct {
	ID         string         `json:"id"`
	Source     string         `json:"source"`
	Target     string         `json:"target"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

func Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	graph, _, _, _, err := store.LoadGraphState(dbPath)
	if err != nil {
		return nil, err
	}

	switch format {
	case ExportFormatJSON:
		return exportJSON(graph, outputPath)
	case ExportFormatJSONL:
		return exportJSONL(graph, outputPath)
	case ExportFormatCSV:
		return exportCSV(graph, outputPath)
	case ExportFormatDOT:
		return exportDOT(graph, outputPath)
	default:
		return nil, fmt.Errorf("unsupported export format %q", format)
	}
}

func Dump(dbPath string) ([]byte, error) {
	graph, _, _, _, err := store.LoadGraphState(dbPath)
	if err != nil {
		return nil, err
	}
	return marshalExportGraph(graph)
}

func SimulateCrash(dbPath string) error {
	return store.SimulateCrash(dbPath)
}

func exportJSON(graph *store.GraphState, outputPath string) ([]byte, error) {
	data, err := marshalExportGraph(graph)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(outputPath, data, 0o644); err != nil {
		return nil, err
	}
	return data, nil
}

func exportJSONL(graph *store.GraphState, outputPath string) ([]byte, error) {
	lines := make([][]byte, 0, len(graph.Nodes)+len(graph.Edges))
	for _, nodeID := range store.SortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		line, err := json.Marshal(map[string]any{
			"kind":       "node",
			"id":         strconv.FormatUint(node.ID, 10),
			"labels":     sortedLabels(node.Labels),
			"properties": exportPropertyMap(node.Properties),
		})
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	for _, edgeID := range store.SortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		line, err := json.Marshal(map[string]any{
			"kind":       "edge",
			"id":         strconv.FormatUint(edge.ID, 10),
			"source":     strconv.FormatUint(edge.SourceID, 10),
			"target":     strconv.FormatUint(edge.TargetID, 10),
			"type":       edge.Type,
			"properties": exportPropertyMap(edge.Properties),
		})
		if err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}

	output := make([]byte, 0)
	for _, line := range lines {
		output = append(output, line...)
		output = append(output, '\n')
	}
	if err := os.WriteFile(outputPath, output, 0o644); err != nil {
		return nil, err
	}
	return output, nil
}

func exportCSV(graph *store.GraphState, outputPath string) ([]byte, error) {
	base := strings.TrimSuffix(outputPath, filepath.Ext(outputPath))
	nodesPath := base + "_nodes.csv"
	edgesPath := base + "_edges.csv"

	if err := writeNodesCSV(graph, nodesPath); err != nil {
		return nil, err
	}
	if err := writeEdgesCSV(graph, edgesPath); err != nil {
		return nil, err
	}
	return nil, nil
}

func exportDOT(graph *store.GraphState, outputPath string) ([]byte, error) {
	var builder strings.Builder
	builder.WriteString("digraph G {\n")
	for _, nodeID := range store.SortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		label := strconv.FormatUint(node.ID, 10)
		if len(node.Labels) > 0 {
			label += " " + strings.Join(node.Labels, ",")
		}
		builder.WriteString(fmt.Sprintf("  n%d [label=%q];\n", node.ID, label))
	}
	for _, edgeID := range store.SortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		builder.WriteString(fmt.Sprintf("  n%d -> n%d [label=%q];\n", edge.SourceID, edge.TargetID, edge.Type))
	}
	builder.WriteString("}\n")

	data := []byte(builder.String())
	if err := os.WriteFile(outputPath, data, 0o644); err != nil {
		return nil, err
	}
	return data, nil
}

func marshalExportGraph(graph *store.GraphState) ([]byte, error) {
	exported := exportedGraph{
		Nodes: make([]exportedNode, 0, len(graph.Nodes)),
		Edges: make([]exportedEdge, 0, len(graph.Edges)),
	}

	for _, nodeID := range store.SortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		exported.Nodes = append(exported.Nodes, exportedNode{
			ID:         strconv.FormatUint(node.ID, 10),
			Labels:     sortedLabels(node.Labels),
			Properties: exportPropertyMap(node.Properties),
		})
	}
	for _, edgeID := range store.SortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		exported.Edges = append(exported.Edges, exportedEdge{
			ID:         strconv.FormatUint(edge.ID, 10),
			Source:     strconv.FormatUint(edge.SourceID, 10),
			Target:     strconv.FormatUint(edge.TargetID, 10),
			Type:       edge.Type,
			Properties: exportPropertyMap(edge.Properties),
		})
	}
	return json.Marshal(exported)
}

func exportPropertyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = exportValue(value)
	}
	return out
}

func sortedLabels(labels []string) []string {
	if len(labels) == 0 {
		return nil
	}
	out := slices.Clone(labels)
	slices.Sort(out)
	return out
}

func exportValue(value any) any {
	switch v := value.(type) {
	case nil, bool, int64, float64, string:
		return v
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = exportValue(item)
		}
		return out
	case map[string]any:
		return exportPropertyMap(v)
	default:
		return v
	}
}

func writeNodesCSV(graph *store.GraphState, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"id", "labels", "properties"}); err != nil {
		return err
	}
	for _, nodeID := range store.SortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		props, err := json.Marshal(exportPropertyMap(node.Properties))
		if err != nil {
			return err
		}
		if err := writer.Write([]string{
			strconv.FormatUint(node.ID, 10),
			strings.Join(node.Labels, "|"),
			string(props),
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}

func writeEdgesCSV(graph *store.GraphState, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"id", "source", "target", "type", "properties"}); err != nil {
		return err
	}
	for _, edgeID := range store.SortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		props, err := json.Marshal(exportPropertyMap(edge.Properties))
		if err != nil {
			return err
		}
		if err := writer.Write([]string{
			strconv.FormatUint(edge.ID, 10),
			strconv.FormatUint(edge.SourceID, 10),
			strconv.FormatUint(edge.TargetID, 10),
			edge.Type,
			string(props),
		}); err != nil {
			return err
		}
	}
	return writer.Error()
}
