package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func LoadGraphState(dbPath string) (*GraphState, uint64, uint64, uint64, error) {
	snapshot, snapshotErr := loadCheckpointSnapshot(dbPath)
	if snapshotErr != nil && !errors.Is(snapshotErr, os.ErrNotExist) {
		return nil, 0, 0, 0, snapshotErr
	}

	walSnapshot, walErr := loadLatestWALSnapshot(dbPath)
	if walErr != nil && !errors.Is(walErr, os.ErrNotExist) {
		return nil, 0, 0, 0, walErr
	}

	var chosen *persistedState
	switch {
	case walSnapshot != nil && (snapshot == nil || walSnapshot.CommitID > snapshot.CommitID):
		chosen = walSnapshot
	case snapshot != nil:
		chosen = snapshot
	case walSnapshot != nil:
		chosen = walSnapshot
	default:
		return nil, 0, 0, 0, os.ErrNotExist
	}

	return decodePersistedState(*chosen)
}

func CheckpointGraphState(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	snapshot, err := buildPersistedState(graph, nextNodeID, nextEdgeID, commitID)
	if err != nil {
		return err
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

func AppendWALCommit(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	snapshot, err := buildPersistedState(graph, nextNodeID, nextEdgeID, commitID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode wal entry: %w", err)
	}
	data = append(data, '\n')

	file, err := os.OpenFile(walFilePath(dbPath), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

func SimulateCrash(dbPath string) error {
	if err := os.Remove(stateFilePath(dbPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove state checkpoint: %w", err)
	}
	if err := os.Remove(filepath.Join(dbPath, ".state.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove temp checkpoint: %w", err)
	}
	return nil
}

func loadCheckpointSnapshot(dbPath string) (*persistedState, error) {
	data, err := os.ReadFile(stateFilePath(dbPath))
	if err != nil {
		return nil, err
	}

	var snapshot persistedState
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	return &snapshot, nil
}

func loadLatestWALSnapshot(dbPath string) (*persistedState, error) {
	file, err := os.Open(walFilePath(dbPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var latest *persistedState
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var entry persistedState
			if decodeErr := json.Unmarshal(trimTrailingNewline(line), &entry); decodeErr == nil {
				entryCopy := entry
				latest = &entryCopy
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read wal: %w", err)
		}
	}
	if latest == nil {
		return nil, os.ErrNotExist
	}
	return latest, nil
}

func buildPersistedState(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) (persistedState, error) {
	snapshot := persistedState{
		CommitID:   commitID,
		NextNodeID: nextNodeID,
		NextEdgeID: nextEdgeID,
		Nodes:      make([]persistedNode, 0, len(graph.Nodes)),
		Edges:      make([]persistedEdge, 0, len(graph.Edges)),
	}

	for _, nodeID := range SortedNodeIDs(graph) {
		node := graph.Nodes[nodeID]
		props, err := encodePropertyMap(node.Properties)
		if err != nil {
			return persistedState{}, fmt.Errorf("encode node %d properties: %w", nodeID, err)
		}
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{
			ID:         node.ID,
			Labels:     CloneStrings(node.Labels),
			Properties: props,
		})
	}
	for _, edgeID := range SortedEdgeIDs(graph) {
		edge := graph.Edges[edgeID]
		props, err := encodePropertyMap(edge.Properties)
		if err != nil {
			return persistedState{}, fmt.Errorf("encode edge %d properties: %w", edgeID, err)
		}
		snapshot.Edges = append(snapshot.Edges, persistedEdge{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: props,
		})
	}
	return snapshot, nil
}

func decodePersistedState(snapshot persistedState) (*GraphState, uint64, uint64, uint64, error) {
	graph := NewGraphState()
	for _, storedNode := range snapshot.Nodes {
		props, err := decodePropertyMap(storedNode.Properties)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("decode node %d properties: %w", storedNode.ID, err)
		}
		graph.Nodes[storedNode.ID] = &NodeRecord{
			ID:         storedNode.ID,
			Labels:     CloneStrings(storedNode.Labels),
			Properties: props,
		}
	}
	for _, storedEdge := range snapshot.Edges {
		props, err := decodePropertyMap(storedEdge.Properties)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("decode edge %d properties: %w", storedEdge.ID, err)
		}
		graph.Edges[storedEdge.ID] = &EdgeRecord{
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
	return graph, nextNodeID, nextEdgeID, snapshot.CommitID, nil
}

func trimTrailingNewline(line []byte) []byte {
	for len(line) > 0 {
		last := line[len(line)-1]
		if last != '\n' && last != '\r' {
			break
		}
		line = line[:len(line)-1]
	}
	return line
}

func CloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
