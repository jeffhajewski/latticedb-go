package store

import (
	"path/filepath"
	"testing"
)

func TestLoadGraphStateRecoversLatestCommitFromWAL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recover.ltdb")

	empty := NewGraphState()
	if err := CheckpointGraphState(dbPath, empty, 1, 1, 0); err != nil {
		t.Fatalf("checkpoint initial state: %v", err)
	}

	committed := NewGraphState()
	committed.Nodes[1] = &NodeRecord{
		ID:         1,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	}
	if err := AppendWALCommit(dbPath, committed, 2, 1, 1); err != nil {
		t.Fatalf("append wal commit: %v", err)
	}

	if err := SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	graph, nextNodeID, nextEdgeID, commitID, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	if commitID != 1 {
		t.Fatalf("unexpected recovered commit id %d", commitID)
	}
	if nextNodeID != 2 || nextEdgeID != 1 {
		t.Fatalf("unexpected recovered id counters node=%d edge=%d", nextNodeID, nextEdgeID)
	}
	node := graph.Nodes[1]
	if node == nil {
		t.Fatalf("expected recovered node 1")
	}
	if got := node.Properties["name"]; got != "Alice" {
		t.Fatalf("unexpected recovered property %#v", got)
	}
}
