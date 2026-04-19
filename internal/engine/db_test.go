package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCommitFailureDoesNotExposeWrites(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "commit_failure.ltdb")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin write tx: %v", err)
	}

	node, err := tx.CreateNode(CreateNodeOptions{
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	backupPath := dbPath + ".backup"
	if err := os.Rename(dbPath, backupPath); err != nil {
		t.Fatalf("rename db dir: %v", err)
	}
	if err := os.WriteFile(dbPath, []byte("blocked"), 0o644); err != nil {
		t.Fatalf("block db path: %v", err)
	}

	if err := tx.Commit(); err == nil {
		t.Fatalf("expected commit to fail")
	}

	if err := db.View(func(view *Tx) error {
		exists, err := view.NodeExists(node.ID)
		if err != nil {
			return err
		}
		if exists {
			t.Fatalf("expected failed commit node %d to remain invisible", node.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after failed commit: %v", err)
	}

	if err := os.Remove(dbPath); err != nil {
		t.Fatalf("remove blocking file: %v", err)
	}
	if err := os.Rename(backupPath, dbPath); err != nil {
		t.Fatalf("restore db dir: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close original db: %v", err)
	}

	reopened, err := Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	if err := reopened.View(func(view *Tx) error {
		exists, err := view.NodeExists(node.ID)
		if err != nil {
			return err
		}
		if exists {
			t.Fatalf("expected failed commit node %d to remain absent after reopen", node.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after reopen: %v", err)
	}
}
