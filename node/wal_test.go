package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Iamyashsiwach/raftkv/raft"
)

func TestWALReplaysOverwritesAndSurvivesTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.wal")
	w, _, _, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	must(t, w.Save(&raft.HardState{Term: 1, Vote: 1}, []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}))
	// A new leader at term 2 overwrites index 2 onward.
	must(t, w.Save(&raft.HardState{Term: 2, Vote: 3, Commit: 2}, []raft.Entry{{Term: 2, Index: 2, Data: []byte("x")}}))
	must(t, w.Close())

	// Simulate a crash halfway through writing the next record.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"e":{"t":2,"i":3,"d":"ab`)
	f.Close()

	w, hs, ents, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("torn tail should be tolerated: %v", err)
	}
	if hs != (raft.HardState{Term: 2, Vote: 3, Commit: 2}) {
		t.Fatalf("hard state = %+v", hs)
	}
	if len(ents) != 2 || ents[1].Term != 2 || string(ents[1].Data) != "x" {
		t.Fatalf("entries = %+v, want index 2 overwritten at term 2 and index 3 gone", ents)
	}

	// Appending after recovery must not leave the torn bytes in the middle of the file.
	must(t, w.Save(nil, []raft.Entry{{Term: 2, Index: 3}}))
	w.Close()
	if _, _, ents, err = OpenWAL(path); err != nil || len(ents) != 3 {
		t.Fatalf("after re-append: %d entries, err %v", len(ents), err)
	}
}

func TestWALRejectsGaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.wal")
	os.WriteFile(path, []byte(`{"e":{"t":1,"i":1}}`+"\n"+`{"e":{"t":1,"i":5}}`+"\n"), 0o600)
	if _, _, _, err := OpenWAL(path); err == nil {
		t.Fatal("expected an error for a log with a gap")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
