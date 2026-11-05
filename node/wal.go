package node

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Iamyashsiwach/raftkv/raft"
)

// WAL is an append-only, fsynced log of Raft's durable state: one JSON record
// per line, either a log entry or a hard state.
//
// Replay applies the same rule Raft does: an entry at index i replaces
// whatever was at i and everything after it. So overwriting a deposed
// leader's uncommitted tail is just another append, never an in-place edit.
//
// ponytail: grows forever (no snapshots or compaction); add log compaction
// once the replay time on restart starts to matter.
type WAL struct {
	f *os.File
	w *bufio.Writer
}

type walRecord struct {
	Entry     *raft.Entry     `json:"e,omitempty"`
	HardState *raft.HardState `json:"hs,omitempty"`
}

// OpenWAL opens or creates the log at path and returns the state to restore.
func OpenWAL(path string) (*WAL, raft.HardState, []raft.Entry, error) {
	var hs raft.HardState
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, hs, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, hs, nil, err
	}
	hs, ents, valid, err := replay(f)
	if err != nil {
		f.Close()
		return nil, hs, nil, fmt.Errorf("wal %s: %w", path, err)
	}
	// Drop a torn final record from a crash mid-write. It was never fsynced,
	// so nothing was ever acknowledged on the strength of it.
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, hs, nil, err
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, hs, nil, err
	}
	return &WAL{f: f, w: bufio.NewWriter(f)}, hs, ents, nil
}

func replay(r io.Reader) (hs raft.HardState, ents []raft.Entry, valid int64, err error) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			return hs, ents, valid, nil // any bytes without a trailing newline are a torn write
		}
		if err != nil {
			return hs, nil, 0, err
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return hs, nil, 0, fmt.Errorf("corrupt record at offset %d: %w", valid, err)
		}
		switch {
		case rec.Entry != nil:
			e := *rec.Entry
			if e.Index == 0 || e.Index > uint64(len(ents))+1 {
				return hs, nil, 0, fmt.Errorf("entry %d at offset %d leaves a gap after %d", e.Index, valid, len(ents))
			}
			ents = append(ents[:e.Index-1], e)
		case rec.HardState != nil:
			hs = *rec.HardState
		}
		valid += int64(len(line))
	}
}

// Save durably appends entries, then the hard state. Entries go first so a
// torn write can never leave a commit index pointing past the log.
func (w *WAL) Save(hs *raft.HardState, ents []raft.Entry) error {
	if hs == nil && len(ents) == 0 {
		return nil
	}
	enc := json.NewEncoder(w.w)
	for i := range ents {
		if err := enc.Encode(walRecord{Entry: &ents[i]}); err != nil {
			return err
		}
	}
	if hs != nil {
		if err := enc.Encode(walRecord{HardState: hs}); err != nil {
			return err
		}
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *WAL) Close() error { return w.f.Close() }
