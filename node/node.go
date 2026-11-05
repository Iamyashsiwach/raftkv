// Package node runs a Raft state machine for real: a WAL on disk, peers over
// HTTP, a wall clock, and a replicated key-value store on top.
package node

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Iamyashsiwach/raftkv/raft"
)

const (
	maxKeyBytes   = 256
	maxValueBytes = 1 << 20
)

var (
	ErrInvalid = errors.New("invalid request")
	// ErrDropped means a new leader overwrote the write before it committed.
	// It definitely did not happen, so retrying is safe.
	ErrDropped = errors.New("write lost to a leader change; safe to retry")
	ErrStopped = errors.New("node stopped")
)

// NotLeaderError tells the client where to go instead. Addr is empty while
// the cluster has no leader (mid-election).
type NotLeaderError struct {
	Leader uint64
	Addr   string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == raft.None {
		return "no leader elected yet; retry shortly"
	}
	return fmt.Sprintf("not the leader; node %d at %s is", e.Leader, e.Addr)
}

type Config struct {
	ID      uint64
	Peers   map[uint64]string // node ID -> host:port, for every member including this one
	DataDir string
	Token   string        // shared secret peers must present on /raft; empty disables auth
	Tick    time.Duration // one Raft clock tick; defaults to 50ms (elections after 0.5-1s of silence)
	Logger  *slog.Logger
}

// Node is one cluster member. The Raft state machine and the waiter table are
// owned exclusively by the Run goroutine; everything else talks to it through
// channels. Only the KV map is shared, so that stale reads skip the loop.
type Node struct {
	cfg  Config
	log  *slog.Logger
	raft *raft.Raft
	wal  *WAL
	tr   *transport

	recvc chan raft.Message
	propc chan proposal
	done  chan struct{}

	waiters map[uint64]waiter // log index -> client waiting for it to apply

	mu sync.RWMutex
	kv map[string]string

	status atomic.Pointer[raft.Status]
}

type command struct {
	Op    string `json:"op"` // put | delete | get
	Key   string `json:"k"`
	Value string `json:"v,omitempty"`
}

type result struct {
	value string
	found bool
	err   error
}

type proposal struct {
	data []byte
	done chan result
}

type waiter struct {
	term uint64
	done chan result
}

func Open(cfg Config) (*Node, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("node %d is not in the cluster %v", cfg.ID, cfg.Peers)
	}
	if cfg.DataDir == "" {
		return nil, errors.New("data dir is required")
	}
	cfg.Tick = cmp.Or(cfg.Tick, 50*time.Millisecond)
	cfg.Logger = cmp.Or(cfg.Logger, slog.Default())

	wal, hs, ents, err := OpenWAL(filepath.Join(cfg.DataDir, fmt.Sprintf("node-%d.wal", cfg.ID)))
	if err != nil {
		return nil, err
	}
	r, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: slices.Sorted(maps.Keys(cfg.Peers)),
		ElectionTick: 10, HeartbeatTick: 1,
		HardState: hs, Entries: ents,
	})
	if err != nil {
		wal.Close()
		return nil, err
	}
	log := cfg.Logger.With("node", cfg.ID)
	n := &Node{
		cfg: cfg, log: log, raft: r, wal: wal,
		tr:      newTransport(cfg.ID, cfg.Peers, cfg.Token, log),
		recvc:   make(chan raft.Message, 4096),
		propc:   make(chan proposal, 256),
		done:    make(chan struct{}),
		waiters: map[uint64]waiter{},
		kv:      map[string]string{},
	}
	n.publishStatus()
	log.Info("restored from disk", "term", hs.Term, "entries", len(ents), "commit", hs.Commit)
	return n, nil
}

// Run drives the node until ctx is cancelled or the disk fails. It must be
// called exactly once.
func (n *Node) Run(ctx context.Context) error {
	defer close(n.done)
	defer n.wal.Close()
	n.tr.start(ctx)

	ticker := time.NewTicker(n.cfg.Tick)
	defer ticker.Stop()
	prev := n.raft.Status()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n.raft.Tick()
		case m := <-n.recvc:
			n.raft.Step(m)
		case p := <-n.propc:
			n.startProposal(p)
		}
		if err := n.handleReady(); err != nil {
			// We can't promise durability any more, and Raft's guarantees
			// rest on it. Stop rather than keep voting on state we may lose.
			return fmt.Errorf("node %d: %w", n.cfg.ID, err)
		}
		if st := n.raft.Status(); st.State != prev.State || st.Term != prev.Term {
			n.log.Info("state change", "state", st.State, "term", st.Term, "leader", st.Lead)
			prev = st
		}
	}
}

func (n *Node) handleReady() error {
	rd := n.raft.Ready()
	if err := n.wal.Save(rd.HardState, rd.Entries); err != nil {
		return fmt.Errorf("persist: %w", err)
	}
	n.tr.send(rd.Messages) // only after the WAL fsync: never promise what isn't durable
	for _, e := range rd.Committed {
		n.apply(e)
	}
	n.publishStatus()
	return nil
}

func (n *Node) startProposal(p proposal) {
	idx, term, err := n.raft.Propose(p.data)
	if err != nil {
		p.done <- result{err: n.notLeader()}
		return
	}
	if old, ok := n.waiters[idx]; ok {
		old.done <- result{err: ErrDropped} // its entry was truncated and the slot reused
	}
	n.waiters[idx] = waiter{term: term, done: p.done}
}

func (n *Node) apply(e raft.Entry) {
	var res result
	if len(e.Data) > 0 { // empty = a new leader's no-op
		var c command
		if err := json.Unmarshal(e.Data, &c); err != nil {
			// Deterministic: every replica rejects the same bytes identically.
			res.err = fmt.Errorf("%w: undecodable command at index %d", ErrInvalid, e.Index)
		} else {
			res = n.applyCommand(c)
		}
	}
	w, ok := n.waiters[e.Index]
	if !ok {
		return
	}
	delete(n.waiters, e.Index)
	if w.term != e.Term {
		res = result{err: ErrDropped} // a different leader's entry landed in our slot
	}
	w.done <- res
}

func (n *Node) applyCommand(c command) result {
	switch c.Op {
	case "put":
		n.mu.Lock()
		n.kv[c.Key] = c.Value
		n.mu.Unlock()
		return result{}
	case "delete":
		n.mu.Lock()
		_, found := n.kv[c.Key]
		delete(n.kv, c.Key)
		n.mu.Unlock()
		return result{found: found}
	case "get":
		n.mu.RLock()
		v, found := n.kv[c.Key]
		n.mu.RUnlock()
		return result{value: v, found: found}
	}
	return result{err: fmt.Errorf("%w: unknown op %q", ErrInvalid, c.Op)}
}

func (n *Node) notLeader() error {
	lead := n.raft.Status().Lead
	return &NotLeaderError{Leader: lead, Addr: n.cfg.Peers[lead]}
}

func (n *Node) publishStatus() {
	st := n.raft.Status()
	n.status.Store(&st)
}

// ---- client API ----

func (n *Node) Put(ctx context.Context, key, value string) error {
	if err := validate(key); err != nil {
		return err
	}
	if len(value) > maxValueBytes {
		return fmt.Errorf("%w: value over %d bytes", ErrInvalid, maxValueBytes)
	}
	_, err := n.propose(ctx, command{Op: "put", Key: key, Value: value})
	return err
}

// Delete reports whether the key existed.
func (n *Node) Delete(ctx context.Context, key string) (bool, error) {
	if err := validate(key); err != nil {
		return false, err
	}
	res, err := n.propose(ctx, command{Op: "delete", Key: key})
	return res.found, err
}

// Get is linearizable: the read goes through the log like a write, so it can
// never return data older than a write that already finished.
//
// ponytail: costs a full replication round trip per read; switch to Raft's
// ReadIndex protocol if read latency becomes the bottleneck.
func (n *Node) Get(ctx context.Context, key string) (string, bool, error) {
	if err := validate(key); err != nil {
		return "", false, err
	}
	res, err := n.propose(ctx, command{Op: "get", Key: key})
	return res.value, res.found, err
}

// StaleGet reads this node's local copy. Fast and works on any node, but may
// lag the leader, or be arbitrarily old if this node is partitioned.
func (n *Node) StaleGet(key string) (string, bool, error) {
	if err := validate(key); err != nil {
		return "", false, err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	v, ok := n.kv[key]
	return v, ok, nil
}

func (n *Node) Status() raft.Status { return *n.status.Load() }

func (n *Node) propose(ctx context.Context, c command) (result, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return result{}, err
	}
	p := proposal{data: data, done: make(chan result, 1)}
	select {
	case n.propc <- p:
	case <-ctx.Done():
		return result{}, ctx.Err()
	case <-n.done:
		return result{}, ErrStopped
	}
	select {
	case res := <-p.done:
		return res, res.err
	case <-ctx.Done():
		// The write may still commit later: the outcome is unknown, not failed.
		return result{}, ctx.Err()
	case <-n.done:
		return result{}, ErrStopped
	}
}

func validate(key string) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: empty key", ErrInvalid)
	case len(key) > maxKeyBytes:
		return fmt.Errorf("%w: key over %d bytes", ErrInvalid, maxKeyBytes)
	case !utf8.ValidString(key):
		return fmt.Errorf("%w: key is not valid UTF-8", ErrInvalid)
	}
	return nil
}
