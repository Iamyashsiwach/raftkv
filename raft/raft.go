// Package raft implements the Raft consensus algorithm as a pure, deterministic
// state machine. It does no I/O and owns no goroutines or clocks: the caller
// feeds it ticks and messages, then drains a Ready describing what to persist,
// what to send and what to apply.
//
// That split (the same one etcd/raft uses) is what makes the chaos tests
// trustworthy: a seeded simulation can drive thousands of crash and partition
// scenarios with no sleeps and no flakiness, and replay any failure exactly.
//
// Beyond the core paper it implements PreVote and CheckQuorum, so a flaky or
// partitioned node cannot inflate terms and depose a healthy leader, and a
// leader cut off from the majority steps down instead of serving stale writes.
package raft

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// None is the zero node ID: no vote cast, no known leader.
const None uint64 = 0

type State uint8

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[s]
}

func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

type MsgType uint8

const (
	MsgPreVote MsgType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
)

// Entry is one slot in the replicated log. An entry with empty Data is a
// leader's no-op, appended on election so earlier-term entries can commit.
type Entry struct {
	Term  uint64 `json:"t"`
	Index uint64 `json:"i"`
	Data  []byte `json:"d,omitempty"`
}

// Message is every RPC in the protocol. Index and LogTerm are overloaded the
// way the paper's RPCs are:
//
//	(Pre)Vote:  Index/LogTerm = candidate's last log index/term
//	App:        Index/LogTerm = index/term of the entry preceding Entries
//	AppResp:    Index = highest index now matching the leader, or a back-off hint on Reject
type Message struct {
	Type    MsgType `json:"y"`
	From    uint64  `json:"f"`
	To      uint64  `json:"to"`
	Term    uint64  `json:"t"`
	LogTerm uint64  `json:"lt,omitempty"`
	Index   uint64  `json:"i,omitempty"`
	Entries []Entry `json:"e,omitempty"`
	Commit  uint64  `json:"c,omitempty"`
	Reject  bool    `json:"r,omitempty"`
}

// HardState must be durable before any message produced alongside it is sent.
type HardState struct {
	Term   uint64 `json:"term"`
	Vote   uint64 `json:"vote"`
	Commit uint64 `json:"commit"`
}

// Ready is everything the caller must do after a Tick, Step or Propose,
// in this order: persist HardState and Entries, send Messages, apply Committed.
type Ready struct {
	HardState *HardState // nil when unchanged
	Entries   []Entry    // new or overwritten log entries; an entry at index i truncates the log after i-1
	Committed []Entry    // safe to apply to the state machine
	Messages  []Message
}

type Status struct {
	ID        uint64 `json:"id"`
	State     State  `json:"state"`
	Term      uint64 `json:"term"`
	Lead      uint64 `json:"lead"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"lastIndex"`
}

type Config struct {
	ID    uint64
	Peers []uint64 // every cluster member, including ID

	// A follower that hears nothing for a random [ElectionTick, 2*ElectionTick)
	// ticks starts an election. The leader heartbeats every HeartbeatTick ticks.
	ElectionTick  int
	HeartbeatTick int

	MaxEntriesPerMsg int        // caps a single append; defaults to 256
	Rand             *rand.Rand // seed it for deterministic tests; defaults to time-seeded

	// Durable state to restore after a restart.
	HardState HardState
	Entries   []Entry
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrEmpty     = errors.New("raft: empty proposals are reserved for leader no-ops")
)

type Raft struct {
	id    uint64
	peers []uint64 // excluding self
	state State
	term  uint64
	vote  uint64
	lead  uint64

	log     []Entry // log[0] is a sentinel {0,0}, so log[i].Index == i
	commit  uint64
	applied uint64 // highest index handed out in Ready.Committed
	stable  uint64 // highest index handed out in Ready.Entries

	// Leader-only replication progress.
	next, match map[uint64]uint64
	active      map[uint64]bool // peers heard from this election period (CheckQuorum)

	votes map[uint64]bool // (pre-)votes received this campaign

	electionTick, heartbeatTick, randomizedElectionTick int
	electionElapsed, heartbeatElapsed                   int
	maxEntries                                          int
	rng                                                 *rand.Rand

	msgs   []Message
	prevHS HardState
}

func New(cfg Config) (*Raft, error) {
	if cfg.ID == None || !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("raft: id %d must be a non-zero member of peers %v", cfg.ID, cfg.Peers)
	}
	if cfg.HeartbeatTick <= 0 || cfg.ElectionTick <= cfg.HeartbeatTick {
		return nil, fmt.Errorf("raft: need 0 < HeartbeatTick (%d) < ElectionTick (%d)", cfg.HeartbeatTick, cfg.ElectionTick)
	}
	log := append([]Entry{{}}, cfg.Entries...)
	for i, e := range log {
		if e.Index != uint64(i) || (i > 0 && e.Term < log[i-1].Term) {
			return nil, fmt.Errorf("raft: restored log is not contiguous at position %d", i)
		}
	}
	if cfg.HardState.Commit > uint64(len(log)-1) {
		return nil, fmt.Errorf("raft: commit %d beyond restored log end %d", cfg.HardState.Commit, len(log)-1)
	}

	r := &Raft{
		id:            cfg.ID,
		log:           log,
		term:          cfg.HardState.Term,
		vote:          cfg.HardState.Vote,
		commit:        cfg.HardState.Commit,
		stable:        uint64(len(log) - 1),
		prevHS:        cfg.HardState,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		maxEntries:    cmp.Or(cfg.MaxEntriesPerMsg, 256),
		rng:           cfg.Rand,
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID && !slices.Contains(r.peers, p) {
			r.peers = append(r.peers, p)
		}
	}
	if r.rng == nil {
		r.rng = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), cfg.ID))
	}
	r.becomeFollower(r.term, None)
	return r, nil
}

// ---- public driver API ----

// Tick advances the logical clock by one unit.
func (r *Raft) Tick() {
	r.electionElapsed++
	if r.state != Leader {
		if r.electionElapsed >= r.randomizedElectionTick {
			r.campaign(true)
		}
		return
	}

	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTick {
		r.heartbeatElapsed = 0
		r.broadcastAppend()
	}
	// CheckQuorum: a leader that hasn't heard from a majority for a whole
	// election period is probably partitioned away. Step down so clients get
	// redirected to the side that can actually commit.
	if r.electionElapsed >= r.electionTick {
		r.electionElapsed = 0
		if !r.quorumActive() {
			r.becomeFollower(r.term, None)
			return
		}
		clear(r.active)
	}
}

// Propose appends data to the log if this node leads. The entry is committed
// once Ready.Committed contains an entry with the returned index AND term; the
// same index with a different term means a new leader overwrote it.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if len(data) == 0 {
		return 0, 0, ErrEmpty
	}
	index = r.appendEntry(data)
	r.broadcastAppend()
	return index, r.term, nil
}

// Step feeds one message from a peer into the state machine.
func (r *Raft) Step(m Message) {
	if m.To != r.id || !slices.Contains(r.peers, m.From) || !wellFormed(m) {
		return
	}

	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && r.inLease() {
			// We heard from a live leader recently: a vote request now comes
			// from a node with a flaky link, not a dead leader. Ignoring it is
			// what stops that node from deposing a healthy leader.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// Answering a pre-vote never changes our term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A granted pre-vote carries the term the candidate *would* use.
		case m.Type == MsgApp:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}

	case m.Term < r.term:
		// Answer stale leaders and pre-candidates with our term so they step
		// down, instead of letting them retry forever.
		switch m.Type {
		case MsgApp:
			r.send(Message{Type: MsgAppResp, To: m.From})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Reject: true})
		}
		return
	}

	if m.Type == MsgVote || m.Type == MsgPreVote {
		r.handleVote(m)
		return
	}
	switch r.state {
	case Follower:
		if m.Type == MsgApp {
			r.electionElapsed = 0
			r.lead = m.From
			r.handleAppend(m)
		}
	case PreCandidate, Candidate:
		r.stepCandidate(m)
	case Leader:
		r.stepLeader(m)
	}
}

// Ready drains pending work. Callers must persist, then send, then apply.
func (r *Raft) Ready() Ready {
	rd := Ready{Messages: r.msgs}
	r.msgs = nil
	if hs := r.hardState(); hs != r.prevHS {
		rd.HardState, r.prevHS = &hs, hs
	}
	if last := r.lastIndex(); last > r.stable {
		rd.Entries = slices.Clone(r.log[r.stable+1:])
		r.stable = last
	}
	if r.commit > r.applied {
		rd.Committed = slices.Clone(r.log[r.applied+1 : r.commit+1])
		r.applied = r.commit
	}
	return rd
}

func (r *Raft) Status() Status {
	return Status{ID: r.id, State: r.state, Term: r.term, Lead: r.lead,
		Commit: r.commit, Applied: r.applied, LastIndex: r.lastIndex()}
}

// ---- elections ----

func (r *Raft) reset(term uint64) {
	if r.term != term {
		r.term, r.vote = term, None
	}
	r.lead = None
	r.electionElapsed, r.heartbeatElapsed = 0, 0
	r.randomizedElectionTick = r.electionTick + r.rng.IntN(r.electionTick)
	r.votes = map[uint64]bool{}
}

func (r *Raft) becomeFollower(term, lead uint64) {
	r.reset(term)
	r.state, r.lead = Follower, lead
}

func (r *Raft) becomeLeader() {
	r.reset(r.term)
	r.state, r.lead = Leader, r.id
	r.next, r.match, r.active = map[uint64]uint64{}, map[uint64]uint64{}, map[uint64]bool{}
	for _, p := range r.peers {
		r.next[p] = r.lastIndex() + 1
	}
	// Raft §5.4.2: a leader may only commit entries from earlier terms
	// indirectly, by committing one from its own term. The no-op makes that
	// happen immediately instead of waiting for the next client write.
	r.appendEntry(nil)
	r.broadcastAppend()
}

// campaign starts a pre-vote (pre=true) or a real election. A pre-vote asks
// "would you vote for me?" without bumping anyone's term, so a node stuck in
// a minority partition can't come back with a huge term and force an election.
func (r *Raft) campaign(pre bool) {
	typ, term := MsgVote, r.term+1
	if pre {
		r.reset(r.term)
		r.state, typ = PreCandidate, MsgPreVote
	} else {
		r.reset(term)
		r.state, r.vote = Candidate, r.id
	}
	if r.poll(r.id, true) == voteWon { // single-node cluster
		r.onVoteWon()
		return
	}
	for _, p := range r.peers {
		r.send(Message{Type: typ, To: p, Term: term, Index: r.lastIndex(), LogTerm: r.lastTerm()})
	}
}

func (r *Raft) handleVote(m Message) {
	resp := MsgVoteResp
	if m.Type == MsgPreVote {
		resp = MsgPreVoteResp
	}
	canVote := r.vote == m.From || // a retransmitted request
		(r.vote == None && r.lead == None) || // haven't voted and know no leader this term
		(m.Type == MsgPreVote && m.Term > r.term) // pre-votes are hypothetical: grant any future term
	if !canVote || !r.logUpToDate(m.Index, m.LogTerm) {
		r.send(Message{Type: resp, To: m.From, Reject: true})
		return
	}
	r.send(Message{Type: resp, To: m.From, Term: m.Term})
	if m.Type == MsgVote {
		r.electionElapsed = 0
		r.vote = m.From
	}
}

func (r *Raft) stepCandidate(m Message) {
	switch m.Type {
	case MsgApp:
		// Someone already won this term.
		r.becomeFollower(m.Term, m.From)
		r.handleAppend(m)
	case MsgPreVoteResp:
		if r.state != PreCandidate || (!m.Reject && m.Term != r.term+1) {
			return // stale answer from an earlier round
		}
		r.onVoteResult(r.poll(m.From, !m.Reject))
	case MsgVoteResp:
		if r.state == Candidate {
			r.onVoteResult(r.poll(m.From, !m.Reject))
		}
	}
}

type voteResult uint8

const (
	votePending voteResult = iota
	voteWon
	voteLost
)

func (r *Raft) poll(from uint64, granted bool) voteResult {
	if _, seen := r.votes[from]; !seen {
		r.votes[from] = granted
	}
	yes := 0
	for _, g := range r.votes {
		if g {
			yes++
		}
	}
	switch {
	case yes >= r.quorum():
		return voteWon
	case len(r.votes)-yes >= r.quorum():
		return voteLost
	}
	return votePending
}

func (r *Raft) onVoteResult(res voteResult) {
	switch res {
	case voteWon:
		r.onVoteWon()
	case voteLost:
		r.becomeFollower(r.term, None)
	}
}

func (r *Raft) onVoteWon() {
	if r.state == PreCandidate {
		r.campaign(false)
		return
	}
	r.becomeLeader()
}

// inLease reports whether we heard from a leader within the minimum election
// timeout, in which case no healthy cluster member should be asking for votes.
func (r *Raft) inLease() bool {
	return r.lead != None && r.electionElapsed < r.electionTick
}

func (r *Raft) logUpToDate(index, term uint64) bool {
	return term > r.lastTerm() || (term == r.lastTerm() && index >= r.lastIndex())
}

// ---- replication ----

func (r *Raft) stepLeader(m Message) {
	if m.Type != MsgAppResp {
		return
	}
	r.active[m.From] = true
	if m.Reject {
		// Back up toward the follower's log end, never below what it has acked.
		r.next[m.From] = max(r.match[m.From]+1, min(r.next[m.From]-1, m.Index+1))
		r.sendAppend(m.From)
		return
	}
	if m.Index > r.match[m.From] {
		r.match[m.From] = m.Index
		r.next[m.From] = max(r.next[m.From], m.Index+1)
		r.maybeCommit()
	}
	if r.next[m.From] <= r.lastIndex() {
		r.sendAppend(m.From) // keep the pipe full when a batch was capped
	}
}

func (r *Raft) handleAppend(m Message) {
	if t, ok := r.termAt(m.Index); !ok || t != m.LogTerm {
		// We lack the entry before this batch, or it conflicts: tell the
		// leader where to back up to.
		r.send(Message{Type: MsgAppResp, To: m.From, Reject: true, Index: min(m.Index-1, r.lastIndex())})
		return
	}
	for i, e := range m.Entries {
		if t, ok := r.termAt(e.Index); ok {
			if t == e.Term {
				continue // already have it (retransmit or reordered message)
			}
			if e.Index <= r.commit {
				// Committed entries are permanent. A leader that disagrees
				// means a safety bug; corrupting the state machine is worse
				// than crashing.
				panic(fmt.Sprintf("raft: node %d asked to overwrite committed entry %d", r.id, e.Index))
			}
			r.log = r.log[:e.Index] // drop the uncommitted tail from a deposed leader
			r.stable = min(r.stable, e.Index-1)
		}
		r.log = append(r.log, m.Entries[i:]...)
		break
	}
	last := m.Index + uint64(len(m.Entries))
	r.commit = max(r.commit, min(m.Commit, last))
	r.send(Message{Type: MsgAppResp, To: m.From, Index: last})
}

func (r *Raft) sendAppend(to uint64) {
	prev := r.next[to] - 1
	prevTerm, _ := r.termAt(prev)
	end := min(r.lastIndex(), prev+uint64(r.maxEntries))
	ents := slices.Clone(r.log[prev+1 : end+1])
	r.send(Message{Type: MsgApp, To: to, Index: prev, LogTerm: prevTerm, Entries: ents, Commit: r.commit})
	if len(ents) > 0 {
		// Optimistic pipelining: assume delivery and keep streaming. A lost
		// message surfaces as a reject on the next heartbeat, which rewinds next.
		r.next[to] = end + 1
	}
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		r.sendAppend(p)
	}
}

func (r *Raft) appendEntry(data []byte) uint64 {
	e := Entry{Term: r.term, Index: r.lastIndex() + 1, Data: data}
	r.log = append(r.log, e)
	r.maybeCommit() // a single-node cluster commits on append
	return e.Index
}

// maybeCommit advances commit to the highest index stored on a majority,
// but only by counting replicas of a current-term entry (Raft §5.4.2, Figure 8).
func (r *Raft) maybeCommit() {
	matched := []uint64{r.lastIndex()}
	for _, p := range r.peers {
		matched = append(matched, r.match[p])
	}
	slices.SortFunc(matched, func(a, b uint64) int { return cmp.Compare(b, a) })
	n := matched[r.quorum()-1]
	if t, _ := r.termAt(n); n > r.commit && t == r.term {
		r.commit = n
	}
}

func (r *Raft) quorumActive() bool {
	n := 1
	for _, p := range r.peers {
		if r.active[p] {
			n++
		}
	}
	return n >= r.quorum()
}

// ---- helpers ----

// wellFormed rejects messages no correct peer would send, so a buggy or
// hostile sender can't push the log into an inconsistent shape.
func wellFormed(m Message) bool {
	for i, e := range m.Entries {
		if e.Index != m.Index+1+uint64(i) || e.Term > m.Term || (i > 0 && e.Term < m.Entries[i-1].Term) {
			return false
		}
	}
	return len(m.Entries) == 0 || m.Entries[0].Term >= m.LogTerm
}

func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) hardState() HardState { return HardState{Term: r.term, Vote: r.vote, Commit: r.commit} }
func (r *Raft) quorum() int          { return (len(r.peers)+1)/2 + 1 }
func (r *Raft) lastIndex() uint64    { return uint64(len(r.log) - 1) }
func (r *Raft) lastTerm() uint64     { return r.log[len(r.log)-1].Term }

func (r *Raft) termAt(i uint64) (uint64, bool) {
	if i > r.lastIndex() {
		return 0, false
	}
	return r.log[i].Term, true
}
