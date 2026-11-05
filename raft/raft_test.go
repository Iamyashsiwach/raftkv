package raft

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
)

// sim is a deterministic network of Raft nodes. Everything (message order,
// drops, crashes, partitions, election jitter) comes from one seeded RNG, so a
// failing seed replays exactly.
//
// It checks Raft's safety properties continuously, not just at the end:
//   - Election Safety: at most one leader per term, ever.
//   - State Machine Safety: every node applies the same entry at each index.
type sim struct {
	t   *testing.T
	rng *rand.Rand
	ids []uint64

	nodes map[uint64]*Raft // nil while crashed
	disk  map[uint64]*disk // survives crashes
	group map[uint64]int   // partition group; only same-group nodes can talk
	drop  float64          // per-message loss probability
	queue []Message

	appliedIdx map[uint64]uint64 // per-node apply cursor, rebuilt from disk on restart
	canonical  []Entry           // the one true applied sequence, learned from whoever applies first
	leaders    map[uint64]uint64 // term -> leader
}

type disk struct {
	hs  HardState
	log []Entry
}

func (d *disk) save(rd Ready) {
	if rd.HardState != nil {
		d.hs = *rd.HardState
	}
	for _, e := range rd.Entries {
		d.log = append(d.log[:e.Index-1], e) // same truncate-then-append rule as the real WAL
	}
}

func newSim(t *testing.T, n int, seed uint64) *sim {
	t.Helper()
	s := &sim{
		t: t, rng: rand.New(rand.NewPCG(seed, 0xC0FFEE)),
		nodes: map[uint64]*Raft{}, disk: map[uint64]*disk{}, group: map[uint64]int{},
		appliedIdx: map[uint64]uint64{}, leaders: map[uint64]uint64{},
	}
	for id := range uint64(n) {
		s.ids = append(s.ids, id+1)
	}
	for _, id := range s.ids {
		s.disk[id] = &disk{}
		s.start(id)
	}
	return s
}

func (s *sim) start(id uint64) {
	s.t.Helper()
	d := s.disk[id]
	r, err := New(Config{
		ID: id, Peers: s.ids, ElectionTick: 10, HeartbeatTick: 1, MaxEntriesPerMsg: 8,
		Rand:      rand.New(rand.NewPCG(s.rng.Uint64(), id)),
		HardState: d.hs, Entries: slices.Clone(d.log),
	})
	if err != nil {
		s.t.Fatalf("restart node %d: %v", id, err)
	}
	s.nodes[id] = r
	s.appliedIdx[id] = 0 // in-memory state machine is gone; it replays from the log
}

func (s *sim) crash(id uint64) { s.nodes[id] = nil }

func (s *sim) up(id uint64) bool { return s.nodes[id] != nil }

func (s *sim) heal() {
	clear(s.group)
	s.drop = 0
}

func (s *sim) partition(groups ...[]uint64) {
	for g, ids := range groups {
		for _, id := range ids {
			s.group[id] = g + 1
		}
	}
}

// tick advances every live node's clock once, then lets the network settle.
func (s *sim) tick() {
	for _, id := range s.ids {
		if s.up(id) {
			s.nodes[id].Tick()
		}
	}
	s.settle()
}

func (s *sim) run(ticks int) {
	for range ticks {
		s.tick()
	}
}

func (s *sim) settle() {
	for range 64 { // bounded: a live cluster always quiesces well before this
		for _, id := range s.ids {
			if s.up(id) {
				s.process(id)
			}
		}
		if len(s.queue) == 0 {
			return
		}
		msgs := s.queue
		s.queue = nil
		s.rng.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
		for _, m := range msgs {
			if s.up(m.To) && s.group[m.From] == s.group[m.To] && s.rng.Float64() >= s.drop {
				s.nodes[m.To].Step(m)
			}
		}
	}
}

func (s *sim) process(id uint64) {
	r := s.nodes[id]
	rd := r.Ready()

	if st := r.Status(); st.State == Leader {
		if prev, ok := s.leaders[st.Term]; ok && prev != id {
			s.t.Fatalf("election safety violated: term %d has leaders %d and %d", st.Term, prev, id)
		}
		s.leaders[st.Term] = id
	}

	s.disk[id].save(rd) // persist before send
	s.queue = append(s.queue, rd.Messages...)
	for _, e := range rd.Committed {
		s.apply(id, e)
	}
}

func (s *sim) apply(id uint64, e Entry) {
	if e.Index != s.appliedIdx[id]+1 {
		s.t.Fatalf("node %d applied index %d after %d: gap or replay", id, e.Index, s.appliedIdx[id])
	}
	s.appliedIdx[id] = e.Index
	if int(e.Index) > len(s.canonical) {
		s.canonical = append(s.canonical, e)
		return
	}
	if want := s.canonical[e.Index-1]; want.Term != e.Term || !bytes.Equal(want.Data, e.Data) {
		s.t.Fatalf("state machine safety violated at index %d: node %d applied %+v, others applied %+v", e.Index, id, e, want)
	}
}

// leader returns the leader every live node in its group agrees on, or None.
func (s *sim) leader() uint64 {
	best, bestTerm := None, uint64(0)
	for _, id := range s.ids {
		if !s.up(id) {
			continue
		}
		if st := s.nodes[id].Status(); st.State == Leader && st.Term >= bestTerm {
			best, bestTerm = id, st.Term
		}
	}
	return best
}

func (s *sim) waitLeader(maxTicks int) uint64 {
	s.t.Helper()
	for range maxTicks {
		if l := s.leader(); l != None {
			return l
		}
		s.tick()
	}
	s.t.Fatalf("no leader after %d ticks", maxTicks)
	return None
}

func (s *sim) propose(data string) (uint64, bool) {
	l := s.leader()
	if l == None {
		return 0, false
	}
	idx, _, err := s.nodes[l].Propose([]byte(data))
	return idx, err == nil
}

func (s *sim) applied(id uint64, data string) bool {
	for _, e := range s.canonical[:s.appliedIdx[id]] {
		if string(e.Data) == data {
			return true
		}
	}
	return false
}

func (s *sim) allApplied(data string) bool {
	for _, id := range s.ids {
		if s.up(id) && !s.applied(id, data) {
			return false
		}
	}
	return true
}

func (s *sim) requireAllApplied(data string) {
	s.t.Helper()
	for _, id := range s.ids {
		if s.up(id) && !s.applied(id, data) {
			s.t.Fatalf("node %d never applied %q (applied up to %d)\n%s", id, data, s.appliedIdx[id], s.dump())
		}
	}
}

func (s *sim) dump() string {
	var b bytes.Buffer
	for _, id := range s.ids {
		if !s.up(id) {
			fmt.Fprintf(&b, "  node %d: down\n", id)
			continue
		}
		r := s.nodes[id]
		fmt.Fprintf(&b, "  node %d: %+v lastTerm=%d next=%v match=%v\n", id, r.Status(), r.lastTerm(), r.next, r.match)
	}
	return b.String()
}

// ---- scenarios ----

func TestSingleNodeCommitsImmediately(t *testing.T) {
	s := newSim(t, 1, 1)
	s.waitLeader(30)
	if _, ok := s.propose("solo"); !ok {
		t.Fatal("propose failed")
	}
	s.settle()
	s.requireAllApplied("solo")
}

func TestElectsExactlyOneLeader(t *testing.T) {
	for seed := range uint64(20) {
		s := newSim(t, 3, seed)
		l := s.waitLeader(100)
		s.run(20) // let followers hear from it
		for _, id := range s.ids {
			if st := s.nodes[id].Status(); st.Lead != l {
				t.Fatalf("seed %d: node %d thinks leader is %d, want %d", seed, id, st.Lead, l)
			}
		}
	}
}

func TestReplicatesInOrderToEveryNode(t *testing.T) {
	s := newSim(t, 5, 7)
	s.waitLeader(100)
	for i := range 40 { // > MaxEntriesPerMsg, so batching and pipelining both get exercised
		if _, ok := s.propose(fmt.Sprintf("v%d", i)); !ok {
			t.Fatalf("propose %d failed", i)
		}
	}
	s.run(5)
	for i := range 40 {
		s.requireAllApplied(fmt.Sprintf("v%d", i))
	}
}

func TestLeaderCrashFailoverAndRecovery(t *testing.T) {
	s := newSim(t, 3, 42)
	old := s.waitLeader(100)
	s.propose("before-crash")
	s.run(3)

	s.crash(old)
	next := s.waitLeader(100)
	if next == old {
		t.Fatal("crashed node is still leader")
	}
	s.propose("after-crash")
	s.run(3)

	s.start(old) // restarts from its disk: term, vote and log survive
	s.run(30)
	s.requireAllApplied("before-crash")
	s.requireAllApplied("after-crash")
	if st := s.nodes[old].Status(); st.State != Follower || st.Lead != next {
		t.Fatalf("recovered node is %v following %d, want follower of %d", st.State, st.Lead, next)
	}
}

func TestPartitionedLeaderStepsDownAndLosesUncommittedWrites(t *testing.T) {
	s := newSim(t, 5, 3)
	old := s.waitLeader(100)
	s.run(5)

	var buddy uint64
	var majority []uint64
	for _, id := range s.ids {
		switch {
		case id == old:
		case buddy == None:
			buddy = id
		default:
			majority = append(majority, id)
		}
	}
	s.partition([]uint64{old, buddy}, majority)

	// The old leader still believes it leads, so it accepts a write it can never commit.
	if _, ok := s.propose("doomed"); !ok {
		t.Fatal("old leader should still accept proposals right after the split")
	}
	s.run(40) // > 2 election timeouts

	if st := s.nodes[old].Status(); st.State == Leader {
		t.Fatal("CheckQuorum: leader cut off from the majority should have stepped down")
	}
	newLeader := s.leader()
	if !slices.Contains(majority, newLeader) {
		t.Fatalf("majority side should have elected a leader, got %d", newLeader)
	}
	s.propose("survivor")
	s.run(3)

	s.heal()
	s.run(30)
	s.requireAllApplied("survivor")
	for _, id := range s.ids {
		if s.applied(id, "doomed") {
			t.Fatalf("node %d applied an entry that was never committed by a majority", id)
		}
	}
}

func TestPreVoteStopsIsolatedNodeFromDisruptingLeader(t *testing.T) {
	s := newSim(t, 3, 11)
	l := s.waitLeader(100)
	s.run(5)
	term := s.nodes[l].Status().Term

	var loner uint64
	for _, id := range s.ids {
		if id != l {
			loner = id
			break
		}
	}
	var rest []uint64
	for _, id := range s.ids {
		if id != loner {
			rest = append(rest, id)
		}
	}
	s.partition([]uint64{loner}, rest)
	s.run(200) // the loner times out ~10+ times

	if got := s.nodes[loner].Status().Term; got != term {
		t.Fatalf("isolated node inflated its term to %d (was %d); pre-vote should prevent that", got, term)
	}
	s.heal()
	s.run(30)
	if s.leader() != l || s.nodes[l].Status().Term != term {
		t.Fatalf("rejoining node disrupted the leader: leader %d term %d, want %d term %d",
			s.leader(), s.nodes[l].Status().Term, l, term)
	}
}

// TestChaos throws random crashes, restarts, partitions and packet loss at
// clusters of 3 and 5 across many seeds. Safety is checked on every message;
// at the end the cluster is healed and must make progress again (liveness).
func TestChaos(t *testing.T) {
	seeds := uint64(40)
	if testing.Short() {
		seeds = 5
	}
	if v, err := strconv.ParseUint(os.Getenv("RAFT_CHAOS_SEEDS"), 10, 32); err == nil {
		seeds = v // e.g. RAFT_CHAOS_SEEDS=5000 for a long soak
	}
	for _, size := range []int{3, 5} {
		for seed := range seeds {
			t.Run(fmt.Sprintf("n%d/seed%d", size, seed), func(t *testing.T) {
				s := newSim(t, size, seed)
				for round := range 400 {
					switch roll := s.rng.Float64(); {
					case roll < 0.02:
						s.crash(s.ids[s.rng.IntN(size)])
					case roll < 0.06:
						if id := s.ids[s.rng.IntN(size)]; !s.up(id) {
							s.start(id)
						}
					case roll < 0.08:
						clear(s.group)
						for _, id := range s.ids {
							s.group[id] = s.rng.IntN(2)
						}
					case roll < 0.11:
						clear(s.group)
					}
					s.drop = 0.1
					s.propose(fmt.Sprintf("r%d", round))
					s.tick()
				}

				s.heal()
				for _, id := range s.ids {
					if !s.up(id) {
						s.start(id)
					}
				}
				// Like a real client: a write sent to a stale leader can be
				// legitimately dropped when a newer leader overwrites it, so retry.
				for attempt := 0; !s.allApplied("final"); attempt++ {
					if attempt == 10 {
						s.requireAllApplied("final")
					}
					s.waitLeader(200)
					s.propose("final")
					s.run(30)
				}
			})
		}
	}
}
