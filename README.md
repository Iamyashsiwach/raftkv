# raftkv

## Overview

A fault-tolerant, replicated key-value store built on a from-scratch implementation of the **Raft consensus algorithm** in Go, with no dependencies beyond the standard library.

Run three nodes, kill any one of them (including the leader), and the cluster keeps accepting reads and writes. When the dead node comes back, it recovers from its write-ahead log and catches up on what it missed.

```
  NODE   ADDR              STATE             TERM  LEADER   COMMIT  APPLIED
  1      localhost:7001    follower             2       3        5        5
  2      localhost:7002    ✖ down
  3      localhost:7003    👑 leader             2       3        5        5
```

**What's implemented**

- Leader election with randomized timeouts, log replication with pipelining and batching, and commit by majority (only for current-term entries, per Raft §5.4.2)
- **PreVote**: a node that was partitioned away can't come back with an inflated term and force a pointless election
- **CheckQuorum**: a leader cut off from the majority steps down instead of accepting writes it can never commit
- Durable WAL with fsync before every acknowledgement, and recovery that tolerates a torn final write
- Linearizable reads (through the log), optional `?stale` local reads, and automatic 307 redirects to the leader
- Peer traffic authenticated with a shared token (`RAFTKV_TOKEN`, compared in constant time)

**Try it**

```bash
make cluster          # terminal 1: starts nodes 1-3
make watch            # terminal 2: live view of the cluster

curl -L -X PUT --data 'hello' localhost:7002/kv/greeting   # any node works
curl -L localhost:7003/kv/greeting
curl localhost:7001/status
```

Then kill the leader's process and watch a new one get elected in about a second.

**Tests**

```bash
make test   # race detector on everything, including a real 3-node HTTP cluster
make soak   # 10,000 seeded chaos runs against the Raft core
```

## System Architecture

```
            HTTP clients (curl -L)
                    │  PUT/GET/DELETE /kv/{key}
                    ▼
 ┌─────────────────── node (one per process) ───────────────────┐
 │  http.go      client API · 307 → leader · /raft peer endpoint │
 │                    │ proposals          ▲ results by index    │
 │                    ▼                    │                     │
 │  node.go      single-goroutine run loop: tick · step · propose│
 │                    │                                          │
 │                    ▼  Ready{HardState, Entries, Messages, Committed}
 │   ┌──────────────────────────────────────────────┐            │
 │   │ raft/   pure state machine: no I/O, no clock │            │
 │   └──────────────────────────────────────────────┘            │
 │        1. persist ▼          2. send ▼          3. apply ▼    │
 │   wal.go (fsync)      transport.go (HTTP)     KV map          │
 └──────────────────────────────────────────────────────────────┘
```

**The Raft core is a pure state machine.** `raft.Raft` has no goroutines, sockets or timers. The caller feeds it `Tick()` and `Step(msg)` and drains a `Ready` that says what to persist, what to send and what to apply, in that order. etcd's raft is built the same way, for the same reason: correctness can be tested deterministically. The test suite runs whole clusters in a seeded simulator that injects crashes, restarts, random partitions, 10% packet loss and message reordering. It checks Raft's safety properties after every message:

- *Election Safety*: never two leaders in the same term
- *State Machine Safety*: every node applies the identical entry at every index

Any failing seed replays exactly.

**One goroutine owns all Raft state.** The node's run loop is the only code that touches the state machine and the table of waiting clients. HTTP handlers talk to it over channels, so the core needs no locks. A client write becomes a proposal. The loop records the log index and term it got, and replies when that index is applied: success if the term matches, or a safe-to-retry error if a new leader overwrote the slot.

**Durability before visibility.** Every `Ready` is written to the WAL and fsynced before any message goes out or anything is applied. The WAL is append-only JSON lines. Overwriting a deposed leader's uncommitted entries is just another append ("an entry at index *i* replaces everything from *i* on"), so recovery is a straight replay.

**Transport.** Each peer gets a bounded queue and its own sender goroutine, which batches up to 64 messages per HTTP request. A slow or dead peer can't stall the others, and a full queue drops messages, which is safe because Raft retransmits on the next heartbeat.

**Known limits.** Cluster membership is static, the log grows without compaction or snapshots, and linearizable reads cost a replication round trip (ReadIndex would avoid that). Each one is the natural next step.
