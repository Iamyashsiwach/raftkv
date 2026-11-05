package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Iamyashsiwach/raftkv/raft"
)

// testCluster runs real nodes over real HTTP on localhost, with a fast clock.
type testCluster struct {
	t     *testing.T
	peers map[uint64]string
	dirs  map[uint64]string
	nodes map[uint64]*running
}

type running struct {
	n      *Node
	srv    *http.Server
	cancel context.CancelFunc
	exited chan error
}

func newTestCluster(t *testing.T, size int) *testCluster {
	c := &testCluster{t: t, peers: map[uint64]string{}, dirs: map[uint64]string{}, nodes: map[uint64]*running{}}
	lns := map[uint64]net.Listener{}
	for id := uint64(1); id <= uint64(size); id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id], c.peers[id], c.dirs[id] = ln, ln.Addr().String(), t.TempDir()
	}
	for id, ln := range lns {
		c.startOn(id, ln)
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) startOn(id uint64, ln net.Listener) {
	c.t.Helper()
	n, err := Open(Config{
		ID: id, Peers: c.peers, DataDir: c.dirs[id], Token: "test-secret",
		Tick:   10 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{n: n, srv: &http.Server{Handler: n.Handler()}, cancel: cancel, exited: make(chan error, 1)}
	go r.srv.Serve(ln)
	go func() { r.exited <- n.Run(ctx) }()
	c.nodes[id] = r
}

func (c *testCluster) restart(id uint64) {
	c.t.Helper()
	var ln net.Listener
	var err error
	for range 50 { // the old socket may take a moment to release the port
		if ln, err = net.Listen("tcp", c.peers[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	c.startOn(id, ln)
}

func (c *testCluster) stop(id uint64) {
	r := c.nodes[id]
	r.cancel()
	r.srv.Close()
	if err := <-r.exited; err != nil {
		c.t.Errorf("node %d exited with %v", id, err)
	}
	delete(c.nodes, id)
}

func (c *testCluster) waitLeader() uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, r := range c.nodes {
			if st := r.n.Status(); st.State == raft.Leader {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader within 10s")
	return 0
}

func (c *testCluster) follower(not uint64) uint64 {
	for id := range c.nodes {
		if id != not {
			return id
		}
	}
	return 0
}

func (c *testCluster) do(method string, id uint64, path, body string) (int, string) {
	c.t.Helper()
	// Retry 503s like a real client would while an election settles.
	for range 100 {
		req, _ := http.NewRequest(method, "http://"+c.peers[id]+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req) // follows the 307 to the leader, resending the body
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			c.t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			return resp.StatusCode, string(b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("%s %s on node %d: still unavailable after retries", method, path, id)
	return 0, ""
}

func (c *testCluster) eventually(what string, ok func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClusterEndToEnd(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitLeader()
	f := c.follower(leader)

	// Writes and reads sent to a follower get redirected to the leader.
	if code, _ := c.do("PUT", f, "/kv/greeting", "hello raft"); code != http.StatusNoContent {
		t.Fatalf("PUT via follower = %d", code)
	}
	if code, body := c.do("GET", f, "/kv/greeting", ""); code != 200 || body != "hello raft" {
		t.Fatalf("GET = %d %q", code, body)
	}
	if code, _ := c.do("GET", leader, "/kv/missing", ""); code != http.StatusNotFound {
		t.Fatalf("GET missing = %d", code)
	}
	if code, _ := c.do("PUT", leader, "/kv/"+strings.Repeat("k", 300), "x"); code != http.StatusBadRequest {
		t.Fatalf("oversized key = %d, want 400", code)
	}

	// Every node's local copy converges.
	for id := range c.nodes {
		c.eventually("replica convergence", func() bool {
			code, body := c.do("GET", id, "/kv/greeting?stale", "")
			return code == 200 && body == "hello raft"
		})
	}

	// Kill the leader: the survivors elect a new one and keep serving.
	c.stop(leader)
	newLeader := c.waitLeader()
	if newLeader == leader {
		t.Fatal("stopped node still reported as leader")
	}
	if code, _ := c.do("PUT", newLeader, "/kv/after-failover", "still here"); code != http.StatusNoContent {
		t.Fatalf("PUT after failover = %d", code)
	}
	if code, _ := c.do("DELETE", newLeader, "/kv/greeting", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d", code)
	}

	// The old leader comes back from its WAL and catches up on what it missed.
	c.restart(leader)
	c.eventually("restarted node to catch up", func() bool {
		code, body := c.do("GET", leader, "/kv/after-failover?stale", "")
		gone, _ := c.do("GET", leader, "/kv/greeting?stale", "")
		return code == 200 && body == "still here" && gone == http.StatusNotFound
	})
}

func TestPeerEndpointRequiresToken(t *testing.T) {
	c := newTestCluster(t, 1)
	resp, err := http.Post("http://"+c.peers[1]+"/raft", "application/json", strings.NewReader("[]"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /raft = %d, want 401", resp.StatusCode)
	}
}
