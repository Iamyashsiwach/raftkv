package node

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Iamyashsiwach/raftkv/raft"
)

const (
	peerQueueSize = 1024
	maxBatch      = 64
	maxRaftBody   = 32 << 20
)

// transport ships Raft messages to peers over HTTP. Each peer gets its own
// bounded queue and sender goroutine, so one slow or dead peer can't stall
// the others or the Raft loop. A full queue drops messages: Raft is built
// for a lossy network and retransmits on the next heartbeat anyway.
type transport struct {
	token  string
	client *http.Client
	peers  map[uint64]string
	queues map[uint64]chan raft.Message
	log    *slog.Logger
}

func newTransport(self uint64, peers map[uint64]string, token string, log *slog.Logger) *transport {
	t := &transport{
		token:  token,
		client: &http.Client{Timeout: time.Second},
		peers:  peers,
		queues: map[uint64]chan raft.Message{},
		log:    log,
	}
	for id := range peers {
		if id != self {
			t.queues[id] = make(chan raft.Message, peerQueueSize)
		}
	}
	return t
}

func (t *transport) start(ctx context.Context) {
	for id, q := range t.queues {
		go t.pump(ctx, id, q)
	}
}

func (t *transport) send(msgs []raft.Message) {
	for _, m := range msgs {
		select {
		case t.queues[m.To] <- m:
		default:
			t.log.Debug("peer queue full, dropping message", "to", m.To)
		}
	}
}

func (t *transport) pump(ctx context.Context, to uint64, q chan raft.Message) {
	url := "http://" + t.peers[to] + "/raft"
	for {
		var batch []raft.Message
		select {
		case <-ctx.Done():
			return
		case m := <-q:
			batch = append(batch, m)
		}
	drain:
		for len(batch) < maxBatch {
			select {
			case m := <-q:
				batch = append(batch, m)
			default:
				break drain
			}
		}
		if err := t.post(ctx, url, batch); err != nil {
			t.log.Debug("send failed", "to", to, "err", err) // peers being down is routine; Raft copes
		}
	}
}

func (t *transport) post(ctx context.Context, url string, batch []raft.Message) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection is reused
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer answered %s", resp.Status)
	}
	return nil
}

// authorized checks the shared cluster token in constant time. With no token
// configured the /raft endpoint is open: fine on localhost, not on a network.
func (t *transport) authorized(r *http.Request) bool {
	if t.token == "" {
		return true
	}
	got := []byte(r.Header.Get("Authorization"))
	want := []byte("Bearer " + t.token)
	return subtle.ConstantTimeCompare(got, want) == 1
}
