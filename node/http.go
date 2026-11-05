package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Iamyashsiwach/raftkv/raft"
)

const requestTimeout = 5 * time.Second

// Handler serves the client API and the internal peer endpoint:
//
//	PUT    /kv/{key}          body is the value
//	GET    /kv/{key}          linearizable; add ?stale to read this node's copy
//	DELETE /kv/{key}
//	GET    /status
//	POST   /raft              peer-to-peer Raft messages
//
// Writes and linearizable reads sent to a follower get a 307 to the leader,
// so `curl -L` works against any node.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", n.handlePut)
	mux.HandleFunc("GET /kv/{key}", n.handleGet)
	mux.HandleFunc("DELETE /kv/{key}", n.handleDelete)
	mux.HandleFunc("GET /status", n.handleStatus)
	mux.HandleFunc("POST /raft", n.handleRaft)
	return mux
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
	if err != nil {
		http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	if err := n.Put(ctx, r.PathValue("key"), string(body)); err != nil {
		n.writeErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var (
		v     string
		found bool
		err   error
	)
	if r.URL.Query().Has("stale") {
		v, found, err = n.StaleGet(key)
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		v, found, err = n.Get(ctx, key)
	}
	if err != nil {
		n.writeErr(w, r, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream") // never let a browser sniff stored values as HTML
	io.WriteString(w, v)
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	found, err := n.Delete(ctx, r.PathValue("key"))
	if err != nil {
		n.writeErr(w, r, err)
		return
	}
	if !found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(n.Status())
}

func (n *Node) handleRaft(w http.ResponseWriter, r *http.Request) {
	if !n.tr.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var msgs []raft.Message
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRaftBody)).Decode(&msgs); err != nil {
		http.Error(w, "bad message batch", http.StatusBadRequest)
		return
	}
	for _, m := range msgs {
		if m.To != n.cfg.ID {
			continue // misrouted; the Raft core would ignore it anyway
		}
		select {
		case n.recvc <- m:
		case <-r.Context().Done():
			return
		case <-n.done:
			http.Error(w, "stopped", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (n *Node) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	var nl *NotLeaderError
	switch {
	case errors.As(err, &nl) && nl.Addr != "":
		// 307 keeps the method and body, so clients can simply follow it.
		http.Redirect(w, r, "http://"+nl.Addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	case errors.As(err, &nl), errors.Is(err, ErrDropped), errors.Is(err, ErrStopped):
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, ErrInvalid):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "timed out waiting for commit; the write may or may not have applied", http.StatusGatewayTimeout)
	default:
		n.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
