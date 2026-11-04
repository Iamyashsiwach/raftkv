// Command raftkv runs a node of the replicated key-value store, or watches a
// cluster live.
//
//	raftkv serve --id 1 --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//	raftkv watch --cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
//
// Set RAFTKV_TOKEN to the same secret on every node to authenticate peer traffic.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Iamyashsiwach/raftkv/node"
	"github.com/Iamyashsiwach/raftkv/raft"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(ctx, os.Args[2:])
	case "watch":
		err = watch(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftkv:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: raftkv serve --id N --cluster 1=host:port,2=host:port,... [--data DIR]")
	fmt.Fprintln(os.Stderr, "       raftkv watch --cluster 1=host:port,2=host:port,...")
	os.Exit(2)
}

func serve(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	id := fs.Uint64("id", 0, "this node's ID (must appear in --cluster)")
	cluster := fs.String("cluster", "", "every member as id=host:port, comma-separated")
	dataDir := fs.String("data", "data", "directory for the write-ahead log")
	verbose := fs.Bool("v", false, "debug logging (shows every failed peer send)")
	fs.Parse(args)

	peers, err := parseCluster(*cluster)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	n, err := node.Open(node.Config{
		ID: *id, Peers: peers, DataDir: *dataDir,
		Token: os.Getenv("RAFTKV_TOKEN"), Logger: logger,
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", peers[*id])
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           n.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	go srv.Serve(ln)
	logger.Info("listening", "node", *id, "addr", ln.Addr())

	runErr := n.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	return runErr
}

func parseCluster(s string) (map[uint64]string, error) {
	peers := map[uint64]string{}
	for part := range strings.SplitSeq(s, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		id, err := strconv.ParseUint(idStr, 10, 64)
		if !ok || err != nil || id == 0 {
			return nil, fmt.Errorf("bad --cluster member %q (want id=host:port with id >= 1)", part)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("bad address for node %d: %w", id, err)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("node %d listed twice", id)
		}
		peers[id] = addr
	}
	if len(peers) == 0 {
		return nil, errors.New("--cluster is required")
	}
	return peers, nil
}

// watch redraws a live table of every node's Raft state. Kill a node in
// another terminal and watch the election happen.
func watch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	cluster := fs.String("cluster", "", "every member as id=host:port, comma-separated")
	fs.Parse(args)
	peers, err := parseCluster(*cluster)
	if err != nil {
		return err
	}
	ids := slices.Sorted(maps.Keys(peers))
	client := &http.Client{Timeout: 300 * time.Millisecond}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		statuses := make([]*raft.Status, len(ids))
		var wg sync.WaitGroup
		for i, id := range ids {
			wg.Go(func() { statuses[i] = fetchStatus(ctx, client, peers[id]) })
		}
		wg.Wait()
		render(ids, peers, statuses)

		select {
		case <-ctx.Done():
			fmt.Println()
			return nil
		case <-ticker.C:
		}
	}
}

func fetchStatus(ctx context.Context, c *http.Client, addr string) *raft.Status {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		return nil
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var raw struct {
		raft.Status
		State string `json:"state"`
	}
	if json.NewDecoder(resp.Body).Decode(&raw) != nil {
		return nil
	}
	st := raw.Status
	for s := raft.Follower; s <= raft.Leader; s++ {
		if s.String() == raw.State {
			st.State = s
		}
	}
	return &st
}

func render(ids []uint64, peers map[uint64]string, statuses []*raft.Status) {
	const (
		reset, dim, bold = "\x1b[0m", "\x1b[2m", "\x1b[1m"
		red, green, yel  = "\x1b[31m", "\x1b[32m", "\x1b[33m"
	)
	var b strings.Builder
	b.WriteString("\x1b[H\x1b[2J") // home + clear
	fmt.Fprintf(&b, "%sraftkv%s  %s%s%s\n\n", bold, reset, dim, time.Now().Format("15:04:05.000"), reset)
	fmt.Fprintf(&b, "  %-6s %-22s %-15s %6s %7s %8s %8s\n", "NODE", "ADDR", "STATE", "TERM", "LEADER", "COMMIT", "APPLIED")
	for i, id := range ids {
		st := statuses[i]
		if st == nil {
			fmt.Fprintf(&b, "  %-6d %-22s %s%-15s%s\n", id, peers[id], red, "✖ down", reset)
			continue
		}
		color, label := dim, st.State.String()
		switch st.State {
		case raft.Leader:
			color, label = green+bold, "👑 leader"
		case raft.Candidate, raft.PreCandidate:
			color = yel
		}
		fmt.Fprintf(&b, "  %-6d %-22s %s%-15s%s %6d %7d %8d %8d\n",
			id, peers[id], color, label, reset, st.Term, st.Lead, st.Commit, st.Applied)
	}
	b.WriteString("\n" + dim + "  ctrl-c to quit · kill the leader to watch an election" + reset + "\n")
	fmt.Print(b.String())
}
