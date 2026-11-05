CLUSTER ?= 1=localhost:7001,2=localhost:7002,3=localhost:7003

.PHONY: build test soak cluster watch clean

build:
	go build -o bin/raftkv ./cmd/raftkv

test:
	go test -race ./...

# Thousands of seeded crash/partition/packet-loss scenarios against the Raft core.
soak:
	RAFT_CHAOS_SEEDS=5000 go test -run TestChaos ./raft/

# Three nodes in the foreground; ctrl-c stops all of them.
cluster: build
	@trap 'kill 0' INT TERM; \
	for id in 1 2 3; do ./bin/raftkv serve --id $$id --cluster $(CLUSTER) --data data & done; \
	wait

watch: build
	./bin/raftkv watch --cluster $(CLUSTER)

clean:
	rm -rf bin data
