# consistenthashring

A concurrent-safe consistent hash ring for local Windows development.
Library + tiny demo CLI + tests. Standard library only: no third-party
services, no networking, no persistence, no GUI, no database.

## Features

- Weighted physical nodes; each node owns \`weight * 100\` virtual nodes.
- Deterministic FNV-1a 64 hashing (Go standard library).
- O(log n) \`Lookup\` over a sorted virtual-node index with wraparound.
- Hash collisions never lose virtual nodes; ordering is fully deterministic.
- Safe for concurrent use (\`sync.RWMutex\`); verified with \`go test -race\`.
- \`Snapshot\` returns an independent copy: nodes sorted by name, total
  virtual node count, and the closed flag.
- \`Close\` rejects further mutations but keeps reads working; idempotent.

## API

\`\`\`go
r := consistenthashring.New()
err := r.AddNode("alpha", 1)      // name non-empty & unique, weight > 0
err  = r.UpdateWeight("alpha", 3) // rebuilds only alpha's virtual nodes
node, err := r.Lookup("user-1")   // ErrEmptyRing when no nodes
err  = r.RemoveNode("alpha")      // removes all of its virtual nodes
snap := r.Snapshot()              // Snapshot{Nodes, VirtualNodes, Closed}
n    := r.Len()                   // physical node count
r.Close()                         // mutations now return ErrClosed
\`\`\`

Sentinel errors: \`ErrEmptyRing\`, \`ErrClosed\`, \`ErrNodeExists\`,
\`ErrNodeNotFound\`, \`ErrEmptyName\`, \`ErrEmptyKey\`, \`ErrInvalidWeight\`.

## Run the demo CLI

\`\`\`sh
go run ./cmd/ringcli
\`\`\`

Demonstrates adding nodes, lookups, a weight update, a removal, and reads
after \`Close\`.

## Test

\`\`\`sh
go test -race ./...
\`\`\`

Covers deterministic lookup, wraparound, virtual-node counts per weight,
\`UpdateWeight\` isolation, \`RemoveNode\` cleanup, collision safety,
snapshot/close semantics, and basic concurrency.
