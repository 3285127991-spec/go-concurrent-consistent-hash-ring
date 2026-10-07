# go-concurrent-consistent-hash-ring

A concurrent-safe consistent hash ring in pure Go (standard library only).
Library + small demo CLI + tests. No network, persistence, or third-party deps.

## How it works

- Each physical node is expanded into `weight * 100` virtual nodes, hashed with
  FNV-1a 64-bit (`hash/fnv`) and kept in a sorted in-memory index.
- `Lookup` hashes the key and binary-searches the first virtual node clockwise,
  wrapping around at the end of the ring: **O(log n)**.
- Virtual nodes are ordered by `(hash, node, index)`, a total deterministic
  order, so hash collisions never overwrite or drop virtual nodes.
- A single `sync.RWMutex` guards all state: mutations take the write lock,
  `Lookup`/`Snapshot`/`Len` take the read lock.

## API

| Method | Behavior |
| --- | --- |
| `New() *Ring` | Empty ring. |
| `AddNode(name, weight) error` | Fails on empty name, `weight <= 0`, duplicate name, closed ring. |
| `UpdateWeight(name, weight) error` | Replaces only that node's virtual nodes; others untouched. |
| `RemoveNode(name) error` | Removes the node and all its virtual nodes. |
| `Lookup(key) (string, error)` | Empty key -> `ErrEmptyKey`; empty ring -> `ErrEmptyRing`. |
| `Snapshot() Snapshot` | Independent copy: nodes sorted by name, total vnode count, `Closed`. |
| `Len() int` | Number of physical nodes. |
| `Close()` | Idempotent. Mutations are rejected (`ErrClosed`); reads keep working. |

## Layout

- `ring/ring.go` - the library
- `ring/ring_test.go` - tests (determinism, wrap-around, weights, updates,
  removal, collisions, snapshot/close, concurrency)
- `cmd/ringdemo/main.go` - demo CLI

## Run

```sh
go run ./cmd/ringdemo
go test -race ./...
```
