// Package consistenthashring implements a concurrent-safe consistent hash
// ring with weighted virtual nodes. It depends only on the Go standard
// library.
package consistenthashring

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// virtualNodeFactor is the fixed number of virtual nodes created per unit of
// weight for each physical node.
const virtualNodeFactor = 100

var (
	ErrEmptyRing     = errors.New("consistenthashring: ring is empty")
	ErrClosed        = errors.New("consistenthashring: ring is closed")
	ErrNodeExists    = errors.New("consistenthashring: node already exists")
	ErrNodeNotFound  = errors.New("consistenthashring: node not found")
	ErrEmptyName     = errors.New("consistenthashring: node name must not be empty")
	ErrEmptyKey      = errors.New("consistenthashring: key must not be empty")
	ErrInvalidWeight = errors.New("consistenthashring: weight must be > 0")
)

// vnode is a single virtual node on the ring.
type vnode struct {
	hash uint64
	name string // owning physical node
	idx  int    // virtual node index, deterministic tie-breaker
}

// less orders vnodes deterministically, even when hashes collide.
func (v vnode) less(o vnode) bool {
	if v.hash != o.hash {
		return v.hash < o.hash
	}
	if v.name != o.name {
		return v.name < o.name
	}
	return v.idx < o.idx
}

// NodeInfo describes one physical node in a Snapshot.
type NodeInfo struct {
	Name   string
	Weight int
}

// Snapshot is an independent copy of the ring state at a point in time.
type Snapshot struct {
	Nodes        []NodeInfo // sorted by Name
	VirtualNodes int        // total virtual nodes on the ring
	Closed       bool
}

// Ring is a concurrent-safe consistent hash ring.
type Ring struct {
	mu     sync.RWMutex
	nodes  map[string]int // physical node name -> weight
	ring   []vnode        // sorted by (hash, name, idx)
	closed bool
}

// New returns an empty, ready-to-use Ring.
func New() *Ring {
	return &Ring{nodes: make(map[string]int)}
}

// hashKey hashes s with FNV-1a 64, a deterministic stdlib algorithm.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// vnodesFor builds the weight*virtualNodeFactor virtual nodes of a node.
func vnodesFor(name string, weight int) []vnode {
	vns := make([]vnode, 0, weight*virtualNodeFactor)
	for i := 0; i < weight*virtualNodeFactor; i++ {
		vns = append(vns, vnode{
			hash: hashKey(name + "#" + strconv.Itoa(i)),
			name: name,
			idx:  i,
		})
	}
	return vns
}

// insertLocked inserts v into the sorted ring, keeping deterministic order.
// Hash collisions never overwrite: every vnode gets its own slot.
func (r *Ring) insertLocked(v vnode) {
	pos := sort.Search(len(r.ring), func(i int) bool { return !r.ring[i].less(v) })
	r.ring = append(r.ring, vnode{})
	copy(r.ring[pos+1:], r.ring[pos:])
	r.ring[pos] = v
}

// removeVNodesLocked drops every virtual node owned by name.
func (r *Ring) removeVNodesLocked(name string) {
	kept := r.ring[:0]
	for _, v := range r.ring {
		if v.name != name {
			kept = append(kept, v)
		}
	}
	r.ring = kept
}

// AddNode adds a physical node with the given weight. The name must be
// non-empty, unique, and weight must be > 0.
func (r *Ring) AddNode(name string, weight int) error {
	if name == "" {
		return ErrEmptyName
	}
	if weight <= 0 {
		return ErrInvalidWeight
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, ok := r.nodes[name]; ok {
		return ErrNodeExists
	}
	r.nodes[name] = weight
	for _, v := range vnodesFor(name, weight) {
		r.insertLocked(v)
	}
	return nil
}

// UpdateWeight changes the weight of an existing node. Only that node's
// virtual nodes are rebuilt; all other virtual nodes stay untouched.
func (r *Ring) UpdateWeight(name string, weight int) error {
	if name == "" {
		return ErrEmptyName
	}
	if weight <= 0 {
		return ErrInvalidWeight
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, ok := r.nodes[name]; !ok {
		return ErrNodeNotFound
	}
	r.removeVNodesLocked(name)
	r.nodes[name] = weight
	for _, v := range vnodesFor(name, weight) {
		r.insertLocked(v)
	}
	return nil
}

// RemoveNode deletes a physical node and all of its virtual nodes.
func (r *Ring) RemoveNode(name string) error {
	if name == "" {
		return ErrEmptyName
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, ok := r.nodes[name]; !ok {
		return ErrNodeNotFound
	}
	r.removeVNodesLocked(name)
	delete(r.nodes, name)
	return nil
}

// Lookup returns the name of the physical node responsible for key: the
// first virtual node clockwise from the key's hash, wrapping around at the
// end of the ring. It returns ErrEmptyRing when the ring has no nodes.
func (r *Ring) Lookup(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	h := hashKey(key)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", ErrEmptyRing
	}
	i := sort.Search(len(r.ring), func(i int) bool { return r.ring[i].hash >= h })
	if i == len(r.ring) {
		i = 0 // wrap around to the first virtual node
	}
	return r.ring[i].name, nil
}

// Snapshot returns an independent copy of the ring state: physical nodes
// sorted by name, the total virtual node count, and the closed flag.
func (r *Ring) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]NodeInfo, 0, len(r.nodes))
	for name, weight := range r.nodes {
		nodes = append(nodes, NodeInfo{Name: name, Weight: weight})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return Snapshot{
		Nodes:        nodes,
		VirtualNodes: len(r.ring),
		Closed:       r.closed,
	}
}

// Len returns the number of physical nodes.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Close marks the ring closed. AddNode, UpdateWeight and RemoveNode are
// rejected afterwards, while Lookup, Snapshot and Len keep working.
// Close is idempotent.
func (r *Ring) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}
