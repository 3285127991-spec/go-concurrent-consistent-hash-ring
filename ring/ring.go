// Package ring implements a concurrent-safe consistent hash ring.
//
// Each physical node is expanded into weight*vnodesPerWeight virtual nodes
// placed on a sorted hash ring. Lookup hashes the key and walks clockwise
// to the first virtual node, wrapping around at the end of the ring.
package ring

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// vnodesPerWeight is the fixed number of virtual nodes created per weight unit.
const vnodesPerWeight = 100

var (
	ErrEmptyRing    = errors.New("ring: no nodes in the ring")
	ErrClosed       = errors.New("ring: closed")
	ErrEmptyName    = errors.New("ring: node name must not be empty")
	ErrEmptyKey     = errors.New("ring: key must not be empty")
	ErrBadWeight    = errors.New("ring: weight must be > 0")
	ErrNodeExists   = errors.New("ring: node already exists")
	ErrNodeNotFound = errors.New("ring: node not found")
)

// NodeInfo describes one physical node.
type NodeInfo struct {
	Name   string
	Weight int
}

// Snapshot is an independent copy of the ring state.
type Snapshot struct {
	Nodes        []NodeInfo // sorted by Name
	VirtualNodes int        // total number of virtual nodes on the ring
	Closed       bool
}

// vnode is one virtual node on the sorted ring index.
type vnode struct {
	hash uint64
	node string // owning physical node name
	idx  int    // virtual node index within the owner, for deterministic order
}

// Ring is a concurrent-safe consistent hash ring.
type Ring struct {
	mu     sync.RWMutex
	hash   func(string) uint64
	nodes  map[string]int // physical node name -> weight
	vnodes []vnode        // sorted by (hash, node, idx)
	closed bool
}

// New returns an empty ring using FNV-1a 64-bit hashing from the standard library.
func New() *Ring {
	return &Ring{
		hash:  fnvHash,
		nodes: make(map[string]int),
	}
}

func fnvHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// AddNode adds a physical node with the given weight.
// It fails if the name is empty, the weight is not positive,
// the node already exists, or the ring is closed.
func (r *Ring) AddNode(name string, weight int) error {
	if name == "" {
		return ErrEmptyName
	}
	if weight <= 0 {
		return ErrBadWeight
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
	r.addVnodes(name, weight)
	return nil
}

// UpdateWeight changes the weight of an existing node, replacing only that
// node's virtual nodes. All other nodes' virtual nodes are left untouched.
func (r *Ring) UpdateWeight(name string, weight int) error {
	if name == "" {
		return ErrEmptyName
	}
	if weight <= 0 {
		return ErrBadWeight
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	if _, ok := r.nodes[name]; !ok {
		return ErrNodeNotFound
	}
	r.removeVnodes(name)
	r.nodes[name] = weight
	r.addVnodes(name, weight)
	return nil
}

// RemoveNode removes a physical node and all of its virtual nodes.
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
	delete(r.nodes, name)
	r.removeVnodes(name)
	return nil
}

// Lookup returns the name of the physical node responsible for key.
// It walks clockwise from the key's hash to the first virtual node and
// wraps around at the end of the ring. It fails on an empty key or ring.
func (r *Ring) Lookup(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.vnodes) == 0 {
		return "", ErrEmptyRing
	}
	h := r.hash(key)
	i := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if i == len(r.vnodes) {
		i = 0 // wrap around to the start of the ring
	}
	return r.vnodes[i].node, nil
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
		VirtualNodes: len(r.vnodes),
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

// addVnodes appends the virtual nodes of one physical node and re-sorts the
// index. The caller must hold the write lock.
func (r *Ring) addVnodes(name string, weight int) {
	for i := 0; i < weight*vnodesPerWeight; i++ {
		r.vnodes = append(r.vnodes, vnode{
			hash: r.hash(name + "#" + strconv.Itoa(i)),
			node: name,
			idx:  i,
		})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return lessVnode(r.vnodes[i], r.vnodes[j])
	})
}

// removeVnodes drops all virtual nodes of one physical node, keeping the
// remaining index sorted. The caller must hold the write lock.
func (r *Ring) removeVnodes(name string) {
	kept := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.node != name {
			kept = append(kept, vn)
		}
	}
	// Clear the tail so removed entries are not retained by the backing array.
	for i := len(kept); i < len(r.vnodes); i++ {
		r.vnodes[i] = vnode{}
	}
	r.vnodes = kept
}

// lessVnode is a total, deterministic order: hash, then node name, then index.
// Colliding hashes never overwrite or drop virtual nodes.
func lessVnode(a, b vnode) bool {
	if a.hash != b.hash {
		return a.hash < b.hash
	}
	if a.node != b.node {
		return a.node < b.node
	}
	return a.idx < b.idx
}
