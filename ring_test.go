package consistenthashring

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func mustAdd(t *testing.T, r *Ring, name string, weight int) {
	t.Helper()
	if err := r.AddNode(name, weight); err != nil {
		t.Fatalf("AddNode(%q, %d): %v", name, weight, err)
	}
}

// TestLookupDeterministic: the same key always maps to the same node, and
// lookups on an empty ring fail with ErrEmptyRing.
func TestLookupDeterministic(t *testing.T) {
	r := New()
	if _, err := r.Lookup("k"); !errors.Is(err, ErrEmptyRing) {
		t.Fatalf("empty ring Lookup err = %v, want ErrEmptyRing", err)
	}
	if _, err := r.Lookup(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key Lookup err = %v, want ErrEmptyKey", err)
	}

	mustAdd(t, r, "a", 1)
	mustAdd(t, r, "b", 2)
	mustAdd(t, r, "c", 3)

	valid := map[string]bool{"a": true, "b": true, "c": true}
	keys := []string{"user-1", "user-2", "session-x", "订单-42", "key/with/slash"}
	for _, k := range keys {
		first, err := r.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		if !valid[first] {
			t.Fatalf("Lookup(%q) = %q, not a ring node", k, first)
		}
		for i := 0; i < 5; i++ {
			got, err := r.Lookup(k)
			if err != nil || got != first {
				t.Fatalf("Lookup(%q) not deterministic: %q then %q (err %v)", k, first, got, err)
			}
		}
	}
}

// TestLookupWraparound: a key hashing past the last virtual node wraps to
// the first virtual node on the ring.
func TestLookupWraparound(t *testing.T) {
	r := New()
	mustAdd(t, r, "a", 1)
	mustAdd(t, r, "b", 1)

	r.mu.RLock()
	maxHash := r.ring[len(r.ring)-1].hash
	want := r.ring[0].name
	r.mu.RUnlock()

	var key string
	for i := 0; ; i++ {
		k := fmt.Sprintf("wrap-%d", i)
		if hashKey(k) > maxHash {
			key = k
			break
		}
	}
	got, err := r.Lookup(key)
	if err != nil {
		t.Fatalf("Lookup(%q): %v", key, err)
	}
	if got != want {
		t.Fatalf("wraparound Lookup = %q, want first vnode owner %q", got, want)
	}
}

// TestWeightVirtualNodeCount: each node gets weight*virtualNodeFactor
// virtual nodes.
func TestWeightVirtualNodeCount(t *testing.T) {
	r := New()
	mustAdd(t, r, "a", 1)
	mustAdd(t, r, "b", 3)

	snap := r.Snapshot()
	if snap.VirtualNodes != 4*virtualNodeFactor {
		t.Fatalf("VirtualNodes = %d, want %d", snap.VirtualNodes, 4*virtualNodeFactor)
	}
	if r.Len() != 2 {
		t.Fatalf("Len = %d, want 2", r.Len())
	}
}

// TestUpdateWeight: only the target node's virtual nodes change; other
// nodes keep exactly the same virtual nodes on the ring.
func TestUpdateWeight(t *testing.T) {
	r := New()
	mustAdd(t, r, "a", 1)
	mustAdd(t, r, "b", 2)

	vnodesOf := func(name string) map[[2]interface{}]bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		set := make(map[[2]interface{}]bool)
		for _, v := range r.ring {
			if v.name == name {
				set[[2]interface{}{v.hash, v.idx}] = true
			}
		}
		return set
	}
	before := vnodesOf("b")

	if err := r.UpdateWeight("a", 4); err != nil {
		t.Fatalf("UpdateWeight: %v", err)
	}
	if snap := r.Snapshot(); snap.VirtualNodes != 6*virtualNodeFactor {
		t.Fatalf("VirtualNodes = %d, want %d", snap.VirtualNodes, 6*virtualNodeFactor)
	}
	after := vnodesOf("b")
	if len(before) != len(after) {
		t.Fatalf("node b vnode count changed: %d -> %d", len(before), len(after))
	}
	for k := range before {
		if !after[k] {
			t.Fatalf("node b lost virtual node %v during UpdateWeight(a)", k)
		}
	}
	if got := len(vnodesOf("a")); got != 4*virtualNodeFactor {
		t.Fatalf("node a vnode count = %d, want %d", got, 4*virtualNodeFactor)
	}
	if err := r.UpdateWeight("missing", 1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("UpdateWeight(missing) err = %v, want ErrNodeNotFound", err)
	}
	if _, err := r.Lookup("anything"); err != nil {
		t.Fatalf("Lookup after UpdateWeight: %v", err)
	}
}

// TestRemoveNode: all virtual nodes of the node are gone, nothing else
// changes.
func TestRemoveNode(t *testing.T) {
	r := New()
	mustAdd(t, r, "a", 1)
	mustAdd(t, r, "b", 2)

	if err := r.RemoveNode("a"); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}
	if snap := r.Snapshot(); snap.VirtualNodes != 2*virtualNodeFactor {
		t.Fatalf("VirtualNodes = %d, want %d", snap.VirtualNodes, 2*virtualNodeFactor)
	}
	r.mu.RLock()
	for _, v := range r.ring {
		if v.name == "a" {
			t.Fatalf("leftover virtual node of removed node: %+v", v)
		}
	}
	r.mu.RUnlock()
	if err := r.RemoveNode("a"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("second RemoveNode err = %v, want ErrNodeNotFound", err)
	}
	for _, k := range []string{"x", "y", "z"} {
		got, err := r.Lookup(k)
		if err != nil || got != "b" {
			t.Fatalf("Lookup(%q) = %q, %v; want b", k, got, err)
		}
	}
}

// TestCollisionSafety: virtual nodes with identical hashes all stay on the
// ring, order stays deterministic, and Lookup picks a stable owner.
func TestCollisionSafety(t *testing.T) {
	r := New()
	colliding := []vnode{
		{hash: 42, name: "b", idx: 1},
		{hash: 42, name: "a", idx: 1},
		{hash: 42, name: "a", idx: 0},
		{hash: 7, name: "a", idx: 2},
	}
	r.mu.Lock()
	for _, v := range colliding {
		r.insertLocked(v)
	}
	r.mu.Unlock()

	r.mu.RLock()
	if len(r.ring) != len(colliding) {
		t.Fatalf("ring has %d vnodes, want %d (collision lost one)", len(r.ring), len(colliding))
	}
	for i := 1; i < len(r.ring); i++ {
		if r.ring[i].less(r.ring[i-1]) {
			t.Fatalf("ring not sorted at %d: %+v before %+v", i, r.ring[i], r.ring[i-1])
		}
	}
	wantOrder := []vnode{
		{hash: 7, name: "a", idx: 2},
		{hash: 42, name: "a", idx: 0},
		{hash: 42, name: "a", idx: 1},
		{hash: 42, name: "b", idx: 1},
	}
	for i, want := range wantOrder {
		if r.ring[i] != want {
			t.Fatalf("ring[%d] = %+v, want %+v", i, r.ring[i], want)
		}
	}
	r.mu.RUnlock()
}

// TestValidation: invalid arguments and duplicate names are rejected.
func TestValidation(t *testing.T) {
	r := New()
	cases := []struct {
		name string
		err  error
		fn   func() error
	}{
		{"add empty name", ErrEmptyName, func() error { return r.AddNode("", 1) }},
		{"add zero weight", ErrInvalidWeight, func() error { return r.AddNode("a", 0) }},
		{"add negative weight", ErrInvalidWeight, func() error { return r.AddNode("a", -2) }},
		{"update empty name", ErrEmptyName, func() error { return r.UpdateWeight("", 1) }},
		{"update bad weight", ErrInvalidWeight, func() error { return r.UpdateWeight("a", 0) }},
		{"remove empty name", ErrEmptyName, func() error { return r.RemoveNode("") }},
	}
	for _, c := range cases {
		if err := c.fn(); !errors.Is(err, c.err) {
			t.Fatalf("%s: err = %v, want %v", c.name, err, c.err)
		}
	}
	mustAdd(t, r, "a", 1)
	if err := r.AddNode("a", 1); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("duplicate AddNode err = %v, want ErrNodeExists", err)
	}
}

// TestSnapshotAndClose: snapshots are sorted independent copies; Close
// blocks mutations but keeps reads, and is idempotent.
func TestSnapshotAndClose(t *testing.T) {
	r := New()
	mustAdd(t, r, "c", 1)
	mustAdd(t, r, "a", 2)
	mustAdd(t, r, "b", 3)

	snap := r.Snapshot()
	if len(snap.Nodes) != 3 || snap.Nodes[0].Name != "a" || snap.Nodes[1].Name != "b" || snap.Nodes[2].Name != "c" {
		t.Fatalf("snapshot nodes not sorted by name: %+v", snap.Nodes)
	}
	if snap.Nodes[0].Weight != 2 {
		t.Fatalf("snapshot weight = %d, want 2", snap.Nodes[0].Weight)
	}
	if snap.Closed {
		t.Fatal("snapshot Closed = true before Close")
	}
	snap.Nodes[0].Name = "mutated"
	snap.Nodes[0].Weight = 99
	again := r.Snapshot()
	if again.Nodes[0].Name != "a" || again.Nodes[0].Weight != 2 {
		t.Fatalf("snapshot is not an independent copy: %+v", again.Nodes[0])
	}

	r.Close()
	r.Close() // idempotent
	if err := r.AddNode("d", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddNode after Close err = %v, want ErrClosed", err)
	}
	if err := r.UpdateWeight("a", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("UpdateWeight after Close err = %v, want ErrClosed", err)
	}
	if err := r.RemoveNode("a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("RemoveNode after Close err = %v, want ErrClosed", err)
	}
	if _, err := r.Lookup("k"); err != nil {
		t.Fatalf("Lookup after Close: %v", err)
	}
	if r.Len() != 3 {
		t.Fatalf("Len after Close = %d, want 3", r.Len())
	}
	closed := r.Snapshot()
	if !closed.Closed || len(closed.Nodes) != 3 || closed.VirtualNodes != 6*virtualNodeFactor {
		t.Fatalf("snapshot after Close = %+v", closed)
	}
}

// TestConcurrent: mixed concurrent mutations and lookups stay race-free and
// keep the ring consistent.
func TestConcurrent(t *testing.T) {
	r := New()
	mustAdd(t, r, "base", 1)

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			name := fmt.Sprintf("n%d", g)
			for i := 0; i < 50; i++ {
				_ = r.AddNode(name, 1+i%3)
				_, _ = r.Lookup(fmt.Sprintf("key-%d-%d", g, i))
				_ = r.UpdateWeight(name, 1+(i+1)%3)
				_ = r.Snapshot()
				_ = r.Len()
				_ = r.RemoveNode(name)
			}
		}(g)
	}
	wg.Wait()

	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1 (only base remains)", r.Len())
	}
	r.mu.RLock()
	for _, v := range r.ring {
		if v.name != "base" {
			t.Fatalf("leftover virtual node of %q after concurrent churn", v.name)
		}
	}
	for i := 1; i < len(r.ring); i++ {
		if r.ring[i].less(r.ring[i-1]) {
			t.Fatal("ring order corrupted after concurrent churn")
		}
	}
	r.mu.RUnlock()
}
