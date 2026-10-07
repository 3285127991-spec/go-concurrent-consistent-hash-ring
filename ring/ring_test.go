package ring

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestValidation(t *testing.T) {
	r := New()
	if _, err := r.Lookup("k"); !errors.Is(err, ErrEmptyRing) {
		t.Fatalf("empty ring Lookup: got %v, want ErrEmptyRing", err)
	}
	if err := r.AddNode("", 1); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("AddNode empty name: got %v, want ErrEmptyName", err)
	}
	if err := r.AddNode("a", 0); !errors.Is(err, ErrBadWeight) {
		t.Fatalf("AddNode zero weight: got %v, want ErrBadWeight", err)
	}
	if err := r.AddNode("a", -2); !errors.Is(err, ErrBadWeight) {
		t.Fatalf("AddNode negative weight: got %v, want ErrBadWeight", err)
	}
	if err := r.AddNode("a", 1); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := r.AddNode("a", 1); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("duplicate AddNode: got %v, want ErrNodeExists", err)
	}
	if _, err := r.Lookup(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("Lookup empty key: got %v, want ErrEmptyKey", err)
	}
	if err := r.UpdateWeight("missing", 1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("UpdateWeight missing: got %v, want ErrNodeNotFound", err)
	}
	if err := r.RemoveNode("missing"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("RemoveNode missing: got %v, want ErrNodeNotFound", err)
	}
}

func TestLookupDeterministic(t *testing.T) {
	r1 := New()
	r2 := New()
	for _, n := range []string{"alpha", "beta", "gamma"} {
		if err := r1.AddNode(n, 2); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"gamma", "beta", "alpha"} { // reverse order
		if err := r2.AddNode(n, 2); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		n1, err := r1.Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		n2, err := r2.Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		if n1 != n2 {
			t.Fatalf("key %q: insertion order changed mapping: %q vs %q", key, n1, n2)
		}
		again, err := r1.Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		if again != n1 {
			t.Fatalf("key %q: repeated Lookup unstable: %q vs %q", key, n1, again)
		}
	}
}

func TestLookupWrapAround(t *testing.T) {
	r := New()
	// Controlled hashes: all vnodes of a/b/c sit at positions 10/20/30.
	r.hash = func(s string) uint64 {
		switch {
		case strings.HasPrefix(s, "a#"):
			return 10
		case strings.HasPrefix(s, "b#"):
			return 20
		case strings.HasPrefix(s, "c#"):
			return 30
		}
		switch s {
		case "low-key":
			return 5
		case "mid-key":
			return 25
		case "wrap-key":
			return 35 // past the last vnode, must wrap to the first
		}
		return 0
	}
	for _, n := range []string{"a", "b", "c"} {
		if err := r.AddNode(n, 1); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[string]string{
		"low-key":  "a", // before the first vnode
		"mid-key":  "c", // between b and c
		"wrap-key": "a", // beyond the end wraps to the first vnode
	}
	for key, want := range cases {
		got, err := r.Lookup(key)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("Lookup(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestWeightVirtualNodeCount(t *testing.T) {
	r := New()
	weights := map[string]int{"n1": 1, "n2": 2, "n3": 3}
	for name, w := range weights {
		if err := r.AddNode(name, w); err != nil {
			t.Fatal(err)
		}
	}
	snap := r.Snapshot()
	if want := 6 * vnodesPerWeight; snap.VirtualNodes != want {
		t.Fatalf("VirtualNodes = %d, want %d", snap.VirtualNodes, want)
	}
	counts := map[string]int{}
	for _, vn := range r.vnodes {
		counts[vn.node]++
	}
	for name, w := range weights {
		if counts[name] != w*vnodesPerWeight {
			t.Fatalf("node %q has %d vnodes, want %d", name, counts[name], w*vnodesPerWeight)
		}
	}
}

func TestUpdateWeight(t *testing.T) {
	r := New()
	for _, n := range []string{"a", "b", "c"} {
		if err := r.AddNode(n, 1); err != nil {
			t.Fatal(err)
		}
	}
	othersBefore := vnodesOf(r, "a", "c")
	if err := r.UpdateWeight("b", 3); err != nil {
		t.Fatal(err)
	}
	if got, want := r.Snapshot().VirtualNodes, 5*vnodesPerWeight; got != want {
		t.Fatalf("VirtualNodes = %d, want %d", got, want)
	}
	othersAfter := vnodesOf(r, "a", "c")
	if fmt.Sprint(othersBefore) != fmt.Sprint(othersAfter) {
		t.Fatal("UpdateWeight changed virtual nodes of other nodes")
	}
	if got := len(vnodesOf(r, "b")); got != 3*vnodesPerWeight {
		t.Fatalf("node b has %d vnodes, want %d", got, 3*vnodesPerWeight)
	}
	if err := r.UpdateWeight("b", 0); !errors.Is(err, ErrBadWeight) {
		t.Fatalf("UpdateWeight zero: got %v, want ErrBadWeight", err)
	}
}

func TestRemoveNode(t *testing.T) {
	r := New()
	for _, n := range []string{"a", "b", "c"} {
		if err := r.AddNode(n, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.RemoveNode("b"); err != nil {
		t.Fatal(err)
	}
	if got := r.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	if got, want := r.Snapshot().VirtualNodes, 2*vnodesPerWeight; got != want {
		t.Fatalf("VirtualNodes = %d, want %d", got, want)
	}
	for _, vn := range r.vnodes {
		if vn.node == "b" {
			t.Fatal("RemoveNode left residual virtual nodes")
		}
	}
	if _, err := r.Lookup("some-key"); err != nil {
		t.Fatalf("Lookup after remove: %v", err)
	}
	if err := r.RemoveNode("b"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("re-remove: got %v, want ErrNodeNotFound", err)
	}
	if err := r.RemoveNode(""); !errors.Is(err, ErrEmptyName) {
		t.Fatalf("remove empty name: got %v, want ErrEmptyName", err)
	}
	_ = r.RemoveNode("a")
	_ = r.RemoveNode("c")
	if _, err := r.Lookup("some-key"); !errors.Is(err, ErrEmptyRing) {
		t.Fatalf("Lookup on emptied ring: got %v, want ErrEmptyRing", err)
	}
}

func TestCollisionSafety(t *testing.T) {
	r := New()
	r.hash = func(string) uint64 { return 42 } // force every vnode to collide
	if err := r.AddNode("x", 2); err != nil {
		t.Fatal(err)
	}
	if err := r.AddNode("y", 1); err != nil {
		t.Fatal(err)
	}
	if got, want := r.Snapshot().VirtualNodes, 3*vnodesPerWeight; got != want {
		t.Fatalf("VirtualNodes = %d, want %d (colliding vnodes lost)", got, want)
	}
	first, err := r.Lookup("k1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Lookup("k2")
	if err != nil {
		t.Fatal(err)
	}
	if first != "x" || second != "x" {
		t.Fatalf("collided Lookup = %q, %q; want deterministic %q", first, second, "x")
	}
	if err := r.RemoveNode("x"); err != nil {
		t.Fatal(err)
	}
	if got, want := r.Snapshot().VirtualNodes, vnodesPerWeight; got != want {
		t.Fatalf("VirtualNodes after remove = %d, want %d", got, want)
	}
	got, err := r.Lookup("k1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "y" {
		t.Fatalf("Lookup after remove = %q, want %q", got, "y")
	}
}

func TestSnapshotAndClose(t *testing.T) {
	r := New()
	for _, n := range []struct {
		name   string
		weight int
	}{{"c", 1}, {"a", 2}, {"b", 3}} {
		if err := r.AddNode(n.name, n.weight); err != nil {
			t.Fatal(err)
		}
	}
	snap := r.Snapshot()
	if snap.Closed {
		t.Fatal("Closed = true on open ring")
	}
	wantNames := []string{"a", "b", "c"}
	if len(snap.Nodes) != len(wantNames) {
		t.Fatalf("Snapshot has %d nodes, want %d", len(snap.Nodes), len(wantNames))
	}
	for i, name := range wantNames {
		if snap.Nodes[i].Name != name {
			t.Fatalf("Snapshot.Nodes[%d] = %q, want %q (not sorted)", i, snap.Nodes[i].Name, name)
		}
	}
	snap.Nodes[0].Weight = 999 // mutate the copy
	if r.Snapshot().Nodes[0].Weight != 2 {
		t.Fatal("Snapshot shares state with the ring")
	}
	r.Close()
	r.Close() // idempotent
	if !r.Snapshot().Closed {
		t.Fatal("Closed = false after Close")
	}
	if err := r.AddNode("d", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddNode after Close: got %v, want ErrClosed", err)
	}
	if err := r.UpdateWeight("a", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("UpdateWeight after Close: got %v, want ErrClosed", err)
	}
	if err := r.RemoveNode("a"); !errors.Is(err, ErrClosed) {
		t.Fatalf("RemoveNode after Close: got %v, want ErrClosed", err)
	}
	if _, err := r.Lookup("k"); err != nil {
		t.Fatalf("Lookup after Close: %v", err)
	}
	if got := r.Len(); got != 3 {
		t.Fatalf("Len after Close = %d, want 3", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := New()
	if err := r.AddNode("base", 1); err != nil {
		t.Fatal(err)
	}
	const writers = 4
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				name := fmt.Sprintf("w%d-%d", g, i)
				if err := r.AddNode(name, i+1); err != nil {
					t.Errorf("AddNode: %v", err)
				}
			}
			if err := r.UpdateWeight(fmt.Sprintf("w%d-0", g), 10); err != nil {
				t.Errorf("UpdateWeight: %v", err)
			}
			for _, i := range []int{3, 4} {
				if err := r.RemoveNode(fmt.Sprintf("w%d-%d", g, i)); err != nil {
					t.Errorf("RemoveNode: %v", err)
				}
			}
		}(g)
	}
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if _, err := r.Lookup(fmt.Sprintf("key-%d", i)); err != nil {
					t.Errorf("Lookup: %v", err) // base node is never removed
				}
				_ = r.Snapshot()
				_ = r.Len()
			}
		}(g)
	}
	wg.Wait()
	// Each writer leaves w-0 (weight 10), w-1 (2), w-2 (3): 15*vnodesPerWeight.
	wantVnodes := (1 + writers*15) * vnodesPerWeight
	if got := r.Snapshot().VirtualNodes; got != wantVnodes {
		t.Fatalf("VirtualNodes = %d, want %d", got, wantVnodes)
	}
	if got, want := r.Len(), 1+writers*3; got != want {
		t.Fatalf("Len = %d, want %d", got, want)
	}
}

// vnodesOf returns the virtual nodes of the named physical nodes, in ring order.
func vnodesOf(r *Ring, names ...string) []vnode {
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	var out []vnode
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, vn := range r.vnodes {
		if set[vn.node] {
			out = append(out, vn)
		}
	}
	return out
}
