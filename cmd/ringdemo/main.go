// Command ringdemo demonstrates the consistent hash ring library:
// adding nodes, lookups, weight updates, removal, and reads after Close.
package main

import (
	"fmt"
	"log"

	"consistenthashring/ring"
)

var keys = []string{"user-1", "user-7", "user-42", "order-100"}

func main() {
	r := ring.New()

	fmt.Println("== AddNode: alpha(1), beta(2), gamma(3) ==")
	must(r.AddNode("alpha", 1))
	must(r.AddNode("beta", 2))
	must(r.AddNode("gamma", 3))
	printSnapshot(r)
	lookupKeys(r)

	fmt.Println("== UpdateWeight: beta 2 -> 4 ==")
	must(r.UpdateWeight("beta", 4))
	printSnapshot(r)
	lookupKeys(r)

	fmt.Println("== RemoveNode: gamma ==")
	must(r.RemoveNode("gamma"))
	printSnapshot(r)
	lookupKeys(r)

	fmt.Println("== Close ==")
	r.Close()
	r.Close() // idempotent
	fmt.Println("AddNode after Close:", r.AddNode("delta", 1))
	fmt.Println("Reads still work after Close:")
	lookupKeys(r)
	printSnapshot(r)
}

func lookupKeys(r *ring.Ring) {
	for _, k := range keys {
		node, err := r.Lookup(k)
		if err != nil {
			log.Fatalf("Lookup(%q): %v", k, err)
		}
		fmt.Printf("  Lookup(%-10q) -> %s\n", k, node)
	}
}

func printSnapshot(r *ring.Ring) {
	snap := r.Snapshot()
	fmt.Printf("  Len=%d VirtualNodes=%d Closed=%v Nodes=%v\n",
		r.Len(), snap.VirtualNodes, snap.Closed, snap.Nodes)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
