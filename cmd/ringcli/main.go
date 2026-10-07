// Command ringcli demonstrates the consistenthashring library: adding
// nodes, lookups, weight updates, removal, and reads after Close.
package main

import (
	"fmt"
	"log"

	"consistenthashring"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func lookup(r *consistenthashring.Ring, key string) {
	node, err := r.Lookup(key)
	if err != nil {
		log.Fatalf("Lookup(%q): %v", key, err)
	}
	fmt.Printf("Lookup(%q) -> %s\n", key, node)
}

func main() {
	r := consistenthashring.New()

	fmt.Println("== add nodes ==")
	must(r.AddNode("alpha", 1))
	must(r.AddNode("beta", 2))
	must(r.AddNode("gamma", 3))
	fmt.Printf("Len=%d Snapshot=%+v\n", r.Len(), r.Snapshot())

	fmt.Println("== lookups ==")
	lookup(r, "user-1001")
	lookup(r, "user-1002")
	lookup(r, "session-abc")

	fmt.Println("== update weight: alpha 1 -> 3 ==")
	must(r.UpdateWeight("alpha", 3))
	fmt.Printf("Snapshot=%+v\n", r.Snapshot())
	lookup(r, "user-1001")

	fmt.Println("== remove node: beta ==")
	must(r.RemoveNode("beta"))
	fmt.Printf("Len=%d Snapshot=%+v\n", r.Len(), r.Snapshot())
	lookup(r, "user-1002")

	fmt.Println("== close, then keep reading ==")
	r.Close()
	r.Close() // idempotent
	if err := r.AddNode("delta", 1); err != nil {
		fmt.Printf("AddNode after Close rejected: %v\n", err)
	}
	lookup(r, "user-1001")
	fmt.Printf("Len=%d Snapshot=%+v\n", r.Len(), r.Snapshot())
}
