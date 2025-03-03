package main

import (
	"fmt"
	bolt "github.com/boltdb/bolt"
	circular "github.com/0187773933/BoltCircular/v1/circular"
)

func main() {
	// Open the BoltDB database (will panic if it fails).
	db, _ := bolt.Open("my.db", 0600, nil)
	defer db.Close()

	// Create a new circular list.
	cl := circular.Open(db, "circular-test")

	// Add some values.
	cl.Add([]byte("A"))
	cl.Add([]byte("B"))
	cl.Add([]byte("C"))

	// Try to add "B" only if it doesn't exist.
	added := cl.AddNx([]byte("B")) // Will not add because "B" exists.
	fmt.Println("AddNx B added?", added)

	// Add "E" with AddNx.
	added = cl.AddNx([]byte("E"))
	fmt.Println("AddNx E added?", added)

	// Get the current element.
	val, idx, total := cl.Current()
	fmt.Printf("Current: %s (index %d of %d)\n", val, idx, total)

	// Iterate to the next element.
	next := cl.Next()
	fmt.Println("Next:", string(next))

	// Remove the current element.
	cl.Remove()
	fmt.Println("Removed current element.")

	// Get the new current element.
	val, idx, total = cl.Current()
	fmt.Printf("Now Current: %s (index %d of %d)\n", val, idx, total)

	next = cl.Next()
	fmt.Println("Next:", string(next))

	next = cl.Next()
	fmt.Println("Next:", string(next))

	next = cl.Next()
	fmt.Println("Next:", string(next))

	previous := cl.Previous()
	fmt.Println("Previous:", string(previous))

	previous = cl.Previous()
	fmt.Println("Previous:", string(previous))

	previous = cl.Previous()
	fmt.Println("Previous:", string(previous))
}