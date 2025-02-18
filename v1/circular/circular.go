// Package circular implements a dynamic circular list using BoltDB
// from github.com/boltdb/bolt. It supports Add, AddNx (add if not exists),
// Remove (which removes the current element), Current (returns the current value,
// its index and total count), Next, and Previous. This version panics on failures.
package circular

import (
	"bytes"
	"encoding/binary"

	bolt "github.com/boltdb/bolt"
)

// CircularList holds the BoltDB handle, bucket name, in-memory slice of keys,
// the current pointer, and the next key to be assigned.
type CircularList struct {
	db           *bolt.DB
	bucketName   []byte
	keys         []int // slice of auto-incremented keys (in insertion order)
	currentIndex int   // index into keys slice for the current element
	nextKey      int   // next key to use when inserting new items
}

// New creates a new CircularList using the given BoltDB handle and bucket name.
// It deletes any existing bucket with that name so you always start with a fresh list.
func New(db *bolt.DB, bucketName string) *CircularList {
	cl := &CircularList{
		db:           db,
		bucketName:   []byte(bucketName),
		keys:         []int{},
		currentIndex: 0,
		nextKey:      0,
	}
	// Delete any existing bucket and create a new one.
	db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket(cl.bucketName)
		_, _ = tx.CreateBucket(cl.bucketName)
		return nil
	})
	return cl
}

// Add inserts a new value into the circular list with an auto-incremented key.
func (cl *CircularList) Add(value []byte) {
	cl.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.bucketName)
		b.Put(intToKey(cl.nextKey), value)
		return nil
	})
	cl.keys = append(cl.keys, cl.nextKey)
	cl.nextKey++
}

// AddNx adds a new value only if an identical value is not already present.
// Returns true if the value was added; otherwise false.
func (cl *CircularList) AddNx(value []byte) bool {
	if cl.exists(value) {
		return false
	}
	cl.Add(value)
	return true
}

// exists scans the in-memory keys and returns true if a stored value equals value.
func (cl *CircularList) exists(value []byte) bool {
	var found bool
	cl.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.bucketName)
		for _, keyInt := range cl.keys {
			v := b.Get(intToKey(keyInt))
			if v != nil && bytes.Equal(v, value) {
				found = true
				break
			}
		}
		return nil
	})
	return found
}

// Remove deletes the current element from the list and advances the pointer.
func (cl *CircularList) Remove() {
	if len(cl.keys) == 0 {
		return
	}
	keyInt := cl.keys[cl.currentIndex]
	cl.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.bucketName)
		b.Delete(intToKey(keyInt))
		return nil
	})
	// Remove the key from the in-memory slice.
	cl.keys = append(cl.keys[:cl.currentIndex], cl.keys[cl.currentIndex+1:]...)
	if len(cl.keys) == 0 {
		cl.currentIndex = 0
	} else if cl.currentIndex >= len(cl.keys) {
		cl.currentIndex = 0
	}
}

// Current returns the current element's value, its 0-based index, and the total count.
func (cl *CircularList) Current() ([]byte, int, int) {
	if len(cl.keys) == 0 {
		return nil, 0, 0
	}
	keyInt := cl.keys[cl.currentIndex]
	var value []byte
	cl.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.bucketName)
		v := b.Get(intToKey(keyInt))
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	return value, cl.currentIndex, len(cl.keys)
}

// Next advances the pointer (wrapping around) and returns the next element's value.
func (cl *CircularList) Next() []byte {
	if len(cl.keys) == 0 {
		return nil
	}
	cl.currentIndex = (cl.currentIndex + 1) % len(cl.keys)
	return cl.getValueByIndex(cl.currentIndex)
}

// Previous moves the pointer backward (wrapping around) and returns the previous element's value.
func (cl *CircularList) Previous() []byte {
	if len(cl.keys) == 0 {
		return nil
	}
	cl.currentIndex = (cl.currentIndex - 1 + len(cl.keys)) % len(cl.keys)
	return cl.getValueByIndex(cl.currentIndex)
}

// getValueByIndex retrieves the stored value for the key at the given index.
func (cl *CircularList) getValueByIndex(i int) []byte {
	var value []byte
	cl.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.bucketName)
		keyInt := cl.keys[i]
		v := b.Get(intToKey(keyInt))
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	return value
}

// intToKey converts an integer into an 8-byte big-endian key.
func intToKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}
