package circular

import (
	"bytes"
	"encoding/binary"
	"errors"

	bolt "github.com/boltdb/bolt"
)

const (
	metaCurrentKey = "__current__" // key for the current pointer
	metaNextKey    = "__next__"    // key for the next auto-increment value
	dataPrefix     = "i"           // prefix for each stored item key
)

// encodeUint64 converts a uint64 to an 8-byte big-endian slice.
func encodeUint64(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

// decodeUint64 converts an 8-byte big-endian slice into a uint64.
func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// intToKey converts an integer into an 8-byte big-endian key.
func intToKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

// dataKey returns the full key for storing an item:
// the dataPrefix followed by the 8-byte big-endian representation.
func dataKey(i int) []byte {
	prefix := []byte(dataPrefix)
	return append(prefix, intToKey(i)...)
}

// CircularList represents a persistent circular list stored in a single BoltDB bucket.
type CircularList struct {
	DB         *bolt.DB
	BucketName []byte
}

// New creates a new circular list in the given bucket,
// deleting any existing bucket with that name.
func New(db *bolt.DB, bucketName string) *CircularList {
	cl := &CircularList{
		DB:         db,
		BucketName: []byte(bucketName),
	}
	db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(cl.BucketName)
		b, err := tx.CreateBucket(cl.BucketName)
		if err != nil {
			return err
		}
		// Initialize meta keys.
		if err := b.Put([]byte(metaCurrentKey), encodeUint64(0)); err != nil {
			return err
		}
		return b.Put([]byte(metaNextKey), encodeUint64(0))
	})
	return cl
}

// Open opens an existing circular list (or creates one if not present)
// from the given bucket name, and ensures that meta keys exist.
func Open(db *bolt.DB, bucketName string) *CircularList {
	cl := &CircularList{
		DB:         db,
		BucketName: []byte(bucketName),
	}
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		if b == nil {
			var err error
			b, err = tx.CreateBucket(cl.BucketName)
			if err != nil {
				return err
			}
		}
		// Ensure the meta keys exist.
		if b.Get([]byte(metaCurrentKey)) == nil {
			if err := b.Put([]byte(metaCurrentKey), encodeUint64(0)); err != nil {
				return err
			}
		}
		if b.Get([]byte(metaNextKey)) == nil {
			if err := b.Put([]byte(metaNextKey), encodeUint64(0)); err != nil {
				return err
			}
		}
		return nil
	})
	return cl
}

// getOrderedDataKeys scans the bucket for all keys with the dataPrefix,
// returning them in sorted order.
func getOrderedDataKeys(b *bolt.Bucket) ([][]byte, error) {
	var keys [][]byte
	c := b.Cursor()
	prefix := []byte(dataPrefix)
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		keys = append(keys, k)
	}
	return keys, nil
}

// getMetaCurrent reads the current pointer from the bucket.
func getMetaCurrent(b *bolt.Bucket) (uint64, error) {
	v := b.Get([]byte(metaCurrentKey))
	if v == nil {
		return 0, errors.New("meta current not found")
	}
	return decodeUint64(v), nil
}

// getMetaNext reads the next auto-increment value from the bucket.
func getMetaNext(b *bolt.Bucket) (uint64, error) {
	v := b.Get([]byte(metaNextKey))
	if v == nil {
		return 0, errors.New("meta next not found")
	}
	return decodeUint64(v), nil
}

func setMetaCurrent(b *bolt.Bucket, val uint64) error {
	return b.Put([]byte(metaCurrentKey), encodeUint64(val))
}

func setMetaNext(b *bolt.Bucket, val uint64) error {
	return b.Put([]byte(metaNextKey), encodeUint64(val))
}

// Add inserts a new value into the circular list using the next auto-increment key.
func (cl *CircularList) Add(value []byte) {
	cl.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		next, err := getMetaNext(b)
		if err != nil {
			return err
		}
		key := dataKey(int(next))
		if err := b.Put(key, value); err != nil {
			return err
		}
		return setMetaNext(b, next+1)
	})
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

// exists scans the bucket for an item with an identical value.
func (cl *CircularList) exists(value []byte) bool {
	found := false
	cl.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		c := b.Cursor()
		prefix := []byte(dataPrefix)
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
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
	cl.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		keys, err := getOrderedDataKeys(b)
		if err != nil {
			return err
		}
		count := len(keys)
		if count == 0 {
			return nil
		}
		cur, err := getMetaCurrent(b)
		if err != nil {
			return err
		}
		index := int(cur)
		if index >= count {
			index = 0
		}
		// Delete the current item.
		if err := b.Delete(keys[index]); err != nil {
			return err
		}
		newCount := count - 1
		if newCount == 0 {
			return setMetaCurrent(b, 0)
		}
		if index >= newCount {
			index = 0
		}
		return setMetaCurrent(b, uint64(index))
	})
}

// Current returns the current element's value, its 0-based index, and the total count.
func (cl *CircularList) Current() ([]byte, int, int) {
	var value []byte
	var index, count int
	cl.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		keys, err := getOrderedDataKeys(b)
		if err != nil {
			return err
		}
		count = len(keys)
		if count == 0 {
			return nil
		}
		cur, err := getMetaCurrent(b)
		if err != nil {
			return err
		}
		index = int(cur)
		if index >= count {
			index = 0
		}
		v := b.Get(keys[index])
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value, index, count
}

// Next advances the pointer (wrapping around) and returns the next element's value.
func (cl *CircularList) Next() []byte {
	var value []byte
	cl.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		keys, err := getOrderedDataKeys(b)
		if err != nil {
			return err
		}
		count := len(keys)
		if count == 0 {
			return nil
		}
		cur, err := getMetaCurrent(b)
		if err != nil {
			return err
		}
		index := int(cur)
		index = (index + 1) % count
		if err := setMetaCurrent(b, uint64(index)); err != nil {
			return err
		}
		v := b.Get(keys[index])
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value
}

// Previous moves the pointer backward (wrapping around) and returns the previous element's value.
func (cl *CircularList) Previous() []byte {
	var value []byte
	cl.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cl.BucketName)
		keys, err := getOrderedDataKeys(b)
		if err != nil {
			return err
		}
		count := len(keys)
		if count == 0 {
			return nil
		}
		cur, err := getMetaCurrent(b)
		if err != nil {
			return err
		}
		index := int(cur)
		index = (index - 1 + count) % count
		if err := setMetaCurrent(b, uint64(index)); err != nil {
			return err
		}
		v := b.Get(keys[index])
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value
}
