// Package api implements the storage backend API.
package api

import (
	"crypto/sha512"
	"encoding/hex"
	"errors"

	"golang.org/x/net/context"

	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
)

// KeySize is the size of a storage key in bytes.
const KeySize = 32

var (
	// ErrKeyNotFound is the error returned when the requested key
	// is not present in storage.
	ErrKeyNotFound = errors.New("storage: key not found")

	// ErrKeyExpired is the error returned when the requested key
	// is expired.
	ErrKeyExpired = errors.New("storage: key expired")

	// ErrIncoherentTime is the error returned when the timekeeping
	// is not coherent.
	ErrIncoherentTime = errors.New("storage: incoherent time")
)

// Key is a storage key.
type Key [KeySize]byte

// String returns a string representation of a key.
func (k Key) String() string {
	return hex.EncodeToString(k[:])
}

// KeyInfo is a key and it's associated metadata in storage.
type KeyInfo struct {
	// Key is the key of the value.
	Key Key

	// Expiration is the expiration time of the key/value pair.
	Expiration epochtime.EpochTime
}

// Backend is a storage backend implementation.
type Backend interface {
	// Get returns the value for a specific immutable key.
	Get(context.Context, Key) ([]byte, error)

	// Insert inserts a specific value, which can later be retreived by
	// it's hash.  The expiration is the number of epochs for which the
	// value should remain available.
	Insert(context.Context, []byte, uint64) error

	// GetKeys returns all of the keys in the storage database, along
	// with their associated metadata.
	GetKeys(context.Context) ([]*KeyInfo, error)

	// Cleanup closes/cleans up the storage backend.
	Cleanup()
}

// HashStorageKey generates a storage key from it's value.
//
// All backends MUST use this method to hash values (generate keys).
func HashStorageKey(value []byte) Key {
	sum := sha512.Sum512_256(value)
	var k Key
	copy(k[:], sum[:])
	return k
}
