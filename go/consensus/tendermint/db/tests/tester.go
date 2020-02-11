// Package tests is a collection of tendermint DB backend tests.
package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	dbm "github.com/tendermint/tm-db"
)

// TestTendermintDB tests the provided tendermint database.
func TestTendermintDB(t *testing.T, db dbm.DB) {
	// Run the sub-tests.
	t.Run("BasicOps", func(t *testing.T) { testBasicOps(t, db) })
	t.Run("BatchOps", func(t *testing.T) { testBatchOps(t, db) })
	t.Run("Iterator", func(t *testing.T) { testIterator(t, db) })
	t.Run("Misc", func(t *testing.T) { testMisc(t, db) })
}

func testBasicOps(t *testing.T, db dbm.DB) {
	// Non-existent keys, don't exist.
	require.False(t, db.Has([]byte("non-existent")), "Has(non-existent)")
	require.Nil(t, db.Get([]byte("non-existent")), "Get(non-existent")

	// The nil key should work, as an empty byte slice.
	v := []byte("Can I has nil key?")
	db.Set(nil, v)
	require.True(t, db.Has(nil), "Has(nil)")
	require.True(t, db.Has([]byte{}), "Has([]byte{})")
	require.EqualValues(t, v, db.Get(nil), "Get(nil)")
	require.EqualValues(t, v, db.Get([]byte{}), "Get([]byte{})")
	db.Delete(nil)
	require.False(t, db.Has(nil), "Has(nil), post Delete()")

	// An actual key should also work.
	key, value := []byte("Yog-Sothoth"), []byte("is the key and guardian of the gate.")
	db.Set(key, value)
	require.True(t, db.Has(key), "Has()")
	require.EqualValues(t, value, db.Get(key), "Get()")
	db.Delete(key)
	require.False(t, db.Has(key), "Has(), post Delete()")

	// The sync equivalents to Set/Delete() should work.
	db.SetSync(key, value)
	require.True(t, db.Has(key), "Has() - SetSync()")
	require.EqualValues(t, value, db.Get(key), "Get() - SetSync()")
	db.DeleteSync(key)
	require.False(t, db.Has(key), "Has(), post DeleteSync()")
}

func testBatchOps(t *testing.T, db dbm.DB) {
	toDeleteKey := []byte("to-delete")
	db.Set(toDeleteKey, []byte("some value"))

	// Build and execute the batch.
	k1, k2 := []byte("key1"), []byte("key2")
	v1, v2 := []byte("value1"), []byte("value2")
	batch := db.NewBatch()
	batch.Set(k1, v1)
	batch.Set(k2, v2)
	batch.Delete(toDeleteKey)
	batch.Write()

	require.EqualValues(t, v1, db.Get(k1), "Get(key1)")
	require.EqualValues(t, v2, db.Get(k2), "Get(key2)")
	require.False(t, db.Has(toDeleteKey), "Has(deleted)")

	// Build and execute the clean-up batch.
	batch = db.NewBatch()
	batch.Delete(k1)
	batch.Delete(k2)
	batch.WriteSync()

	require.False(t, db.Has(k1), "Has(key1), post WriteSync")
	require.False(t, db.Has(k2), "Has(key2), post WriteSync")
}

func testIterator(t *testing.T, db dbm.DB) {
	// Note: Weird failures will happen if the database isn't empty
	// due to prior tests not running to completion.

	entries := []struct {
		key, value []byte
	}{
		{[]byte{}, []byte("nil")},
		{[]byte("a"), []byte("a")},
		{[]byte("ab"), []byte("ab")},
		{[]byte("ac"), []byte("ac")},
		{[]byte("b"), []byte("b")},
		{[]byte("c"), []byte("c")},
	}

	const (
		subStart = 1 // `a`
		subEnd   = 4 // `b`
	)

	// Populate the database.
	batch := db.NewBatch()
	for _, ent := range entries {
		batch.Set(ent.key, ent.value)
	}
	batch.Write()

	// Traverse forward (entire range).
	fwdIter := db.Iterator(nil, nil)
	for i, ent := range entries {
		require.True(t, fwdIter.Valid(), "Fwd[%d]: Valid()", i)
		require.EqualValues(t, ent.key, fwdIter.Key(), "Fwd[%d]: Key()", i)
		require.EqualValues(t, ent.value, fwdIter.Value(), "Fwd[%d]: Value()", i)
		fwdIter.Next()
	}
	require.False(t, fwdIter.Valid(), "Fwd[tail]: Valid()")

	// Ensure the accessors for an invalid iterator panic.
	require.Panics(t, func() { fwdIter.Key() }, "Key(), invalid iterator")
	require.Panics(t, func() { fwdIter.Value() }, "Value(), invalid iterator")
	require.Panics(t, func() { fwdIter.Value() }, "Next(), invalid iterator")

	fwdIter.Close()

	// Traverse forward (subset).
	fwdSubIter := db.Iterator([]byte("a"), []byte("b"))
	for i := subStart; i < subEnd; i++ {
		ent := entries[i]
		require.True(t, fwdSubIter.Valid(), "Fwd[%d]: Valid(), skip", i)
		require.EqualValues(t, ent.key, fwdSubIter.Key(), "Fwd[%d]: Key(), skip", i)
		require.EqualValues(t, ent.value, fwdSubIter.Value(), "Fwd[%d]: Value(), skip", i)
		fwdSubIter.Next()
	}
	require.False(t, fwdSubIter.Valid(), "Fwd[tail]: Valid(), skip")

	start, end := fwdSubIter.Domain() // Might as well do this here.
	require.EqualValues(t, []byte("a"), start, "Domain() start")
	require.EqualValues(t, []byte("b"), end, "Domain() end")
	fwdSubIter.Close()

	// Traverse backward (entire range).
	revIter := db.ReverseIterator(nil, nil)
	for i := len(entries) - 1; i >= 0; i-- {
		ent := entries[i]
		require.True(t, revIter.Valid(), "Rev[%d]: Valid()", i)
		require.EqualValues(t, ent.key, revIter.Key(), "Rev[%d]: Key()", i)
		require.EqualValues(t, ent.value, revIter.Value(), "Rev[%d]: Value()", i)
		revIter.Next()
	}
	require.False(t, revIter.Valid(), "Rev[tail]: Valid()")
	revIter.Close()

	// Traverse backward (subset).
	revSubIter := db.ReverseIterator([]byte("a"), []byte("b"))
	for i := subEnd - 1; i >= subStart; i-- { // End is exclusive (v0.27.0)
		ent := entries[i]
		require.True(t, revSubIter.Valid(), "Rev[%d]: Valid(), skip", i)
		require.EqualValues(t, ent.key, revSubIter.Key(), "Rev[%d]: Key(), skip", i)
		require.EqualValues(t, ent.value, revSubIter.Value(), "Rev[%d]: Value(), skip", i)
		revSubIter.Next()
	}
	require.False(t, revSubIter.Valid(), "Rev[tail]: Valid(), skip")

	// Traverse backward (subset, inexact end).
	revSubIEIter := db.ReverseIterator([]byte("a"), []byte("ad"))
	for i := subEnd - 1; i >= subStart; i-- { // End is exclusive (v0.27.0)
		ent := entries[i]
		require.True(t, revSubIEIter.Valid(), "RevSubIE[%d]: Valid(), skip", i)
		require.EqualValues(t, ent.key, revSubIEIter.Key(), "RevSubIE[%d]: Key(), skip", i)
		require.EqualValues(t, ent.value, revSubIEIter.Value(), "RevSubIE[%d]: Value(), skip", i)
		revSubIEIter.Next()
	}
	require.False(t, revSubIEIter.Valid(), "RevSubIE[tail]: Valid(), skip")

	// Deliberately leave revSubIter un-Close()ed, to test that the
	// Next() call that invalidated the iterator cleans everything up.
	//
	// Note: This is only possible with the BoltDB backend.
	stats := db.Stats()
	if stats["database.type"] == "BoltDB" {
		require.Equal(t, "0", stats["database.tx.read.open"], "Dangling transactions???")
	}
}

func testMisc(t *testing.T, db dbm.DB) {
	stats := db.Stats()
	t.Logf("DB Stats(): %+v", stats)

	db.Print() // Produces no output, though it does log at debug level.
}
