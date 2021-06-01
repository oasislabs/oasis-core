package history

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"

	"github.com/oasisprotocol/oasis-core/go/common"
	cmnBadger "github.com/oasisprotocol/oasis-core/go/common/badger"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/keyformat"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

const dbVersion = 1

var (
	// metadataKeyFmt is the metadata key format.
	//
	// Value is CBOR-serialized dbMetadata.
	metadataKeyFmt = keyformat.New(0x01)
	// blockKeyFmt is the block index key format.
	//
	// Value is CBOR-serialized roothash.AnnotatedBlock.
	blockKeyFmt = keyformat.New(0x02, uint64(0))
	// roundResultsKeyFmt is the round result index key format.
	//
	// Value is CBOR-serialized roothash.RoundResults.
	roundResultsKeyFmt = keyformat.New(0x03, uint64(0))
	// stakingEventsKeyFmt is the round staking events index key format.
	//
	// Value is CBOR-serialized list of staking.Events.
	stakingEventsKeyFmt = keyformat.New(0x04, uint64(0))
	// pendingStakingEventsKeyFmt is the per height pending staking events index key format.
	//
	// Value is a CBOR-serialized list of staking.Events.
	pendingStakingEventsKeyFmt = keyformat.New(0x05, int64(0))
)

type dbMetadata struct {
	// RuntimeID is the runtime ID this database is for.
	RuntimeID common.Namespace `json:"runtime_id"`
	// Version is the database schema version.
	Version uint64 `json:"version"`

	// LastConsensusHeight is the last consensus height.
	LastConsensusHeight int64 `json:"last_consensus_height"`
	// LastRound is the last round.
	LastRound uint64 `json:"last_round"`
}

// DB is the history database.
type DB struct {
	logger *logging.Logger

	db *badger.DB
	gc *cmnBadger.GCWorker
}

func newDB(fn string, runtimeID common.Namespace) (*DB, error) {
	logger := logging.GetLogger("runtime/history").With("path", fn)

	opts := badger.DefaultOptions(fn)
	opts = opts.WithLogger(cmnBadger.NewLogAdapter(logger))
	opts = opts.WithSyncWrites(true)
	opts = opts.WithCompression(options.None)

	db, err := cmnBadger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("runtime/history: failed to open database: %w", err)
	}

	d := &DB{
		logger: logger,
		db:     db,
		gc:     cmnBadger.NewGCWorker(logger, db),
	}

	// Ensure metadata is valid.
	if err = d.ensureMetadata(runtimeID); err != nil {
		d.close()
		return nil, err
	}

	return d, nil
}

func (d *DB) queryGetMetadata(tx *badger.Txn) (*dbMetadata, error) {
	item, err := tx.Get(metadataKeyFmt.Encode())
	if err != nil {
		return nil, err
	}

	var meta dbMetadata
	err = item.Value(func(val []byte) error {
		return cbor.Unmarshal(val, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (d *DB) ensureMetadata(runtimeID common.Namespace) error {
	return d.db.Update(func(tx *badger.Txn) error {
		meta, err := d.queryGetMetadata(tx)
		switch err {
		case nil:
		case badger.ErrKeyNotFound:
			// Create new metadata section.
			meta := dbMetadata{
				RuntimeID: runtimeID,
				Version:   dbVersion,
			}
			return tx.Set(metadataKeyFmt.Encode(), cbor.Marshal(meta))
		default:
			return err
		}

		// Verify metadata section.
		if meta.Version != dbVersion {
			return fmt.Errorf("runtime/history: unsupported database version (expected: %d got: %d)",
				dbVersion,
				meta.Version,
			)
		}

		if !meta.RuntimeID.Equal(&runtimeID) {
			return fmt.Errorf("runtime/history: database for different runtime (expected: %s got: %s)",
				runtimeID,
				meta.RuntimeID,
			)
		}
		return nil
	})
}

func (d *DB) metadata() (*dbMetadata, error) {
	var meta *dbMetadata
	err := d.db.View(func(tx *badger.Txn) error {
		var err error
		meta, err = d.queryGetMetadata(tx)
		return err
	})
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (d *DB) consensusCheckpoint(height int64) error {
	return d.db.Update(func(tx *badger.Txn) error {
		meta, err := d.queryGetMetadata(tx)
		if err != nil {
			return err
		}

		if height < meta.LastConsensusHeight {
			return fmt.Errorf("runtime/history: consensus checkpoint at lower height (current: %d wanted: %d)",
				meta.LastConsensusHeight,
				height,
			)
		}

		meta.LastConsensusHeight = height
		return tx.Set(metadataKeyFmt.Encode(), cbor.Marshal(meta))
	})
}

func (d *DB) commit(blk *roothash.AnnotatedBlock, roundResults *roothash.RoundResults) error {
	return d.db.Update(func(tx *badger.Txn) error {
		meta, err := d.queryGetMetadata(tx)
		if err != nil {
			return err
		}

		rtID := blk.Block.Header.Namespace
		if !rtID.Equal(&meta.RuntimeID) {
			return fmt.Errorf("runtime/history: runtime mismatch (expected: %s got: %s)",
				meta.RuntimeID,
				rtID,
			)
		}

		if blk.Height <= meta.LastConsensusHeight {
			return fmt.Errorf("runtime/history: commit at lower consensus height (current: %d wanted: %d)",
				meta.LastConsensusHeight,
				blk.Height,
			)
		}

		if blk.Block.Header.Round <= meta.LastRound && blk.Block.Header.Round != 0 {
			return fmt.Errorf("runtime/history: commit at lower round (current: %d wanted: %d)",
				meta.LastRound,
				blk.Block.Header.Round,
			)
		}

		if err = tx.Set(blockKeyFmt.Encode(blk.Block.Header.Round), cbor.Marshal(blk)); err != nil {
			return err
		}

		if err = tx.Set(roundResultsKeyFmt.Encode(blk.Block.Header.Round), cbor.Marshal(roundResults)); err != nil {
			return err
		}

		// Load pending staking events.
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		var events []*staking.Event
		for it.Seek(pendingStakingEventsKeyFmt.Encode()); it.Valid(); it.Next() {
			// TODO: sanity check no height is greater than blk.Height and less than last committed round height.
			var evs []*staking.Event
			err = it.Item().Value(func(val []byte) error {
				return cbor.UnmarshalTrusted(val, &evs)
			})
			if err != nil {
				return err
			}
			events = append(events, evs...)
		}
		// Remove pending events.
		if err = tx.Delete(pendingStakingEventsKeyFmt.Encode()); err != nil {
			return err
		}

		if err = tx.Set(stakingEventsKeyFmt.Encode(blk.Block.Header.Round), cbor.Marshal(events)); err != nil {
			return err
		}

		meta.LastRound = blk.Block.Header.Round
		if blk.Height > meta.LastConsensusHeight {
			meta.LastConsensusHeight = blk.Height
		}

		return tx.Set(metadataKeyFmt.Encode(), cbor.Marshal(meta))
	})
}

func (d *DB) commitPendingConsensusEvents(height int64, stakingEvents []*staking.Event) error {
	return d.db.Update(func(tx *badger.Txn) error {
		meta, err := d.queryGetMetadata(tx)
		if err != nil {
			return err
		}

		if height <= meta.LastConsensusHeight {
			return fmt.Errorf("runtime/history: commit pending consensus events at lower consensus height (current: %d wanted: %d)",
				meta.LastConsensusHeight,
				height,
			)
		}

		if err = tx.Set(pendingStakingEventsKeyFmt.Encode(height), cbor.Marshal(stakingEvents)); err != nil {
			return err
		}
		// Note: Doesn't commit consensus height.

		return nil
	})
}

func (d *DB) getBlock(round uint64) (*roothash.AnnotatedBlock, error) {
	var blk roothash.AnnotatedBlock
	txErr := d.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(blockKeyFmt.Encode(round))
		switch err {
		case nil:
		case badger.ErrKeyNotFound:
			return roothash.ErrNotFound
		default:
			return err
		}

		return item.Value(func(val []byte) error {
			return cbor.UnmarshalTrusted(val, &blk)
		})
	})
	if txErr != nil {
		return nil, txErr
	}
	return &blk, nil
}

func (d *DB) getRoundResults(round uint64) (*roothash.RoundResults, error) {
	var roundResults *roothash.RoundResults
	txErr := d.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(roundResultsKeyFmt.Encode(round))
		switch err {
		case nil:
		case badger.ErrKeyNotFound:
			return roothash.ErrNotFound
		default:
			return err
		}

		return item.Value(func(val []byte) error {
			return cbor.UnmarshalTrusted(val, &roundResults)
		})
	})
	if txErr != nil {
		return nil, txErr
	}
	return roundResults, nil
}

func (d *DB) getStakingEvents(round uint64) ([]*staking.Event, error) {
	var stakingEvents []*staking.Event
	txErr := d.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(stakingEventsKeyFmt.Encode(round))
		switch err {
		case nil:
		case badger.ErrKeyNotFound:
			return roothash.ErrNotFound
		default:
			return err
		}

		return item.Value(func(val []byte) error {
			return cbor.UnmarshalTrusted(val, &stakingEvents)
		})
	})
	if txErr != nil {
		return nil, txErr
	}
	return stakingEvents, nil
}

func (d *DB) close() {
	d.gc.Close()
	d.db.Close()
}
