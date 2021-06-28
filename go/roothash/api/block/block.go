// Package block implements the roothash block and header.
package block

import "github.com/oasisprotocol/oasis-core/go/common"

// Block is an Oasis block.
//
// Keep this in sync with /runtime/src/common/roothash.rs.
type Block struct {
	// Header is the block header.
	Header Header `json:"header"`
}

// NewGenesisBlock creates a new empty genesis block given a runtime
// id and POSIX timestamp.
func NewGenesisBlock(id common.Namespace, timestamp uint64) *Block {
	var blk Block

	blk.Header.Version = 0
	blk.Header.Timestamp = TimestampType(timestamp)
	blk.Header.HeaderType = Normal
	blk.Header.Namespace = id
	blk.Header.PreviousHash.Empty()
	blk.Header.IORoot.Empty()
	blk.Header.StateRoot.Empty()
	blk.Header.MessagesHash.Empty()

	return &blk
}

// NewEmptyBlock creates a new empty block with a specific type.
func NewEmptyBlock(child *Block, timestamp uint64, htype HeaderType) *Block {
	var blk Block

	blk.Header.Version = child.Header.Version
	blk.Header.Namespace = child.Header.Namespace
	blk.Header.Round = child.Header.Round + 1
	blk.Header.Timestamp = TimestampType(timestamp)
	blk.Header.HeaderType = htype
	blk.Header.PreviousHash = child.Header.EncodedHash()
	blk.Header.IORoot.Empty()
	// State root is unchanged.
	blk.Header.StateRoot = child.Header.StateRoot
	blk.Header.MessagesHash.Empty()

	return &blk
}
