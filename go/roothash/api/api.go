// Package api implements the root hash backend API and common datastructures.
package api

import (
	"encoding"
	"encoding/hex"
	"errors"

	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/crypto/hash"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/pubsub"

	pbRoothash "github.com/oasislabs/ekiden/go/grpc/roothash"
)

const (
	// HashSize is the size of the various hashes in bytes.
	HashSize = 32
)

var (
	// ErrNilProtobuf is the error returned when a protobuf is nil.
	ErrNilProtobuf = errors.New("roothash: protobuf is nil")

	// ErrMalformedHash is the error returned when a hash is malformed.
	ErrMalformedHash = errors.New("roothash: malformed hash")

	_ encoding.BinaryMarshaler   = (*Commitment)(nil)
	_ encoding.BinaryUnmarshaler = (*Commitment)(nil)
)

// Commitment is a backend specific commitment from a compute node.
type Commitment struct {
	// Data is the opaque commitment.
	Data []byte
}

// MarshalBinary encodes a commitment into binary form.
func (c *Commitment) MarshalBinary() (data []byte, err error) {
	data = append([]byte{}, c.Data...)
	return
}

// UnmarshalBinary decodes a binary marshaled commitment.
func (c *Commitment) UnmarshalBinary(data []byte) error {
	c.Data = append([]byte{}, data...)

	return nil
}

// FromProto deserializes a protobuf into a commitment.
func (c *Commitment) FromProto(pb *pbRoothash.Commitment) error {
	if pb == nil {
		return ErrNilProtobuf
	}

	return c.UnmarshalBinary(pb.GetData())
}

// ToProto serializes a commitment into a protobuf.
func (c *Commitment) ToProto() *pbRoothash.Commitment {
	pb := new(pbRoothash.Commitment)

	pb.Data, _ = c.MarshalBinary()

	return pb
}

// String returns a string representation of the commitment.
func (c *Commitment) String() string {
	return hex.EncodeToString(c.Data)
}

// Backend is a root hash consensus implementation.
type Backend interface {
	// GetLatestBlock returns the latest block.
	//
	// The metadata contained in this block can be further used to get
	// the latest state from the storage backend.
	GetLatestBlock(context.Context, signature.PublicKey) (*Block, error)

	// WatchBlocks returns a channel that produces a stream of blocks.
	//
	// The latest block if any will get pushed to the stream immediately.
	// Subsequent blocks will be pushed into the stream as they are
	// confirmed.
	WatchBlocks(signature.PublicKey) (<-chan *Block, *pubsub.Subscription, error)

	// WatchBlocksSince returns a channel that produces a stream of blocks
	// starting at the specified round.
	//
	// The block at the specified round is included as the first
	// entry in the stream.  Following blocks are pushed in order as
	// they are confirmed.
	WatchBlocksSince(signature.PublicKey, Round) (<-chan *Block, *pubsub.Subscription, error)

	// WatchEvents returns a stream of protocol events.
	WatchEvents(signature.PublicKey) (<-chan *Event, *pubsub.Subscription, error)

	// Commit commits to a result of processing a batch of contract invocations.
	Commit(context.Context, signature.PublicKey, *Commitment) error
}

// Event is a protocol event.
type Event struct {
	// DiscrepancyDetected is the CallBatch hash that is set when a
	// discrepancy is detected to signal to the backup workers that a
	// computation should be re-executed.
	DiscrepancyDetected *hash.Hash

	// RoundFailed is the error that is set when a round fails.
	RoundFailed error
}
