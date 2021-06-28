package block

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
)

// ErrInvalidVersion is the error returned when a version is invalid.
var ErrInvalidVersion = errors.New("roothash: invalid version")

// HeaderType is the type of header.
type HeaderType uint8

// Custom time stamp type that encodes like time.Time when marshaling to JSON.
type TimestampType uint64

func (tst TimestampType) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Unix(int64(tst), 0))
}

const (
	// Invalid is an invalid header type and should never be stored.
	Invalid HeaderType = 0

	// Normal is a normal header.
	Normal HeaderType = 1

	// RoundFailed is a header resulting from a failed round. Such a
	// header contains no transactions but advances the round as normal
	// to prevent replays of old commitments.
	RoundFailed HeaderType = 2

	// EpochTransition is a header resulting from an epoch transition.
	//
	// Such a header contains no transactions but advances the round as
	// normal.
	// TODO: Consider renaming this to CommitteeTransition.
	EpochTransition HeaderType = 3

	// Suspended is a header resulting from the runtime being suspended.
	//
	// Such a header contains no transactions but advances the round as
	// normal.
	Suspended HeaderType = 4
)

// Header is a block header.
//
// Keep this in sync with /runtime/src/common/roothash.rs.
type Header struct { // nolint: maligned
	// Version is the protocol version number.
	Version uint16 `json:"version"`

	// Namespace is the header's chain namespace.
	Namespace common.Namespace `json:"namespace"`

	// Round is the block round.
	Round uint64 `json:"round"`

	// Timestamp is the block timestamp (POSIX time).
	Timestamp TimestampType `json:"timestamp"`

	// HeaderType is the header type.
	HeaderType HeaderType `json:"header_type"`

	// PreviousHash is the previous block hash.
	PreviousHash hash.Hash `json:"previous_hash"`

	// IORoot is the I/O merkle root.
	IORoot hash.Hash `json:"io_root"`

	// StateRoot is the state merkle root.
	StateRoot hash.Hash `json:"state_root"`

	// MessagesHash is the hash of emitted runtime messages.
	MessagesHash hash.Hash `json:"messages_hash"`

	// StorageSignatures are the storage receipt signatures for the merkle
	// roots.
	StorageSignatures []signature.Signature `json:"storage_signatures"`
}

// IsParentOf returns true iff the header is the parent of a child header.
func (h *Header) IsParentOf(child *Header) bool {
	childHash := child.EncodedHash()
	return h.PreviousHash.Equal(&childHash)
}

// MostlyEqual compares vs another header for equality, omitting the
// StorageSignatures field as it is not universally guaranteed to be present.
//
// Locations where this matter should do the comparison manually.
func (h *Header) MostlyEqual(cmp *Header) bool {
	a, b := *h, *cmp
	a.StorageSignatures, b.StorageSignatures = []signature.Signature{}, []signature.Signature{}
	aHash, bHash := a.EncodedHash(), b.EncodedHash()
	return aHash.Equal(&bHash)
}

// EncodedHash returns the encoded cryptographic hash of the header.
func (h *Header) EncodedHash() hash.Hash {
	return hash.NewFrom(h)
}

// StorageRoots returns the storage roots contained in this header.
func (h *Header) StorageRoots() []storage.Root {
	return []storage.Root{
		{
			Namespace: h.Namespace,
			Version:   h.Round,
			Type:      storage.RootTypeIO,
			Hash:      h.IORoot,
		},
		{
			Namespace: h.Namespace,
			Version:   h.Round,
			Type:      storage.RootTypeState,
			Hash:      h.StateRoot,
		},
	}
}

// RootsForStorageReceipt gets the merkle roots that must be part of
// a storage receipt.
func (h *Header) RootsForStorageReceipt() []hash.Hash {
	return []hash.Hash{
		h.IORoot,
		h.StateRoot,
	}
}

// RootTypesForStorageReceipt gets the storage root type sequence for the roots
// returned by RootsForStorageReceipt.
func (h *Header) RootTypesForStorageReceipt() []storage.RootType {
	// NOTE: Keep these in the same order as in RootsForStorageReceipt above!
	return []storage.RootType{
		storage.RootTypeIO,
		storage.RootTypeState,
	}
}

// VerifyStorageReceiptSignatures validates that the storage receipt signatures
// match the signatures for the current merkle roots.
//
// Note: Ensuring that the signatures are signed by keypair(s) that are
// expected is the responsibility of the caller.
func (h *Header) VerifyStorageReceiptSignatures() error {
	receiptBody := storage.ReceiptBody{
		Version:   1,
		Namespace: h.Namespace,
		Round:     h.Round,
		RootTypes: h.RootTypesForStorageReceipt(),
		Roots:     h.RootsForStorageReceipt(),
	}

	if !signature.VerifyManyToOne(storage.ReceiptSignatureContext, cbor.Marshal(receiptBody), h.StorageSignatures) {
		return signature.ErrVerifyFailed
	}

	return nil
}

// VerifyStorageReceipt validates that the provided storage receipt
// matches the header.
func (h *Header) VerifyStorageReceipt(receipt *storage.ReceiptBody) error {
	if !receipt.Namespace.Equal(&h.Namespace) {
		return errors.New("roothash: receipt has unexpected namespace")
	}

	if receipt.Round != h.Round {
		return errors.New("roothash: receipt has unexpected round")
	}

	roots := h.RootsForStorageReceipt()
	types := h.RootTypesForStorageReceipt()
	if len(receipt.Roots) != len(roots) {
		return errors.New("roothash: receipt has unexpected number of roots")
	}
	if len(receipt.RootTypes) != len(types) {
		return errors.New("roothash: receipt has unexpected number of root types")
	}

	for idx, v := range roots {
		if types[idx] != receipt.RootTypes[idx] {
			return errors.New("roothash: receipt has unexpected root types")
		}
		if !bytes.Equal(v[:], receipt.Roots[idx][:]) {
			return errors.New("roothash: receipt has unexpected roots")
		}
	}

	return nil
}
