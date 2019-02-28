package block

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"

	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/crypto/hash"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	storage "github.com/oasislabs/ekiden/go/storage/api"

	pbRoothash "github.com/oasislabs/ekiden/go/grpc/roothash"
)

const (
	// NamespaceSize is the size of a chain namespace identifier in bytes.
	NamespaceSize = 32

	// RoundSize is the size of a round in bytes.
	RoundSize = 32
)

var (
	// ErrInvalidVersion is the error returned when a version is invalid.
	ErrInvalidVersion = errors.New("roothash: invalid version")

	// ErrInvalidRound is the error returned when a round is invalid
	// (out of range).
	ErrInvalidRound = errors.New("roothash: invalid round")

	// ErrMalformedNamespace is the error returned when a namespace
	// identifier is malformed.
	ErrMalformedNamespace = errors.New("roothash: malformed namespace")

	// ErrMalformedRound is the error returned when a round is invalid.
	ErrMalformedRound = errors.New("roothash: malformed round")

	_ encoding.BinaryMarshaler   = (*Namespace)(nil)
	_ encoding.BinaryUnmarshaler = (*Namespace)(nil)
	_ encoding.BinaryMarshaler   = (*Round)(nil)
	_ encoding.BinaryUnmarshaler = (*Round)(nil)
	_ cbor.Marshaler             = (*Header)(nil)
	_ cbor.Unmarshaler           = (*Header)(nil)
)

// Namespace is a chain namespace identifier.
type Namespace [NamespaceSize]byte

// MarshalBinary encodes a namespace identifier into binary form.
func (n *Namespace) MarshalBinary() (data []byte, err error) {
	data = append([]byte{}, n[:]...)
	return
}

// UnmarshalBinary decodes a binary marshaled namespace identifier.
func (n *Namespace) UnmarshalBinary(data []byte) error {
	if len(data) != NamespaceSize {
		return ErrMalformedNamespace
	}

	copy(n[:], data)

	return nil
}

// String returns the string representation of a chain namespace identifier.
func (n *Namespace) String() string {
	return hex.EncodeToString(n[:])
}

// Round is a round number.
//
// While this is encoded as a unsigned 256 bit big endian integer
// on the wire, it is a 64 bit unsigned integer in reality.
type Round [RoundSize]byte

// ToU64 returns the 64 bit unsigned representation of a round.
func (r *Round) ToU64() (uint64, error) {
	// Ensure the value fits in 64 bits.
	if !memIsZero(r[:RoundSize-8]) {
		return 0, ErrInvalidRound
	}

	return binary.BigEndian.Uint64(r[RoundSize-8:]), nil
}

// FromU64 sets a round from a uint64.
func (r *Round) FromU64(x uint64) {
	for i := 0; i < RoundSize-8; i++ {
		r[i] = 0
	}

	binary.BigEndian.PutUint64(r[RoundSize-8:], x)
}

// Increment returns the round incremented by 1.
func (r *Round) Increment() Round {
	v, err := r.ToU64()
	if err != nil {
		panic(err)
	}

	var ret Round
	ret.FromU64(v + 1)
	return ret
}

// MarshalBinary encodes a round into binary form.
func (r *Round) MarshalBinary() (data []byte, err error) {
	data = append([]byte{}, bytes.TrimLeft(r[:], "\x00")...)
	return
}

// UnmarshalBinary decodes a binary marshaled round.
func (r *Round) UnmarshalBinary(data []byte) error {
	if len(data) > 0 && data[0] == 0 {
		// Leading empty bytes should be stripped.
		return ErrMalformedRound
	} else if len(data) > RoundSize {
		return ErrMalformedRound
	}

	for i := range r {
		r[i] = 0
	}
	copy(r[RoundSize-len(data):], data)

	_, err := r.ToU64()
	return err
}

// String returns the string representation of a round.
func (r *Round) String() string {
	v, err := r.ToU64()
	if err != nil {
		return "[invalid]"
	}

	return strconv.FormatUint(v, 10)
}

// HeaderType is the type of header.
type HeaderType uint8

const (
	// Normal is a normal header.
	Normal HeaderType = 0

	// RoundFailed is a header resulting from a failed round. Such a
	// header contains no transactions but advances the round as normal
	// to prevent replays of old commitments.
	RoundFailed HeaderType = 1

	// EpochTransition is a header resulting from an epoch transition.
	// Such a header contains no transactions but advances the round as
	// normal.
	EpochTransition HeaderType = 2
)

// Header is a block header.
type Header struct { // nolint: maligned
	// Version is the protocol version number.
	Version uint16 `codec:"version"`

	// Namespace is the header's chain namespace.
	Namespace Namespace `codec:"namespace"`

	// Round is the block round.
	Round Round `codec:"round"`

	// Timestamp is the block timestamp (POSIX time).
	Timestamp uint64 `codec:"timestamp"`

	// HeaderType is the header type.
	HeaderType HeaderType `codec:"header_type"`

	// PreviousHash is the previous block hash.
	PreviousHash hash.Hash `codec:"previous_hash"`

	// GroupHash is the computation group hash.
	GroupHash hash.Hash `codec:"group_hash"`

	// InputHash is the input hash.
	InputHash hash.Hash `codec:"input_hash"`

	// OutputHash is the output hash.
	OutputHash hash.Hash `codec:"output_hash"`

	// StateRoot is the state root hash.
	StateRoot hash.Hash `codec:"state_root"`

	// CommitmentsHash is the Commitments hash.
	CommitmentsHash hash.Hash `codec:"commitments_hash"`

	// StorageReceipt is the storage receipt for the hashes.
	StorageReceipt signature.Signature `codec:"storage_receipt"`
}

// IsParentOf returns true iff the header is the parent of a child header.
func (h *Header) IsParentOf(child *Header) bool {
	childHash := child.EncodedHash()
	return h.PreviousHash.Equal(&childHash)
}

// MostlyEqual compares vs another header for equality, omitting the
// StorageReceipt field as it is not universally guaranteed to be present.
//
// Locations where this matter should do the comparison manually.
func (h *Header) MostlyEqual(cmp *Header) bool {
	a, b := *h, *cmp
	a.StorageReceipt, b.StorageReceipt = signature.Signature{}, signature.Signature{}
	aHash, bHash := a.EncodedHash(), b.EncodedHash()
	return aHash.Equal(&bHash)
}

// FromProto deserializes a protobuf into a header.
func (h *Header) FromProto(pb *pbRoothash.Header) error { // nolint: gocyclo
	if pb == nil {
		return errNilProtobuf
	}

	// Version (range check)
	ver := pb.GetVersion()
	if ver > math.MaxUint16 {
		return ErrInvalidVersion
	}
	h.Version = uint16(ver)

	if err := h.Namespace.UnmarshalBinary(pb.GetNamespace()); err != nil {
		return err
	}
	if err := h.Round.UnmarshalBinary(pb.GetRound()); err != nil {
		return err
	}
	h.Timestamp = pb.GetTimestamp()
	h.HeaderType = HeaderType(pb.GetHeaderType())
	if err := h.PreviousHash.UnmarshalBinary(pb.GetPreviousHash()); err != nil {
		return err
	}
	if err := h.GroupHash.UnmarshalBinary(pb.GetGroupHash()); err != nil {
		return err
	}
	if err := h.InputHash.UnmarshalBinary(pb.GetInputHash()); err != nil {
		return err
	}
	if err := h.OutputHash.UnmarshalBinary(pb.GetOutputHash()); err != nil {
		return err
	}
	if err := h.StateRoot.UnmarshalBinary(pb.GetStateRoot()); err != nil {
		return err
	}
	if err := h.CommitmentsHash.UnmarshalBinary(pb.GetCommitmentsHash()); err != nil {
		return err
	}
	if sr := pb.GetStorageReceipt(); sr != nil {
		if err := cbor.Unmarshal(sr, &h.StorageReceipt); err != nil {
			return err
		}
	}

	return nil
}

// ToProto serializes a header into a protobuf.
func (h *Header) ToProto() *pbRoothash.Header {
	pb := new(pbRoothash.Header)

	pb.Version = uint32(h.Version)
	pb.Namespace, _ = h.Namespace.MarshalBinary()
	pb.Round, _ = h.Round.MarshalBinary()
	pb.Timestamp = h.Timestamp
	pb.HeaderType = uint32(h.HeaderType)
	pb.PreviousHash, _ = h.PreviousHash.MarshalBinary()
	pb.GroupHash, _ = h.GroupHash.MarshalBinary()
	pb.InputHash, _ = h.InputHash.MarshalBinary()
	pb.OutputHash, _ = h.OutputHash.MarshalBinary()
	pb.StateRoot, _ = h.StateRoot.MarshalBinary()
	pb.CommitmentsHash, _ = h.CommitmentsHash.MarshalBinary()
	pb.StorageReceipt = cbor.Marshal(&h.StorageReceipt)

	return pb
}

// MarshalCBOR serializes the type into a CBOR byte vector.
func (h *Header) MarshalCBOR() []byte {
	return cbor.Marshal(h)
}

// UnmarshalCBOR decodes a CBOR marshaled header.
func (h *Header) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, h)
}

// EncodedHash returns the encoded cryptographic hash of the header.
func (h *Header) EncodedHash() hash.Hash {
	var hh hash.Hash

	hh.From(h)

	return hh
}

// KeysForStorageReceipt gets the storage keys required to request a
// storage receipt.
func (h *Header) KeysForStorageReceipt() []storage.Key {
	keys := make([]storage.Key, 0, 3)

	for _, h := range []hash.Hash{
		h.InputHash,
		h.OutputHash,
		h.StateRoot,
	} {
		if h.IsEmpty() {
			continue
		}
		var key storage.Key
		copy(key[:], h[:])
		keys = append(keys, key)
	}

	return keys
}

// VerifyStorageReceiptSignature validates that the storage receipt
// signature matches the hashes.
//
// Note: Ensuring that the signature is signed by the keypair that is
// expected is the responsibility of the caller.
func (h *Header) VerifyStorageReceiptSignature() error {
	receipt := storage.Receipt{
		Keys: h.KeysForStorageReceipt(),
	}

	signed := signature.Signed{
		Blob:      receipt.MarshalCBOR(),
		Signature: h.StorageReceipt,
	}

	var check storage.SignedReceipt
	return signed.Open(storage.ReceiptSignatureContext, &check)
}

// VerifyStorageReceipt validates that the provided storage receipt
// matches the header.
func (h *Header) VerifyStorageReceipt(receipt *storage.Receipt) error {
	keys := h.KeysForStorageReceipt()
	if len(receipt.Keys) != len(keys) {
		return errors.New("roothash: receipt has unexpected number of keys")
	}

	for idx, v := range keys {
		if !bytes.Equal(v[:], receipt.Keys[idx][:]) {
			return errors.New("roothash: receipt has unexpected keys")
		}
	}

	return nil
}

func memIsZero(b []byte) bool {
	var acc byte

	for _, v := range b {
		acc |= v
	}

	return acc == 0
}
