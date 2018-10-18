// Package runtime implements common runtime routines.
package api

import (
	"encoding/hex"
	"errors"

	"github.com/oasislabs/ekiden/go/common"
	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	pbRegistry "github.com/oasislabs/ekiden/go/grpc/registry"
)

var (
	// ErrMalformedStoreID is the error returned when a storage service
	// ID is malformed.
	ErrMalformedStoreID = errors.New("runtime: Malformed store ID")

	// ErrNilProtobuf is the error returned when a protobuf is nil.
	ErrNilProtobuf = errors.New("node: Protobuf is nil")

	_ cbor.Marshaler   = (*Runtime)(nil)
	_ cbor.Unmarshaler = (*Runtime)(nil)
)

// StoreID is a storage service ID.
// TODO: Move this to the storage package when it exists.
type StoreID [32]byte

// MarshalBinary encodes a StoreID into binary form.
func (id *StoreID) MarshalBinary() (data []byte, err error) {
	data = append([]byte{}, id[:]...)
	return
}

// UnmarshalBinary decodes a binary marshaled StoreID.
func (id *StoreID) UnmarshalBinary(data []byte) error {
	const idSize = 32

	if len(data) != idSize {
		return ErrMalformedStoreID
	}

	copy(id[:], data)
	return nil
}

// String returns a string representation of a StoreID.
func (id *StoreID) String() string {
	return hex.EncodeToString(id[:])
}

// Runtime represents a runtime.
type Runtime struct {
	// ID is a globally unique long term identifier of the runtime.
	ID signature.PublicKey `codec:"id"`

	// StoreID is the storage service ID associated with the runtime.
	StoreID StoreID `codec:"store_id"`

	// Code is the runtime code body.
	Code []byte `codec:"code"`

	// MinimumBond is the mimimum stake required by the runtime.
	MinimumBond uint64 `codec:"minimum_bond"`

	// ModeNonDeterministic indicates if the runtime should be executed
	// in a non-deterministic manner.
	ModeNonDeterministic bool `codec:"mode_nondeterministic"`

	// FeaturesSGX indicates if the runtime requires SGX.
	FeaturesSGX bool `codec:"features_sgx"`

	// AdvertisementRate is the number of tokens/second of runtime
	// instance advertisement.
	AdvertisementRate uint64 `codec:"advertisement_rate"`

	// ReplicaGroupSize is the size of the computation group.
	ReplicaGroupSize uint64 `codec:"replica_group_size"`

	// ReplicaGroupBackupSize is the size of the discrepancy resolution
	// replica group.
	ReplicaGroupBackupSize uint64 `codec:"replica_group_backup_size"`

	// ReplicaAllowedStragglers is the number of allowed stragglers.
	ReplicaAllowedStragglers uint64 `codec:"replica_allowed_stragglers"`

	// StorageGroupSize is the size of the storage group.
	StorageGroupSize uint64 `codec:"storage_group_size"`
}

// String returns a string representation of itself.
func (c *Runtime) String() string {
	return "<Runtime id=" + c.ID.String() + ">"
}

// Clone returns a copy of itself.
func (c *Runtime) Clone() common.Cloneable {
	runtimeCopy := *c
	return &runtimeCopy
}

// FromProto deserializes a protobuf into a Runtime.
func (c *Runtime) FromProto(pb *pbRegistry.Runtime) error {
	if pb == nil {
		return ErrNilProtobuf
	}

	if c.ID == nil {
		c.ID = signature.PublicKey{}
	}

	if err := c.ID.UnmarshalBinary(pb.GetId()); err != nil {
		return err
	}

	if err := c.StoreID.UnmarshalBinary(pb.GetStoreId()); err != nil {
		return err
	}

	c.Code = append([]byte{}, pb.GetCode()...)
	c.MinimumBond = pb.GetMinimumBond()
	c.AdvertisementRate = pb.GetAdvertisementRate()
	c.ReplicaGroupSize = pb.GetReplicaGroupSize()
	c.ReplicaGroupBackupSize = pb.GetReplicaGroupBackupSize()
	c.ReplicaAllowedStragglers = pb.GetReplicaAllowedStragglers()
	c.StorageGroupSize = pb.GetStorageGroupSize()

	switch pb.GetMode() {
	case pbRegistry.Runtime_Nondeterministic:
		c.ModeNonDeterministic = true
	default:
		c.ModeNonDeterministic = false
	}

	c.FeaturesSGX = pbWantsSGX(pb)

	return nil
}

// ToProto serializes a Runtime into a protobuf.
func (c *Runtime) ToProto() *pbRegistry.Runtime {
	pb := new(pbRegistry.Runtime)
	var err error

	if pb.Id, err = c.ID.MarshalBinary(); err != nil {
		return nil
	}
	if pb.StoreId, err = c.StoreID.MarshalBinary(); err != nil {
		return nil
	}
	pb.Code = append([]byte{}, c.Code...)
	pb.MinimumBond = c.MinimumBond
	pb.AdvertisementRate = c.AdvertisementRate
	pb.ReplicaGroupSize = c.ReplicaGroupSize
	pb.ReplicaGroupBackupSize = c.ReplicaGroupBackupSize
	pb.ReplicaAllowedStragglers = c.ReplicaAllowedStragglers
	pb.StorageGroupSize = c.StorageGroupSize

	switch c.ModeNonDeterministic {
	case true:
		pb.Mode = pbRegistry.Runtime_Nondeterministic
	case false:
		pb.Mode = pbRegistry.Runtime_Deterministic
	}

	if c.FeaturesSGX {
		pb.Features = append([]pbRegistry.Runtime_Features{}, pbRegistry.Runtime_SGX)
	}

	return pb
}

// ToSignable serializes the Runtime into a signature compatible byte vector.
func (c *Runtime) ToSignable() []byte {
	return c.MarshalCBOR()
}

// MarshalCBOR serializes the type into a CBOR byte vector.
func (c *Runtime) MarshalCBOR() []byte {
	return cbor.Marshal(c)
}

// UnmarshalCBOR deserializes a CBOR byte vector into given type.
func (c *Runtime) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, c)
}

func pbWantsSGX(pb *pbRegistry.Runtime) bool {
	for _, f := range pb.GetFeatures() {
		if f == pbRegistry.Runtime_SGX {
			return true
		}
	}

	return false
}

// SignedRuntime is a signed blob containing a CBOR-serialized Runtime.
type SignedRuntime struct {
	signature.Signed
}

// Open first verifies the blob signature and then unmarshals the blob.
func (s *SignedRuntime) Open(context []byte, runtime *Runtime) error { // nolint: interfacer
	return s.Signed.Open(context, runtime)
}
