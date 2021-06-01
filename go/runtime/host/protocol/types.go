package protocol

import (
	"fmt"
	"reflect"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/ias"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/block"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/commitment"
	"github.com/oasisprotocol/oasis-core/go/roothash/api/message"
	"github.com/oasisprotocol/oasis-core/go/runtime/transaction"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
)

// NOTE: Bump RuntimeProtocol version in go/common/version if you
//       change any of the structures below.

// MessageType is a message type.
type MessageType uint8

// String returns a string representation of a message type.
func (m MessageType) String() string {
	switch m {
	case MessageRequest:
		return "request"
	case MessageResponse:
		return "response"
	default:
		return fmt.Sprintf("[malformed: %d]", m)
	}
}

const (
	// Invalid message (should never be seen on the wire).
	MessageInvalid MessageType = 0

	// Request message.
	MessageRequest MessageType = 1

	// Response message.
	MessageResponse MessageType = 2
)

// Message is a protocol message.
type Message struct {
	ID          uint64      `json:"id"`
	MessageType MessageType `json:"message_type"`
	Body        Body        `json:"body"`
	SpanContext []byte      `json:"span_context"`
}

// Body is a protocol message body.
type Body struct {
	Empty *Empty `json:",omitempty"`
	Error *Error `json:",omitempty"`

	// Runtime interface.
	RuntimeInfoRequest                    *RuntimeInfoRequest                    `json:",omitempty"`
	RuntimeInfoResponse                   *RuntimeInfoResponse                   `json:",omitempty"`
	RuntimePingRequest                    *Empty                                 `json:",omitempty"`
	RuntimeShutdownRequest                *Empty                                 `json:",omitempty"`
	RuntimeCapabilityTEERakInitRequest    *RuntimeCapabilityTEERakInitRequest    `json:",omitempty"`
	RuntimeCapabilityTEERakInitResponse   *Empty                                 `json:",omitempty"`
	RuntimeCapabilityTEERakReportRequest  *Empty                                 `json:",omitempty"`
	RuntimeCapabilityTEERakReportResponse *RuntimeCapabilityTEERakReportResponse `json:",omitempty"`
	RuntimeCapabilityTEERakAvrRequest     *RuntimeCapabilityTEERakAvrRequest     `json:",omitempty"`
	RuntimeCapabilityTEERakAvrResponse    *Empty                                 `json:",omitempty"`
	RuntimeRPCCallRequest                 *RuntimeRPCCallRequest                 `json:",omitempty"`
	RuntimeRPCCallResponse                *RuntimeRPCCallResponse                `json:",omitempty"`
	RuntimeLocalRPCCallRequest            *RuntimeLocalRPCCallRequest            `json:",omitempty"`
	RuntimeLocalRPCCallResponse           *RuntimeLocalRPCCallResponse           `json:",omitempty"`
	RuntimeCheckTxBatchRequest            *RuntimeCheckTxBatchRequest            `json:",omitempty"`
	RuntimeCheckTxBatchResponse           *RuntimeCheckTxBatchResponse           `json:",omitempty"`
	RuntimeExecuteTxBatchRequest          *RuntimeExecuteTxBatchRequest          `json:",omitempty"`
	RuntimeExecuteTxBatchResponse         *RuntimeExecuteTxBatchResponse         `json:",omitempty"`
	RuntimeAbortRequest                   *Empty                                 `json:",omitempty"`
	RuntimeAbortResponse                  *Empty                                 `json:",omitempty"`
	RuntimeKeyManagerPolicyUpdateRequest  *RuntimeKeyManagerPolicyUpdateRequest  `json:",omitempty"`
	RuntimeKeyManagerPolicyUpdateResponse *Empty                                 `json:",omitempty"`
	RuntimeQueryRequest                   *RuntimeQueryRequest                   `json:",omitempty"`
	RuntimeQueryResponse                  *RuntimeQueryResponse                  `json:",omitempty"`

	// Host interface.
	HostRPCCallRequest          *HostRPCCallRequest          `json:",omitempty"`
	HostRPCCallResponse         *HostRPCCallResponse         `json:",omitempty"`
	HostStorageSyncRequest      *HostStorageSyncRequest      `json:",omitempty"`
	HostStorageSyncResponse     *HostStorageSyncResponse     `json:",omitempty"`
	HostLocalStorageGetRequest  *HostLocalStorageGetRequest  `json:",omitempty"`
	HostLocalStorageGetResponse *HostLocalStorageGetResponse `json:",omitempty"`
	HostLocalStorageSetRequest  *HostLocalStorageSetRequest  `json:",omitempty"`
	HostLocalStorageSetResponse *Empty                       `json:",omitempty"`
}

// Type returns the message type by determining the name of the first non-nil member.
func (body Body) Type() string {
	b := reflect.ValueOf(body)
	for i := 0; i < b.NumField(); i++ {
		if !b.Field(i).IsNil() {
			return reflect.TypeOf(body).Field(i).Name
		}
	}
	return ""
}

// Empty is an empty message body.
type Empty struct {
}

// Error is a message body representing an error.
type Error struct {
	Module  string `json:"module,omitempty"`
	Code    uint32 `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// String returns a string representation of this runtime error.
func (e Error) String() string {
	return fmt.Sprintf("runtime error: module: %s code: %d message: %s", e.Module, e.Code, e.Message)
}

// RuntimeInfoRequest is a worker info request message body.
type RuntimeInfoRequest struct {
	// RuntimeID is the assigned runtime ID of the loaded runtime.
	RuntimeID common.Namespace `json:"runtime_id"`

	// ConsensusBackend is the name of the consensus backend that is in use for the consensus layer.
	ConsensusBackend string `json:"consensus_backend"`
	// ConsensusProtocolVersion is the consensus protocol version that is in use for the consensus
	// layer.
	ConsensusProtocolVersion version.Version `json:"consensus_protocol_version"`
	// ConsensusChainContext is the consensus layer chain domain separation context.
	ConsensusChainContext string `json:"consensus_chain_context"`
}

// RuntimeInfoResponse is a worker info response message body.
type RuntimeInfoResponse struct {
	// ProtocolVersion is the runtime protocol version supported by the worker.
	ProtocolVersion version.Version `json:"protocol_version"`

	// RuntimeVersion is the version of the runtime.
	RuntimeVersion version.Version `json:"runtime_version"`
}

// RuntimeCapabilityTEERakInitRequest is a worker RFC 0009 CapabilityTEE
// initialization request message body.
type RuntimeCapabilityTEERakInitRequest struct {
	TargetInfo []byte `json:"target_info"`
}

// RuntimeCapabilityTEERakReportResponse is a worker RFC 0009 CapabilityTEE RAK response message body.
type RuntimeCapabilityTEERakReportResponse struct {
	RakPub signature.PublicKey `json:"rak_pub"`
	Report []byte              `json:"report"`
	Nonce  string              `json:"nonce"`
}

// RuntimeCapabilityTEERakAvrRequest is a worker RFC 0009 CapabilityTEE RAK AVR setup request message body.
type RuntimeCapabilityTEERakAvrRequest struct {
	AVR ias.AVRBundle `json:"avr"`
}

// RuntimeRPCCallRequest is a worker RPC call request message body.
type RuntimeRPCCallRequest struct {
	// Request.
	Request []byte `json:"request"`
}

// RuntimeRPCCallResponse is a worker RPC call response message body.
type RuntimeRPCCallResponse struct {
	// Response.
	Response []byte `json:"response"`
}

// RuntimeLocalRPCCallRequest is a worker local RPC call request message body.
type RuntimeLocalRPCCallRequest struct {
	// Request.
	Request []byte `json:"request"`
}

// RuntimeLocalRPCCallResponse is a worker local RPC call response message body.
type RuntimeLocalRPCCallResponse struct {
	// Response.
	Response []byte `json:"response"`
}

// RuntimeCheckTxBatchRequest is a worker check tx batch request message body.
type RuntimeCheckTxBatchRequest struct {
	// ConsensusBlock is the consensus light block at the last finalized round
	// height (e.g., corresponding to .Block.Header.Round).
	ConsensusBlock consensus.LightBlock `json:"consensus_block"`

	// Batch of runtime inputs to check.
	Inputs transaction.RawBatch `json:"inputs"`
	// Block on which the batch check should be based.
	Block block.Block `json:"block"`
	// Epoch is the current epoch number.
	Epoch beacon.EpochTime `json:"epoch"`
}

// CheckTxResult contains the result of a CheckTx operation.
type CheckTxResult struct {
	// Error is the error (if any) that resulted from the operation.
	Error Error `json:"error"`

	// Meta contains metadata that can be used for scheduling transactions by the scheduler.
	Meta *CheckTxMetadata `json:"meta,omitempty"`
}

// CheckTxMetadata is the transaction check-tx metadata.
type CheckTxMetadata struct {
	// Priority is the transaction's priority.
	Priority uint64 `json:"priority,omitempty"`

	// Weight are runtime specific transaction weights.
	Weights map[string]uint64 `json:"weights,omitempty"`
}

// IsSuccess returns true if transaction execution was successful.
func (r *CheckTxResult) IsSuccess() bool {
	return r.Error.Code == errors.CodeNoError
}

// ToCheckedTransaction creates CheckedTransaction from CheckTx result.
//
// Assumes a successful result.
func (r *CheckTxResult) ToCheckedTransaction(rawTx []byte) *transaction.CheckedTransaction {
	switch r.Meta {
	case nil:
		return transaction.NewCheckedTransaction(rawTx, 0, nil)
	default:
		return transaction.NewCheckedTransaction(rawTx, r.Meta.Priority, r.Meta.Weights)
	}
}

// RuntimeCheckTxBatchResponse is a worker check tx batch response message body.
type RuntimeCheckTxBatchResponse struct {
	// Batch of CheckTx results corresponding to transactions passed on input.
	Results []CheckTxResult `json:"results"`
}

// ComputedBatch is a computed batch.
type ComputedBatch struct {
	// Header is the compute results header.
	Header commitment.ComputeResultsHeader `json:"header"`
	// Log that generates the I/O tree.
	IOWriteLog storage.WriteLog `json:"io_write_log"`
	// Batch of storage write operations.
	StateWriteLog storage.WriteLog `json:"state_write_log"`
	// If this runtime uses a TEE, then this is the signature of Header with
	// node's RAK for this runtime.
	RakSig signature.RawSignature `json:"rak_sig"`
	// Messages are the emitted runtime messages.
	Messages []message.Message `json:"messages"`
}

// String returns a string representation of a computed batch.
func (b *ComputedBatch) String() string {
	return "<ComputedBatch>"
}

// RuntimeExecuteTxBatchRequest is a worker execute tx batch request message body.
type RuntimeExecuteTxBatchRequest struct {
	// ConsensusBlock is the consensus light block at the last finalized round
	// height (e.g., corresponding to .Block.Header.Round).
	ConsensusBlock consensus.LightBlock `json:"consensus_block"`

	// StakingEvents are the runtime related staking events since last round
	// and up to ConsensusBlock height.
	StakingEvents []*staking.Event `json:"staking_events"`

	// RoundResults are the results of executing the previous successful round.
	RoundResults *roothash.RoundResults `json:"round_results"`

	// IORoot is the I/O root containing the inputs (transactions) that
	// the compute node should use. It must match what is passed in "inputs".
	IORoot hash.Hash `json:"io_root"`
	// Batch of inputs (transactions).
	Inputs transaction.RawBatch `json:"inputs"`
	// Block on which the batch computation should be based.
	Block block.Block `json:"block"`
	// Epoch is the current epoch number.
	Epoch beacon.EpochTime `json:"epoch"`

	// MaxMessages is the maximum number of messages that can be emitted in this
	// round. Any more messages will be rejected by the consensus layer.
	MaxMessages uint32 `json:"max_messages"`
}

// RuntimeExecuteTxBatchResponse is a worker execute tx batch response message body.
type RuntimeExecuteTxBatchResponse struct {
	Batch ComputedBatch `json:"batch"`
}

// RuntimeKeyManagerPolicyUpdateRequest is a runtime key manager policy request
// message body.
type RuntimeKeyManagerPolicyUpdateRequest struct {
	SignedPolicyRaw []byte `json:"signed_policy_raw"`
}

// RuntimeQueryRequest is a runtime query request message body.
type RuntimeQueryRequest struct {
	// ConsensusBlock is the consensus light block at the last finalized round
	// height (e.g., corresponding to .Header.Round).
	ConsensusBlock consensus.LightBlock `json:"consensus_block"`

	// Header is the current block header.
	Header block.Header `json:"header"`
	// Epoch is the current epoch number.
	Epoch beacon.EpochTime `json:"epoch"`

	Method string          `json:"method"`
	Args   cbor.RawMessage `json:"args,omitempty"`
}

// RuntimeQueryRequest is a runtime query response message body.
type RuntimeQueryResponse struct {
	Data cbor.RawMessage `json:"data,omitempty"`
}

// HostRPCCallRequest is a host RPC call request message body.
type HostRPCCallRequest struct {
	Endpoint string `json:"endpoint"`
	Request  []byte `json:"request"`
}

// HostRPCCallResponse is a host RPC call response message body.
type HostRPCCallResponse struct {
	Response []byte `json:"response"`
}

// HostStorageEndpoint is the host storage endpoint.
type HostStorageEndpoint uint8

const (
	// HostStorageEndpointRuntime is the runtime state storage endpoint.
	HostStorageEndpointRuntime HostStorageEndpoint = 0
	// HostStorageEndpointConsensus is the consensus layer state storage endpoint.
	HostStorageEndpointConsensus HostStorageEndpoint = 1
)

// HostStorageSyncRequest is a host storage read syncer request message body.
type HostStorageSyncRequest struct {
	// Endpoint is the storage endpoint to which this request should be routed.
	Endpoint HostStorageEndpoint `json:"endpoint,omitempty"`

	SyncGet         *storage.GetRequest         `json:",omitempty"`
	SyncGetPrefixes *storage.GetPrefixesRequest `json:",omitempty"`
	SyncIterate     *storage.IterateRequest     `json:",omitempty"`
}

// HostStorageSyncResponse is a host storage read syncer response body.
type HostStorageSyncResponse struct {
	ProofResponse *storage.ProofResponse `json:",omitempty"`
}

// HostLocalStorageGetRequest is a host local storage get request message body.
type HostLocalStorageGetRequest struct {
	Key []byte `json:"key"`
}

// HostLocalStorageGetResponse is a host local storage get response message body.
type HostLocalStorageGetResponse struct {
	Value []byte `json:"value"`
}

// HostLocalStorageSetRequest is a host local storage set request message body.
type HostLocalStorageSetRequest struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
