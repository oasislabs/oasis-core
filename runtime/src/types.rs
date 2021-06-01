//! Types used by the worker-host protocol.
use std::collections::BTreeMap;

use serde::{self, Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes;
use serde_repr::*;
use thiserror::Error;

use crate::{
    common::{
        cbor,
        crypto::{
            hash::Hash,
            signature::{PublicKey, Signature},
        },
        namespace::Namespace,
        sgx::avr::AVR,
        version::Version,
    },
    consensus::{
        beacon::EpochTime,
        roothash::{self, Block, ComputeResultsHeader, Header},
        staking::Event as StakingEvent,
        tendermint::LightBlock,
    },
    storage::mkvs::{sync, WriteLog},
    transaction::types::TxnBatch,
};

/// Computed batch.
#[derive(Debug, Serialize, Deserialize)]
pub struct ComputedBatch {
    /// Compute results header.
    pub header: ComputeResultsHeader,
    /// Log that generates the I/O tree.
    pub io_write_log: WriteLog,
    /// Log of changes to the state tree.
    pub state_write_log: WriteLog,
    /// If this runtime uses a TEE, then this is the signature of the batch's
    /// BatchSigMessage with the node's RAK for this runtime.
    pub rak_sig: Signature,
    /// Messages emitted by the runtime.
    pub messages: Vec<roothash::Message>,
}

/// Storage sync request.
#[derive(Debug, Serialize, Deserialize)]
pub enum StorageSyncRequest {
    SyncGet(sync::GetRequest),
    SyncGetPrefixes(sync::GetPrefixesRequest),
    SyncIterate(sync::IterateRequest),
}

/// Storage sync response.
#[derive(Debug, Serialize, Deserialize)]
pub enum StorageSyncResponse {
    ProofResponse(sync::ProofResponse),
}

/// Host storage endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum HostStorageEndpoint {
    Runtime = 0,
    Consensus = 1,
}

/// Runtime host protocol message body.
#[derive(Debug, Serialize, Deserialize)]
pub enum Body {
    // An empty body.
    Empty {},

    // An error response.
    Error(Error),

    // Runtime interface.
    RuntimeInfoRequest {
        runtime_id: Namespace,
        consensus_backend: String,
        consensus_protocol_version: Version,
        consensus_chain_context: String,
    },
    RuntimeInfoResponse {
        protocol_version: Version,
        runtime_version: Version,
    },
    RuntimePingRequest {},
    RuntimeShutdownRequest {},
    RuntimeAbortRequest {},
    RuntimeAbortResponse {},
    RuntimeCapabilityTEERakInitRequest {
        #[serde(with = "serde_bytes")]
        target_info: Vec<u8>,
    },
    RuntimeCapabilityTEERakInitResponse {},
    RuntimeCapabilityTEERakReportRequest {},
    RuntimeCapabilityTEERakReportResponse {
        rak_pub: PublicKey,
        #[serde(with = "serde_bytes")]
        report: Vec<u8>,
        nonce: String,
    },
    RuntimeCapabilityTEERakAvrRequest {
        avr: AVR,
    },
    RuntimeCapabilityTEERakAvrResponse {},
    RuntimeRPCCallRequest {
        #[serde(with = "serde_bytes")]
        request: Vec<u8>,
    },
    RuntimeRPCCallResponse {
        #[serde(with = "serde_bytes")]
        response: Vec<u8>,
    },
    RuntimeLocalRPCCallRequest {
        #[serde(with = "serde_bytes")]
        request: Vec<u8>,
    },
    RuntimeLocalRPCCallResponse {
        #[serde(with = "serde_bytes")]
        response: Vec<u8>,
    },
    RuntimeCheckTxBatchRequest {
        consensus_block: LightBlock,
        inputs: TxnBatch,
        block: Block,
        epoch: EpochTime,
    },
    RuntimeCheckTxBatchResponse {
        results: Vec<CheckTxResult>,
    },
    RuntimeExecuteTxBatchRequest {
        consensus_block: LightBlock,
        #[serde(skip_serializing_if = "Option::is_none")]
        staking_events: Option<Vec<StakingEvent>>,
        round_results: roothash::RoundResults,
        io_root: Hash,
        #[serde(skip_serializing_if = "Option::is_none")]
        inputs: Option<TxnBatch>,
        block: Block,
        epoch: EpochTime,
        max_messages: u32,
    },
    RuntimeExecuteTxBatchResponse {
        batch: ComputedBatch,
    },
    RuntimeKeyManagerPolicyUpdateRequest {
        #[serde(with = "serde_bytes")]
        signed_policy_raw: Vec<u8>,
    },
    RuntimeKeyManagerPolicyUpdateResponse {},
    RuntimeQueryRequest {
        consensus_block: LightBlock,
        header: Header,
        epoch: EpochTime,
        method: String,
        args: cbor::Value,
    },
    RuntimeQueryResponse {
        data: cbor::Value,
    },

    // Host interface.
    HostRPCCallRequest {
        endpoint: String,
        #[serde(with = "serde_bytes")]
        request: Vec<u8>,
    },
    HostRPCCallResponse {
        #[serde(with = "serde_bytes")]
        response: Vec<u8>,
    },
    HostStorageSyncRequest {
        endpoint: HostStorageEndpoint,
        #[serde(flatten)]
        request: StorageSyncRequest,
    },
    HostStorageSyncResponse {
        #[serde(flatten)]
        response: StorageSyncResponse,
    },
    HostStorageSyncSerializedResponse {
        #[serde(with = "serde_bytes")]
        serialized: Vec<u8>,
    },
    HostLocalStorageGetRequest {
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
    HostLocalStorageGetResponse {
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    HostLocalStorageSetRequest {
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    HostLocalStorageSetResponse {},
}

/// A serializable error.
#[derive(Clone, Debug, Default, Error, Serialize, Deserialize)]
#[error("module: {module} code: {code} message: {message}")]
pub struct Error {
    #[serde(default)]
    pub module: String,

    #[serde(default)]
    pub code: u32,

    #[serde(default)]
    pub message: String,
}

impl Error {
    /// Create a new error.
    pub fn new(module: &str, code: u32, msg: &str) -> Self {
        Self {
            module: module.to_owned(),
            code,
            message: msg.to_owned(),
        }
    }
}

/// Result of a CheckTx operation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckTxResult {
    #[serde(rename = "error")]
    pub error: Error,

    #[serde(rename = "meta")]
    pub meta: Option<CheckTxMetadata>,
}

/// CheckTx transaction metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckTxMetadata {
    #[serde(skip_serializing_if = "num_traits::Zero::is_zero")]
    #[serde(default)]
    pub priority: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub weights: Option<BTreeMap<CheckTxWeight, u64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(untagged)]
pub enum CheckTxWeight {
    /// Consensus messages weight key.
    #[serde(rename = "consensus_messages")]
    ConsensusMessages,
    /// Runtime specific weight key.
    Custom(String),
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum MessageType {
    /// Invalid message (should never be seen on the wire).
    Invalid = 0,
    /// Request.
    Request = 1,
    /// Response.
    Response = 2,
}

impl serde::Serialize for MessageType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> serde::Deserialize<'de> for MessageType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u8::deserialize(deserializer)? {
            1 => Ok(MessageType::Request),
            2 => Ok(MessageType::Response),
            _ => Err(serde::de::Error::custom("invalid message type")),
        }
    }
}

/// Runtime protocol message.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    /// Unique request identifier.
    pub id: u64,
    /// Message type.
    pub message_type: MessageType,
    /// Message body.
    pub body: Body,
    /// Opentracing's SpanContext serialized in binary format.
    #[serde(with = "serde_bytes")]
    pub span_context: Vec<u8>,
}
