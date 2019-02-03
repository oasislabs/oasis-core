//! Protocol trait implementations.
use sgx_types;

use ekiden_core::{
    bytes::{B256, H256},
    enclave::api as identity_api,
    error::{Error, Result},
    futures::{block_on, prelude::*},
    rpc::client::ClientEndpoint,
    runtime::batch::CallBatch,
};
use ekiden_roothash_base::Block;
use ekiden_storage_base::{InsertOptions, StorageBackend};
use ekiden_untrusted::{enclave::identity::IAS, rpc::router::Handler as EnclaveRpcHandler};

use super::{
    protocol::Handler,
    types::{Body, ComputedBatch},
    Host, Protocol, Worker,
};

impl Worker for Protocol {
    fn worker_shutdown(&self) -> BoxFuture<()> {
        self.make_request(Body::WorkerShutdownRequest {})
            .and_then(|body| match body {
                Body::Empty {} => Ok(()),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn worker_abort(&self) -> BoxFuture<()> {
        self.make_request(Body::WorkerAbortRequest {})
            .and_then(|body| match body {
                Body::WorkerAbortResponse {} => Ok(()),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn capabilitytee_gid(&self) -> BoxFuture<[u8; 4]> {
        self.make_request(Body::WorkerCapabilityTEEGidRequest {})
            .and_then(|body| match body {
                Body::WorkerCapabilityTEEGidResponse { gid } => Ok(gid),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn capabilitytee_rak_quote(
        &self,
        quote_type: u32,
        spid: [u8; 16],
        sig_rl: Vec<u8>,
    ) -> BoxFuture<(B256, Vec<u8>)> {
        self.make_request(Body::WorkerCapabilityTEERakQuoteRequest {
            quote_type,
            spid: spid.to_vec(),
            sig_rl,
        })
        .and_then(|body| match body {
            Body::WorkerCapabilityTEERakQuoteResponse { rak_pub, quote } => Ok((rak_pub, quote)),
            _ => Err(Error::new("malformed response")),
        })
        .into_box()
    }

    fn rpc_call(&self, request: Vec<u8>) -> BoxFuture<Vec<u8>> {
        self.make_request(Body::WorkerRPCCallRequest { request })
            .and_then(|body| match body {
                Body::WorkerRPCCallResponse { response } => Ok(response),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn runtime_call_batch(&self, calls: CallBatch, block: Block) -> BoxFuture<ComputedBatch> {
        self.make_request(Body::WorkerRuntimeCallBatchRequest { calls, block })
            .and_then(|body| match body {
                Body::WorkerRuntimeCallBatchResponse { batch } => Ok(batch),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }
}

impl Host for Protocol {
    fn rpc_call(&self, endpoint: ClientEndpoint, request: Vec<u8>) -> BoxFuture<Vec<u8>> {
        self.make_request(Body::HostRPCCallRequest { endpoint, request })
            .and_then(|body| match body {
                Body::HostRPCCallResponse { response } => Ok(response),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn ias_get_spid(&self) -> BoxFuture<sgx_types::sgx_spid_t> {
        self.make_request(Body::HostIasGetSpidRequest {})
            .and_then(|body| match body {
                Body::HostIasGetSpidResponse { spid } => {
                    if spid.len() != 16 {
                        return Err(Error::new("malformed response"));
                    }

                    let mut sgx_spid: sgx_types::sgx_spid_t = Default::default();
                    sgx_spid.id.copy_from_slice(&spid[..16]);

                    Ok(sgx_spid)
                }
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn ias_get_quote_type(&self) -> BoxFuture<sgx_types::sgx_quote_sign_type_t> {
        self.make_request(Body::HostIasGetQuoteTypeRequest {})
            .and_then(|body| match body {
                Body::HostIasGetQuoteTypeResponse { quote_type: 0 } => {
                    Ok(sgx_types::sgx_quote_sign_type_t::SGX_UNLINKABLE_SIGNATURE)
                }
                Body::HostIasGetQuoteTypeResponse { quote_type: 1 } => {
                    Ok(sgx_types::sgx_quote_sign_type_t::SGX_LINKABLE_SIGNATURE)
                }
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn ias_sigrl(&self, gid: &sgx_types::sgx_epid_group_id_t) -> BoxFuture<Vec<u8>> {
        self.make_request(Body::HostIasSigRlRequest { gid: gid.clone() })
            .and_then(|body| match body {
                Body::HostIasSigRlResponse { sigrl } => Ok(sigrl),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn ias_report(&self, quote: Vec<u8>) -> BoxFuture<identity_api::AvReport> {
        self.make_request(Body::HostIasReportRequest { quote })
            .and_then(|body| match body {
                Body::HostIasReportResponse {
                    avr,
                    signature,
                    certificates,
                } => {
                    let mut report = identity_api::AvReport::new();
                    report.set_body(avr);
                    report.set_signature(signature);
                    report.set_certificates(certificates);
                    Ok(report)
                }
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn storage_get(&self, key: H256) -> BoxFuture<Vec<u8>> {
        self.make_request(Body::HostStorageGetRequest { key })
            .and_then(|body| match body {
                Body::HostStorageGetResponse { value } => Ok(value),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }

    fn storage_get_batch(&self, keys: Vec<H256>) -> BoxFuture<Vec<Option<Vec<u8>>>> {
        self.make_request(Body::HostStorageGetBatchRequest { keys })
            .and_then(|body| match body {
                Body::HostStorageGetBatchResponse { values } => Ok(values
                    .into_iter()
                    .map(|x| x.map(|bytes| bytes.into()))
                    .collect()),
                _ => Err(Error::new("malformed response")),
            })
            .into_box()
    }
}

impl IAS for Protocol {
    fn get_spid(&self) -> sgx_types::sgx_spid_t {
        block_on(self.environment(), self.ias_get_spid())
            .expect("ias get spid request must not fail")
    }

    fn get_quote_type(&self) -> sgx_types::sgx_quote_sign_type_t {
        block_on(self.environment(), self.ias_get_quote_type())
            .expect("ias get quote type request must not fail")
    }

    fn sigrl(&self, gid: &sgx_types::sgx_epid_group_id_t) -> Vec<u8> {
        block_on(self.environment(), self.ias_sigrl(gid)).expect("ias sigrl request must not fail")
    }

    fn report(&self, quote: &[u8]) -> identity_api::AvReport {
        block_on(self.environment(), self.ias_report(quote.to_vec()))
            .expect("ias report request must not fail")
    }
}

impl EnclaveRpcHandler for Protocol {
    fn get_endpoints(&self) -> Vec<ClientEndpoint> {
        vec![ClientEndpoint::KeyManager]
    }

    fn handle(&self, endpoint: &ClientEndpoint, request: Vec<u8>) -> Result<Vec<u8>> {
        block_on(self.environment(), Host::rpc_call(self, *endpoint, request))
    }
}

impl StorageBackend for Protocol {
    fn get(&self, key: H256) -> BoxFuture<Vec<u8>> {
        self.storage_get(key)
    }

    fn get_batch(&self, keys: Vec<H256>) -> BoxFuture<Vec<Option<Vec<u8>>>> {
        self.storage_get_batch(keys)
    }

    fn insert(&self, _value: Vec<u8>, _expiry: u64, _opts: InsertOptions) -> BoxFuture<()> {
        unimplemented!("worker cannot insert directly to storage");
    }

    fn insert_batch(&self, _values: Vec<(Vec<u8>, u64)>, _opts: InsertOptions) -> BoxFuture<()> {
        unimplemented!("worker cannot insert directly to storage");
    }

    fn get_keys(&self) -> BoxStream<(H256, u64)> {
        unimplemented!();
    }
}

/// Worker protocol handler.
pub struct WorkerHandler<T: Worker>(pub T);
/// Host protocol handler.
pub struct HostHandler<T: Host>(pub T);

impl<T: Worker> Handler for WorkerHandler<T> {
    fn handle(&self, body: Body) -> BoxFuture<Body> {
        match body {
            Body::WorkerPingRequest {} => future::ok(Body::Empty {}).into_box(),
            Body::WorkerShutdownRequest {} => {
                self.0.worker_shutdown().map(|_| Body::Empty {}).into_box()
            }
            Body::WorkerAbortRequest {} => self
                .0
                .worker_abort()
                .map(|_| Body::WorkerAbortResponse {})
                .into_box(),
            Body::WorkerCapabilityTEEGidRequest {} => self
                .0
                .capabilitytee_gid()
                .map(|gid| Body::WorkerCapabilityTEEGidResponse { gid })
                .into_box(),
            Body::WorkerCapabilityTEERakQuoteRequest {
                quote_type,
                spid,
                sig_rl,
            } => {
                if spid.len() != 16 {
                    return future::err(Error::new("malformed SPID")).into_box();
                }

                let mut cspid = [0u8; 16];
                cspid.copy_from_slice(&spid[..]);

                self.0
                    .capabilitytee_rak_quote(quote_type, cspid, sig_rl)
                    .map(
                        |(rak_pub, quote)| Body::WorkerCapabilityTEERakQuoteResponse {
                            rak_pub,
                            quote,
                        },
                    )
                    .into_box()
            }
            Body::WorkerRPCCallRequest { request } => self
                .0
                .rpc_call(request)
                .map(|response| Body::WorkerRPCCallResponse { response })
                .into_box(),
            Body::WorkerRuntimeCallBatchRequest { calls, block } => self
                .0
                .runtime_call_batch(calls, block)
                .map(|batch| Body::WorkerRuntimeCallBatchResponse { batch })
                .into_box(),
            _ => future::err(Error::new("unsupported method")).into_box(),
        }
    }
}

impl<T: Host> Handler for HostHandler<T> {
    fn handle(&self, body: Body) -> BoxFuture<Body> {
        match body {
            Body::HostRPCCallRequest { endpoint, request } => self
                .0
                .rpc_call(endpoint, request)
                .map(|response| Body::HostRPCCallResponse { response })
                .into_box(),
            Body::HostIasGetSpidRequest {} => self
                .0
                .ias_get_spid()
                .map(|spid| Body::HostIasGetSpidResponse {
                    spid: spid.id.to_vec(),
                })
                .into_box(),
            Body::HostIasGetQuoteTypeRequest {} => self
                .0
                .ias_get_quote_type()
                .map(|quote_type| Body::HostIasGetQuoteTypeResponse {
                    quote_type: quote_type as u32,
                })
                .into_box(),
            Body::HostIasSigRlRequest { gid } => self
                .0
                .ias_sigrl(&gid)
                .map(|sigrl| Body::HostIasSigRlResponse { sigrl })
                .into_box(),
            Body::HostIasReportRequest { quote } => self
                .0
                .ias_report(quote)
                .map(|mut report| Body::HostIasReportResponse {
                    avr: report.take_body(),
                    signature: report.take_signature(),
                    certificates: report.take_certificates(),
                })
                .into_box(),
            Body::HostStorageGetRequest { key } => self
                .0
                .storage_get(key)
                .map(|value| Body::HostStorageGetResponse { value })
                .into_box(),
            Body::HostStorageGetBatchRequest { keys } => self
                .0
                .storage_get_batch(keys)
                .map(|values| Body::HostStorageGetBatchResponse {
                    values: values
                        .into_iter()
                        .map(|x| x.map(|bytes| bytes.into()))
                        .collect(),
                })
                .into_box(),
            _ => future::err(Error::new("unsupported method")).into_box(),
        }
    }
}
