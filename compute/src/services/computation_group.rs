//! Inter-node service.
use std::sync::Arc;

use grpcio;
use grpcio::{RpcStatus, RpcStatusCode};

use ekiden_compute_api::{ComputationGroup, SubmitBatchRequest, SubmitBatchResponse};
use ekiden_core::bytes::H256;
use ekiden_core::error::Result;
use ekiden_core::futures::Future;
use ekiden_core::x509::get_node_id;

use super::super::consensus::ConsensusFrontend;

struct Inner {
    /// Consensus frontend.
    consensus_frontend: Arc<ConsensusFrontend>,
}

#[derive(Clone)]
pub struct ComputationGroupService {
    inner: Arc<Inner>,
}

impl ComputationGroupService {
    /// Create new computation group service.
    pub fn new(consensus_frontend: Arc<ConsensusFrontend>) -> Self {
        ComputationGroupService {
            inner: Arc::new(Inner { consensus_frontend }),
        }
    }
}

impl ComputationGroup for ComputationGroupService {
    fn submit_batch(
        &self,
        ctx: grpcio::RpcContext,
        request: SubmitBatchRequest,
        sink: grpcio::UnarySink<SubmitBatchResponse>,
    ) {
        measure_histogram_timer!("submit_batch_time");
        measure_counter_inc!("submit_batch_calls");

        let f = || -> Result<()> {
            let node_id = get_node_id(&ctx)?;
            let batch_hash = H256::try_from(request.get_batch_hash())?;

            self.inner
                .consensus_frontend
                .process_remote_batch(node_id, batch_hash)?;

            Ok(())
        };

        let f = match f() {
            Ok(()) => sink.success(SubmitBatchResponse::new()),
            Err(error) => sink.fail(RpcStatus::new(
                RpcStatusCode::Internal,
                Some(error.description().to_owned()),
            )),
        };
        ctx.spawn(f.map_err(|_error| ()));
    }
}
