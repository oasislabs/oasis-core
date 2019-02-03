//! Entity registry gRPC client.
use std::{convert::TryFrom, error::Error as StdError};

use grpcio::Channel;

use ekiden_common::{
    address::Address,
    bytes::B256,
    error::{Error, Result},
    futures::prelude::*,
    x509::Certificate,
};
use ekiden_registry_api as api;
use ekiden_registry_base::{EntityRegistryBackend, NodeTransport};

/// Scheduler client implements the Scheduler interface.
pub struct EntityRegistryClient(api::EntityRegistryClient);

impl EntityRegistryClient {
    pub fn new(channel: Channel) -> Self {
        EntityRegistryClient(api::EntityRegistryClient::new(channel))
    }
}

impl EntityRegistryBackend for EntityRegistryClient {
    fn get_node_transport(&self, id: B256) -> BoxFuture<NodeTransport> {
        let mut request = api::NodeRequest::new();
        request.set_id(id.to_vec());
        match self.0.get_node_transport_async(&request) {
            Ok(f) => Box::new(f.map_err(|error| Error::new(error.description())).and_then(
                |mut response| {
                    let certificate = Certificate::try_from(response.get_certificate().clone())?;
                    let mut addresses = response.take_addresses().into_vec();
                    let addresses: Result<_> = addresses
                        .drain(..)
                        .map(|address| Address::try_from(address))
                        .collect();
                    let addresses = addresses?;

                    Ok(NodeTransport {
                        addresses,
                        certificate,
                    })
                },
            )),
            Err(error) => Box::new(future::err(Error::new(error.description()))),
        }
    }
}
