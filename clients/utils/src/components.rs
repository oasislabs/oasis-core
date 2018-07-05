//! DI components for use in clients.
use ekiden_consensus_client;
use ekiden_core;
use ekiden_di::{Component, KnownComponents};
use ekiden_epochtime;
use ekiden_ethereum;
use ekiden_instrumentation_prometheus;
use ekiden_registry_client;
use ekiden_scheduler_client;
use ekiden_storage_frontend;

/// Register known components for dependency injection.
pub fn register_components(known_components: &mut KnownComponents) {
    // Environment.
    ekiden_core::environment::GrpcEnvironment::register(known_components);
    // Time Notifier.
    ekiden_epochtime::local::LocalTimeSourceNotifier::register(known_components);
    // Beacon.
    ekiden_ethereum::EthereumRandomBeaconViaWebsocket::register(known_components);
    // Local identities.
    ekiden_ethereum::identity::EthereumEntityIdentity::register(known_components);
    ekiden_ethereum::identity::EthereumNodeIdentity::register(known_components);
    // Ethereum service.
    ekiden_ethereum::web3_di::Web3Factory::register(known_components);
    // Instrumentation.
    ekiden_instrumentation_prometheus::PrometheusMetricCollector::register(known_components);
    // Scheduler.
    ekiden_scheduler_client::SchedulerClient::register(known_components);
    // Entity registry.
    ekiden_registry_client::EntityRegistryClient::register(known_components);
    // Consensus.
    ekiden_consensus_client::ConsensusClient::register(known_components);
    // Storage.
    ekiden_storage_frontend::StorageClient::register(known_components);
}

/// Create known component registry.
pub fn create_known_components() -> KnownComponents {
    let mut known_components = KnownComponents::new();
    register_components(&mut known_components);

    known_components
}
