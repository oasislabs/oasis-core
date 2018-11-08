#![feature(try_from)]

extern crate sgx_types;

extern crate base64;
extern crate grpcio;
#[macro_use]
extern crate log;
extern crate lru_cache;
extern crate protobuf;
extern crate reqwest;
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde_cbor;
extern crate thread_local;

extern crate ekiden_compute_api;
extern crate ekiden_core;
extern crate ekiden_registry_base;
extern crate ekiden_roothash_base;
extern crate ekiden_rpc_api;
extern crate ekiden_rpc_client;
extern crate ekiden_scheduler_base;
extern crate ekiden_storage_api;
extern crate ekiden_storage_base;
extern crate ekiden_storage_batch;
extern crate ekiden_storage_dummy;
extern crate ekiden_storage_multilayer;
extern crate ekiden_tools;
extern crate ekiden_tracing;
extern crate ekiden_untrusted;
#[macro_use]
extern crate ekiden_instrumentation;

mod group;
mod handlers;
mod node;
mod roothash;
mod services;
mod statetransfer;
mod worker;

// Everything above should be moved into a library, while everything below should be in the binary.

#[macro_use]
extern crate clap;
extern crate pretty_env_logger;

extern crate ekiden_di;
extern crate ekiden_epochtime;
extern crate ekiden_instrumentation_prometheus;
extern crate ekiden_registry_client;
extern crate ekiden_roothash_client;
extern crate ekiden_scheduler_client;
extern crate ekiden_storage_frontend;

use std::path::Path;

use clap::{App, Arg};
use log::LevelFilter;

use ekiden_core::bytes::{B256, H128};
use ekiden_core::environment::Environment;
use ekiden_core::identity::local::load_node_certificate;
use ekiden_di::{Component, KnownComponents};
use ekiden_instrumentation::{set_boxed_metric_collector, MetricCollector};

use self::node::{ComputeNode, ComputeNodeConfiguration, ComputeNodeTestOnlyConfiguration,
                 ProxyIASConfiguration};
use self::roothash::{RootHashConfiguration, RootHashTestOnlyConfiguration};
use self::worker::{KeyManagerConfiguration, WorkerConfiguration};

/// Register known components for dependency injection.
fn register_components(known_components: &mut KnownComponents) {
    // Environment.
    ekiden_core::environment::GrpcEnvironment::register(known_components);
    // Storage.
    ekiden_storage_frontend::StorageClient::register(known_components);
    ekiden_storage_multilayer::MultilayerBackend::register(known_components);
    // Root hash.
    ekiden_roothash_client::RootHashClient::register(known_components);
    ekiden_roothash_client::InternalRootHashSigner::register(known_components);
    // Scheduler.
    ekiden_scheduler_client::SchedulerClient::register(known_components);
    // Entity registry.
    ekiden_registry_client::EntityRegistryClient::register(known_components);
    // Runtime registry.
    ekiden_registry_client::RuntimeRegistryClient::register(known_components);
    // Local identities.
    ekiden_core::identity::LocalEntityIdentity::register(known_components);
    ekiden_core::identity::LocalNodeIdentity::register(known_components);
    // Instrumentation.
    ekiden_instrumentation_prometheus::PrometheusMetricCollector::register(known_components);
}

fn main() {
    // Create known components registry.
    let mut known_components = KnownComponents::new();
    register_components(&mut known_components);

    let matches = App::new("Ekiden Compute Node")
        .version("0.1.0")
        .author("Jernej Kos <jernej@kos.mx>")
        .about("Ekident compute node server")
        .arg(
            Arg::with_name("runtime")
                .index(1)
                .value_name("RUNTIME")
                .help("Signed runtime filename")
                .takes_value(true)
                .required(true)
                .display_order(1)
                .index(1),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .takes_value(true)
                .default_value("9001")
                .display_order(2),
        )
        .arg(
            Arg::with_name("ias-spid")
                .long("ias-spid")
                .value_name("SPID")
                .help("IAS SPID in hex format")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ias-quote-sign-type")
                .long("ias-quote-sign-type")
                .value_name("QUOTE_SIGN_TYPE")
                .help("the quote signature type associated with the SPID")
                .takes_value(true)
                .possible_values(&["unlinkable", "linkable"])
                .default_value("linkable"),
        )
        .arg(
            Arg::with_name("ias-proxy-addr")
                .long("ias-proxy-addr")
                .value_name("PROXY_ADDR")
                .help("IAS proxy address (host:port)")
                .takes_value(true)
                .default_value("127.0.0.1:42261"),
        )
        // TODO: IAS proxy info
        .arg(
            Arg::with_name("key-manager-host")
                .long("key-manager-host")
                .takes_value(true)
                .default_value("127.0.0.1")
                .required_unless("disable-key-manager"),
        )
        .arg(
            Arg::with_name("key-manager-port")
                .long("key-manager-port")
                .takes_value(true)
                .default_value("9003")
                .required_unless("disable-key-manager"),
        )
        .arg(
            Arg::with_name("key-manager-cert")
                .long("key-manager-cert")
                .takes_value(true)
                .default_value("/code/tests/keymanager/km.key")
                .required_unless("disable-key-manager"),
        )
        .arg(Arg::with_name("disable-key-manager").long("disable-key-manager"))
        // TODO: Remove this once we have independent runtime registration.
        .arg(
            Arg::with_name("compute-replicas")
                .long("compute-replicas")
                .help("Number of replicas in the computation group")
                .takes_value(true)
                .default_value("1"),
        )
        // TODO: Remove this once we have independent runtime registration.
        .arg(
            Arg::with_name("compute-backup-replicas")
                .long("compute-backup-replicas")
                .help("Number of backup replicas in the computation group")
                .takes_value(true)
                .default_value("1"),
        )
        // TODO: Remove this once we have independent runtime registration.
        .arg(
            Arg::with_name("compute-allowed-stragglers")
                .long("compute-allowed-stragglers")
                .help("Number of allowed stragglers in the computation group")
                .takes_value(true)
                .default_value("0"),
        )
        .arg(
            Arg::with_name("max-batch-size")
                .long("max-batch-size")
                .help("Maximum size of a batch of requests")
                .default_value("1000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-batch-size-bytes")
                .long("max-batch-size-bytes")
                .help("Maximum size (in bytes) of a batch of requests")
                .default_value("16777216")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max-batch-timeout")
                .long("max-batch-timeout")
                .help("Maximum timeout when waiting for a batch (in ms)")
                .default_value("1000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("identity-file")
                .long("identity-file")
                .help("Path for saving persistent enclave identity")
                .default_value("identity.pb")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("no-persist-identity")
                .long("no-persist-identity")
                .help("Do not persist enclave identity (useful for runtime development)"),
        )
        .arg(
            Arg::with_name("forwarded-rpc-timeout")
                .long("forwarded-rpc-timeout")
                .help("Time limit in seconds for forwarded gRPC calls. If an RPC takes longer than this, we treat it as failed.")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("test-inject-discrepancy")
                .long("test-inject-discrepancy")
                .help("TEST ONLY OPTION: inject discrepancy into batch processing")
                .hidden(true)
        )
        .arg(
            Arg::with_name("test-runtime-id")
                .long("test-runtime-id")
                .help("TEST ONLY OPTION: override runtime identifier")
                .takes_value(true)
                .hidden(true)
        )
        .arg(
            Arg::with_name("test-fail-after-registration")
                .long("test-fail-after-registration")
                .help("TEST ONLY OPTION: fail after registration")
                .hidden(true)
        )
        .arg(
            Arg::with_name("test-fail-after-commit")
                .long("test-fail-after-commit")
                .help("TEST ONLY OPTION: fail after commit")
                .hidden(true)
        )
        .arg(
            Arg::with_name("test-skip-commit-until-round")
                .long("test-skip-commit-until-round")
                .help("TEST ONLY OPTION: skip commit until given round")
                .takes_value(true)
                .hidden(true)
        )
        .args(&known_components.get_arguments())
        .args(&ekiden_tracing::get_arguments())
        .get_matches();

    // Initialize logger.
    pretty_env_logger::formatted_builder()
        .unwrap()
        .filter(None, LevelFilter::Trace)
        .filter(Some("mio"), LevelFilter::Warn)
        .filter(Some("tokio_threadpool"), LevelFilter::Warn)
        .filter(Some("tokio_reactor"), LevelFilter::Warn)
        .filter(Some("tokio_io"), LevelFilter::Warn)
        .filter(Some("tokio_core"), LevelFilter::Warn)
        .filter(Some("web3"), LevelFilter::Info)
        .filter(Some("hyper"), LevelFilter::Warn)
        .filter(Some("rusoto_core::request"), LevelFilter::Info)
        .filter(Some("pagecache::io"), LevelFilter::Debug)
        .filter(Some("want"), LevelFilter::Debug)
        .init();

    // Initialize component container.
    let mut container = known_components
        .build_with_arguments(&matches)
        .expect("failed to initialize component container");

    // Initialize metric collector.
    let metrics = container
        .inject_owned::<MetricCollector>()
        .expect("failed to inject MetricCollector");
    set_boxed_metric_collector(metrics).unwrap();

    // Initialize tracing.
    ekiden_tracing::report_forever("ekiden-compute", &matches);

    let environment = container.inject::<Environment>().unwrap();

    // Setup compute node.
    let mut node = ComputeNode::new(
        ComputeNodeConfiguration {
            port: value_t!(matches, "port", u16).unwrap_or(9001),
            // TODO: Remove this once we have independent runtime registration.
            compute_replicas: value_t!(matches, "compute-replicas", u64)
                .unwrap_or_else(|e| e.exit()),
            // TODO: Remove this once we have independent runtime registration.
            compute_backup_replicas: value_t!(matches, "compute-backup-replicas", u64)
                .unwrap_or_else(|e| e.exit()),
            // TODO: Remove this once we have independent runtime registration.
            compute_allowed_stragglers: value_t!(matches, "compute-allowed-stragglers", u64)
                .unwrap_or_else(|e| e.exit()),
            // Root hash frontend configuration.
            roothash: RootHashConfiguration {
                max_batch_size: value_t!(matches, "max-batch-size", usize).unwrap_or(1000),
                max_batch_size_bytes: value_t!(matches, "max-batch-size-bytes", usize)
                    .unwrap_or(16777216),
                max_batch_timeout: value_t!(matches, "max-batch-timeout", u64).unwrap_or(1000),
                test_only: RootHashTestOnlyConfiguration {
                    inject_discrepancy: matches.is_present("test-inject-discrepancy"),
                    fail_after_commit: matches.is_present("test-fail-after-commit"),
                    skip_commit_until_round: value_t!(matches, "test-skip-commit-until-round", u64)
                        .unwrap_or(0),
                },
            },
            // IAS configuration.
            ias: if matches.is_present("ias-spid") {
                Some(ProxyIASConfiguration {
                    spid: value_t!(matches, "ias-spid", H128).unwrap_or_else(|e| e.exit()),
                    quote_type: matches.value_of("ias-quote-sign-type").unwrap().to_string(),
                    addr: matches.value_of("ias-proxy-addr").unwrap().to_string(),
                })
            } else {
                warn!("IAS is not configured, validation will always return an error.");

                None
            },
            // Worker configuration.
            worker: {
                // Check if passed runtime exists.
                let runtime_filename = matches.value_of("runtime").unwrap();
                if !Path::new(runtime_filename).exists() {
                    panic!(format!("Could not find runtime: {}", runtime_filename))
                }

                WorkerConfiguration {
                    runtime_filename: runtime_filename.to_owned(),
                    saved_identity_path: if matches.is_present("no-persist-identity") {
                        None
                    } else {
                        Some(
                            Path::new(matches.value_of("identity-file").unwrap_or("identity.pb"))
                                .to_owned(),
                        )
                    },
                    forwarded_rpc_timeout: if matches.is_present("rpc-timeout") {
                        Some(std::time::Duration::new(
                            value_t_or_exit!(matches, "forwarded-rpc-timeout", u64),
                            0,
                        ))
                    } else {
                        None
                    },
                    // Key manager configuration.
                    key_manager: if !matches.is_present("disable-key-manager") {
                        Some(KeyManagerConfiguration {
                            host: matches.value_of("key-manager-host").unwrap().to_owned(),
                            port: value_t!(matches, "key-manager-port", u16).unwrap_or(9003),
                            // TODO: This should be handled by the registry in the future.
                            cert: load_node_certificate(&matches
                                .value_of("key-manager-cert")
                                .unwrap())
                                .expect("unable to load key manager's certificate"),
                        })
                    } else {
                        None
                    },
                }
            },
            test_only: ComputeNodeTestOnlyConfiguration {
                runtime_id: if matches.is_present("test-runtime-id") {
                    Some(value_t_or_exit!(matches, "test-runtime-id", B256))
                } else {
                    None
                },
                fail_after_registration: matches.is_present("test-fail-after-registration"),
            },
        },
        container,
    ).expect("failed to initialize compute node");

    // Start compute node.
    node.start();

    // Start the environment.
    environment.start();
}
