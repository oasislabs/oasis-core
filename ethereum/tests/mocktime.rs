use std::sync::Arc;

extern crate ekiden_common;
extern crate ekiden_epochtime;
extern crate ekiden_ethereum;
#[macro_use(defer)]
extern crate scopeguard;
extern crate grpcio;
extern crate web3;

use ekiden_common::bytes::{B256, H160};
use ekiden_common::entity::Entity;
use ekiden_common::environment::{Environment, GrpcEnvironment};
use ekiden_common::futures::{Future, FutureExt, Stream};
use ekiden_common::testing;
use ekiden_epochtime::interface::{TimeSource, TimeSourceNotifier, EPOCH_INTERVAL};
use ekiden_ethereum::truffle::{deploy_truffle, mine, start_truffle, DEVELOPMENT_ADDRESS};
use ekiden_ethereum::EthereumMockTime;
use web3::api::Web3;
use web3::transports::WebSocket;

#[test]
fn mocktime_integration() {
    testing::try_init_logging();

    let grpc_environment = grpcio::EnvBuilder::new().build();
    let env = Arc::new(GrpcEnvironment::new(grpc_environment));

    // Spin up truffle.
    let mut truffle = start_truffle(env!("CARGO_MANIFEST_DIR"));
    defer! {{
        drop(truffle.kill());
    }};

    // Connect to truffle.
    let (handle, transport) = WebSocket::new("ws://localhost:9545").unwrap();
    let client = Web3::new(transport.clone());

    // Make sure our contracts are deployed.
    let addresses = deploy_truffle(env!("CARGO_MANIFEST_DIR"));
    let address = addresses
        .get("MockEpoch")
        .expect("could not find contract address");

    // Run a driver to make some background transactions such that things confirm.
    env.spawn(mine(transport).discard());

    // Initialize the time source.
    let time_source = EthereumMockTime::new(
        Arc::new(client),
        Arc::new(Entity {
            id: B256::zero(),
            eth_address: Some(H160::from_slice(DEVELOPMENT_ADDRESS)),
        }),
        H160::from_slice(&address),
        env.clone(),
    ).unwrap();

    // Ensure that the cache is coherent and contains the default values.
    let (epoch, till) = TimeSource::get_epoch(&time_source).unwrap();
    assert_eq!(epoch, 0);
    assert_eq!(till, EPOCH_INTERVAL);

    // Subscribe to the time source.
    let get_epochs = time_source.watch_epochs().take(1).collect();

    // Set the mock epoch.
    const TEST_EPOCH: u64 = 0xcafedeadfeedface;
    const TEST_TILL: u64 = 86400 - 1;
    let _ = time_source
        .set_mock_time(TEST_EPOCH, TEST_TILL)
        .wait()
        .unwrap();
    // Note: The cache may still be stale at this point, so check the
    // notifications first.

    // Ensure that the notification happened as expected from the set_mock_time.
    let epochs = get_epochs.wait().unwrap();
    assert_eq!(epochs.len(), 1);
    assert_eq!(epochs[0], TEST_EPOCH);

    // Ensure the local cached epoch/till values were updated.
    let (epoch, till) = TimeSource::get_epoch(&time_source).unwrap();
    assert_eq!(epoch, TEST_EPOCH);
    assert_eq!(till, TEST_TILL);

    drop(handle);
}
