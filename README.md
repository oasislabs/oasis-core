# Ekiden

[![CircleCI](https://circleci.com/gh/oasislabs/ekiden/tree/master.svg?style=svg&circle-token=97f633035afbb45f26ed1b2f3f78a1e8e8a5e756)](https://circleci.com/gh/oasislabs/ekiden/tree/master)


## Developing, building and using Ekiden

If you are interested in developing contracts in the ekiden runtime, you should check out the documentation of the
[hello world contract](https://github.com/oasislabs/contract-helloworld).

## Developing and building the Ekiden system

The canonical development environment is defined by our development Docker container.
This is done for two reasons: First it ensures good code hygene by replicating the expectations of an SGX development environment.
Second, it provides the expected dependencies and tools, which are relatively complex to replicate in a local environment.

On MacOS, the ekiden docker setup needs more than 10 GB memory and at least 4 CPU cores. Make sure to have the correct docker settings before starting the ekiden container or you might experience build failures.

To enter the ekiden docker environment, use the ekiden cargo extension. It relies on the nightly rust toolchain.
```
$ rustup install nightly
$ cargo +nightly install --force --path tools
```

To start the development container:
```
$ cargo ekiden shell
```

All the following commands should be run in this container and not on
your host.  The actual prompt from the bash shell running in the
container will look like `root@xxxx:/code#` where `xxxx` is the docker
container id; in the text below, we will just use `#`.

## Building core contracts

Starting directory is
```
# cd /code
```

For building contracts we have our own Cargo extension which should be installed:
```
# cargo install --force --path tools
```

To build the token contract:
```
# cd contracts/token
# cargo ekiden build-contract
```

The built contract will be stored under `target/contract/token.so`.

To build the client token contract:
```
# cd clients/token
# cargo build
```

## Running a contract

Starting directory is
```
# cd /code
```

You need to run multiple Ekiden services, so it is recommended to run each of these in a
separate container shell, attached to the same container. The following examples use the
token contract, but the process is the same for any contract.

To start the shared dummy node:
```
# ./target/debug/ekiden-node-dummy --time-source mockrpc --storage-backend dummy
```

To start the compute node for the token contract (you need to start two):
```
# cargo run -p ekiden-compute -- \
    --time-source-notifier system \
    --entity-ethereum-address 0000000000000000000000000000000000000000 \
    --batch-storage immediate_remote \
    --no-persist-identity \
    target/contract/token.so
```

After starting the nodes, to manually advance the epoch in the shared dummy node:
```
# ./target/debug/ekiden-node-dummy-controller set-epoch --epoch 1
```

The contract's compute node will listen on `127.0.0.1` (loopback), TCP port `9001` by default.

Development notes:

* If you are developing a contract and changing things, be sure to either use the `--no-persist-identity` flag or remove the referenced enclave identity file (e.g., `/tmp/token.identity.pb`). Otherwise the compute node will fail to start as it will be impossible to unseal the old identity. For more information about the content of enclave identity check [enclave identity documentation](docs/enclave-identity.md#state).

## Running tests and benchmarks

To run all tests (some should be skipped due to compile errors):
```
# cargo test --all \
    --exclude ekiden-untrusted \
    --exclude ekiden-enclave-untrusted \
    --exclude ekiden-rpc-untrusted \
    --exclude ekiden-db-untrusted \
    --exclude ekiden-contract-untrusted \
    -- --test-threads 1
```

To run end-to-end tests:
```
# ./scripts/test-e2e.sh
```

## Developing

We welcome anyone to fork and submit a pull request! Please make sure to run `rustfmt` before submitting.

```
# cargo fmt
```

## Packages
- `beacon`: Random beacon for preventing predictability
- `common`: Common functionality like error handling
- `compute`: Ekiden compute node
- `consensus`: Ekiden consensus interface and backends
- `contracts`: Example and mangaement code to run in the Ekiden runtime (`key-manager`, `token`)
- `core`: Core external-facing libraries (aggregates `common`, `enclave`, `rpc`, `db`, etc.)
- `db`: Database functionality for use in enclaves
- `di`: Dependency Injection for runtime selection of components
- `docker`: Docker environment definitions
- `enclave`: Enclave loader and identity attestation
- `epochtime`: Time synchronization
- `ethereum`: Contract definitions of the `beacon`, `consensus`, `epochtime`, `registry`, and `stake` components
- `instrumentation`: Metric collection and instrumentation utilities
- `node`: Centralized "backend" for centralized implemnetations of APIs (e.g. a location to use as a pretend AWS)
- `registry`: Management of which hosts are online in the system
- `rpc`: RPC functionality for use in enclaves
- `scheduler`: Algorithms for assigning nodes to committees
- `scripts`: Bash scripts for development
- `stake`: ERC20 integration and API - economics of participation
- `storage`: Persistance and integration with DB and network file stores
- `testnet`: Scripts of deployment and Ops of the system
- `tools`: Build tools
