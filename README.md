# Ekiden

[![CircleCI](https://circleci.com/gh/oasislabs/ekiden/tree/master.svg?style=svg&circle-token=97f633035afbb45f26ed1b2f3f78a1e8e8a5e756)](https://circleci.com/gh/oasislabs/ekiden/tree/master)

## Dependencies

Here is a brief list of system dependencies currently used for development:
- [rustc](https://www.rust-lang.org/en-US/)
- [cargo](http://doc.crates.io/)
- [xargo](https://github.com/japaric/xargo)
- [docker](https://www.docker.com/)
- [protoc](https://github.com/google/protobuf/releases)

## Developing, building and running external contracts

For instructions on building contracts, you should check out the documentation of the
[hello world contract](https://github.com/oasislabs/contract-helloworld).

## Setting up the development environment

The easiest way to build SGX code is to use the provided scripts, which run a Docker
container with all the included tools.

To start the SGX development container:
```bash
$ cargo ekiden shell
```

If you haven't installed the ekiden cargo extension, it relies on the nightly rust toolchain.
```bash
$ rustup install nightly
$ cargo +nightly install --force --path tools ekiden-tools
```

All the following commands should be run in the container and not on
the host.  The actual prompt from the bash shell running in the
container will look like `root@xxxx:/code#' where `xxxx` is the docker
container id; in the text below, we will just use `#`.

## Building core contracts

For building contracts we have our own Cargo extension which should be installed:
```bash
# cargo install --force --path tools ekiden-tools
```

The following examples use the key manager and token contracts, but the process is the
same for any contract. To build the key manager (required by all other contracts):
```bash
# cd contracts/key-manager
# cargo ekiden build-contract
```

The built contract will be stored under `target/contract/ekiden-key-manager.so`.

To build the token contract:
```bash
# cd contracts/token
# cargo ekiden build-contract
```

The built contract will be stored under `target/contract/token.so`.

## Running a contract

You need to run multiple Ekiden services, so it is recommended to run each of these in a
separate container shell, attached to the same container. The following examples use the
token contract, but the process is the same for any contract.

To start the dummy consensus node:
```bash
# cargo run -p ekiden-consensus -- -x
```

The `-x` flag tells the consensus node to not depend on Tendermint.

To start the compute node for the key manager contract:
```bash
# cargo run -p ekiden-compute -- \
    -p 9003 \
    --disable-key-manager \
    --no-persist-identity \
    target/contract/ekiden-key-manager.so
```

To start the compute node for the token contract:
```bash
# cargo run -p ekiden-compute -- \
    --no-persist-identity \
    target/contract/token.so
```

The contract's compute node will listen on `127.0.0.1` (loopback), TCP port `9001` by default.

Development notes:

* If you are developing a contract and changing things, be sure to either use the `--no-persist-identity` flag or remove the referenced enclave identity file (e.g., `/tmp/token.identity.pb`). Otherwise the compute node will fail to start as it will be impossible to unseal the old identity. For more information about the content of enclave identity check [enclave identity documentation](docs/enclave-identity.md#state).
* Also, when the contract hash changes, the contract will be unable to decrypt any old state as the key manager will give it fresh keys. So be sure to also clear (if you are using a Tendermint node) and restart the consensus node.

## Running tests and benchmarks

To run all tests (some should be skipped due to compile errors):
```bash
# cargo test --all \
    --exclude ekiden-untrusted \
    --exclude ekiden-enclave-untrusted \
    --exclude ekiden-rpc-untrusted \
    --exclude ekiden-db-untrusted \
    --exclude ekiden-consensus \
    -- --test-threads 1
```

## Developing

We welcome anyone to fork and submit a pull request! Please make sure to run `rustfmt` before submitting.

```bash
# cargo fmt
```

## Packages
- `core`: Core external-facing libraries (aggregates `common`, `enclave`, `rpc`, `db`, etc.)
- `common`: Common functionality like error handling
- `enclave`: Enclave loader and identity attestation
- `rpc`: RPC functionality for use in enclaves
- `db`: Database functionality for use in enclaves
- `compute`: Ekiden compute node
- `consensus`: Ekiden consensus node
- `contracts`: Core contracts (`key-manager`, `token`)
- `tools`: Build tools
- `scripts`: Bash scripts for development
