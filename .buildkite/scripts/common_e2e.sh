################################
# Common functions for E2E tests
################################

# Temporary test base directory.
TEST_BASE_DIR=$(realpath ${TEST_BASE_DIR:-$(mktemp -d --tmpdir ekiden-e2e-XXXXXXXXXX)})

# Path to Ekiden root.
EKIDEN_ROOT_PATH=${EKIDEN_ROOT_PATH:-${WORKDIR}}
# Path to the Ekiden node.
EKIDEN_NODE=${EKIDEN_NODE:-${EKIDEN_ROOT_PATH}/go/ekiden/ekiden}
# Path to the runtime loader.
EKIDEN_RUNTIME_LOADER=${EKIDEN_RUNTIME_LOADER:-${EKIDEN_ROOT_PATH}/target/debug/ekiden-runtime-loader}
# TEE hardware (optional).
EKIDEN_TEE_HARDWARE=${EKIDEN_TEE_HARDWARE:-""}
# Runtime identifier.
EKIDEN_RUNTIME_ID=${EKIDEN_RUNTIME_ID:-"0000000000000000000000000000000000000000000000000000000000000000"}
# Keymanager runtime identifier.
EKIDEN_KM_RUNTIME_ID=${EKIDEN_KM_RUNTIME_ID:-"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}
# SGX MRSIGNER used to sign enclaves (default is the Fortanix test key).
EKIDEN_MRSIGNER=${EKIDEN_MRSIGNER:-"9affcfae47b848ec2caf1c49b4b283531e1cc425f93582b36806e52a43d78d1a"}

# Run a Tendermint validator committee.
#
# Sets:
#   EKIDEN_COMMITTEE_DIR
#   EKIDEN_GENESIS_FILE
#   EKIDEN_IAS_PROXY_PORT
#   EKIDEN_IAS_PROXY_CERT
#   EKIDEN_EPOCHTIME_BACKEND
#   EKIDEN_VALIDATOR_SOCKET
#   EKIDEN_CLIENT_SOCKET
#   EKIDEN_ENTITY_DESCRIPTOR
#   EKIDEN_ENTITY_PRIVATE_KEY
#
# Optional named arguments:
#
#   epochtime_backend - epochtime backend (default: tendermint)
#   id - commitee identifier (default: 1)
#   replica_group_size - runtime replica group size (default: 2)
#   replica_group_backup_size - runtime replica group backup size (default: 1)
#   storage_group_size - number of storage nodes for the runtime (default: 2)
#
run_backend_tendermint_committee() {
    # Optional arguments with default values.
    local epochtime_backend="tendermint"
    local id=1
    local replica_group_size=2
    local replica_group_backup_size=1
    local storage_group_size=2
    local roothash_genesis_blocks=""
    local nodes=3
    local runtime_genesis=""
    local restore_genesis_file=""
    # Load named arguments that override defaults.
    local "${@}"

    local committee_dir=${TEST_BASE_DIR}/committee-${id}
    local base_datadir=${committee_dir}/committee-data
    local validator_files=""
    local entity_node_files=""

    # Provision the entity for everything.
    local entity_dir=${committee_dir}/entity
    local km_file=${entity_dir}/km_status.json
    if [[ -z "${restore_genesis_file}" ]]; then
        # Keep the existing entity if restoring state from genesis file.
        rm -Rf ${entity_dir}

        # If not restoring from genesis file, provision everything.
        ${EKIDEN_NODE} \
            registry entity init \
            --entity.debug.allow_entity_signed_nodes \
            --datadir ${entity_dir}

        # Provision the validators.
        for idx in $(seq 1 $nodes); do
            local datadir=${base_datadir}-${idx}
            rm -rf ${datadir}

            let port=(idx-1)+26656
            ${EKIDEN_NODE} \
                registry node init \
                --datadir ${datadir} \
                --entity ${entity_dir} \
                --node.consensus_address 127.0.0.1:${port} \
                --node.expiration 1000000 \
                --node.role validator \
                --node.is_self_signed
            validator_files="$validator_files --node=${datadir}/node_genesis.json"
            entity_node_files="$entity_node_files --entity.node.descriptor=${datadir}/node_genesis.json"
        done

        # Update the entity descriptor to include the node IDs of the
        # self-signed nodes.
        ${EKIDEN_NODE} \
            registry entity update \
            --entity.debug.allow_entity_signed_nodes \
            ${entity_node_files} \
            --datadir ${entity_dir}

        # Provision the key manager runtime.
        if [[ "${EKIDEN_KM_MRENCLAVE:-}" == "" ]]; then
            echo "ERROR: Key manager MRENCLAVE not configured, did you use run_test?"
            exit 1
        fi
        ${EKIDEN_NODE} \
            registry runtime init_genesis \
            --runtime.id ${EKIDEN_KM_RUNTIME_ID} \
            ${EKIDEN_TEE_HARDWARE:+--runtime.tee_hardware ${EKIDEN_TEE_HARDWARE}} \
            ${EKIDEN_TEE_HARDWARE:+--runtime.version.enclave ${EKIDEN_MRSIGNER}${EKIDEN_KM_MRENCLAVE}} \
            --runtime.kind keymanager \
            --runtime.genesis.file keymanager_genesis.json \
            --entity ${entity_dir} \
            --datadir ${entity_dir}

        # Create KM policy file, sign it with 3 keys, and generate genesis block status file.
        ${EKIDEN_NODE} \
            keymanager init_policy \
            --keymanager.policy.file "${entity_dir}/km_policy.cbor" \
            --keymanager.policy.id ${EKIDEN_KM_RUNTIME_ID} \
            --keymanager.policy.serial 1 \
            --keymanager.policy.enclave.id "${EKIDEN_MRSIGNER}${EKIDEN_KM_MRENCLAVE}" \
            --keymanager.policy.may.query "${EKIDEN_RUNTIME_ID}=${EKIDEN_MRSIGNER}${EKIDEN_RUNTIME_MRENCLAVE}"

        for k in 1 2 3; do
            ${EKIDEN_NODE} \
                keymanager sign_policy \
                --keymanager.policy.signature.file "${entity_dir}/km_policy.cbor.sign.$k" \
                --keymanager.policy.file "${entity_dir}/km_policy.cbor" \
                --debug.allow_test_keys \
                --keymanager.policy.testkey $k
        done

        ${EKIDEN_NODE} \
            keymanager init_status \
            --keymanager.status.file "${km_file}" \
            --debug.allow_test_keys \
            --keymanager.status.id ${EKIDEN_KM_RUNTIME_ID} \
            --keymanager.policy.file "${entity_dir}/km_policy.cbor" \
            --keymanager.policy.signature.file "${entity_dir}/km_policy.cbor.sign.1" \
            --keymanager.policy.signature.file "${entity_dir}/km_policy.cbor.sign.2" \
            --keymanager.policy.signature.file "${entity_dir}/km_policy.cbor.sign.3"

        # Provision the runtime.
        if [[ "${EKIDEN_RUNTIME_MRENCLAVE:-}" == "" ]]; then
            echo "ERROR: Runtime MRENCLAVE not configured, did you use run_test?"
            exit 1
        fi
        ${EKIDEN_NODE} \
            registry runtime init_genesis \
            --runtime.id ${EKIDEN_RUNTIME_ID} \
            --runtime.replica_group_size ${replica_group_size} \
            --runtime.replica_group_backup_size ${replica_group_backup_size} \
            --runtime.storage_group_size ${storage_group_size} \
            ${runtime_genesis:+--runtime.genesis.state ${runtime_genesis}} \
            ${EKIDEN_TEE_HARDWARE:+--runtime.tee_hardware ${EKIDEN_TEE_HARDWARE}} \
            ${EKIDEN_TEE_HARDWARE:+--runtime.version.enclave ${EKIDEN_MRSIGNER}${EKIDEN_RUNTIME_MRENCLAVE}} \
            --runtime.keymanager ${EKIDEN_KM_RUNTIME_ID} \
            --runtime.kind compute \
            --entity ${entity_dir} \
            --datadir ${entity_dir}
    fi

    # Create the genesis document.
    local genesis_file=${committee_dir}/genesis.json
    rm -Rf ${genesis_file}

    if [[ ! -z "${restore_genesis_file}" ]]; then
        cp ${restore_genesis_file} ${genesis_file}
    else
        ${EKIDEN_NODE} \
            genesis init \
            --genesis.file ${genesis_file} \
            ${EKIDEN_TEE_HARDWARE:+--keymanager ${km_file}} \
            --keymanager.operator ${entity_dir}/entity_genesis.json \
            --entity ${entity_dir}/entity_genesis.json \
            --runtime ${entity_dir}/keymanager_genesis.json \
            --runtime ${entity_dir}/runtime_genesis.json \
            ${roothash_genesis_blocks:+--roothash ${roothash_genesis_blocks}} \
            ${validator_files}
    fi

    # Run the IAS proxy if needed.
    local ias_proxy_port=9001
    local ias_dir=${committee_dir}/ias-proxy
    rm -Rf {$ias_dir}

    if [ "${EKIDEN_TEE_HARDWARE}" == "intel-sgx" ]; then
        # Note: This can just use a real client and watch the
        # registry by setting `--address` and `--ias.wait_runtimes`
        # to the appropriate values.
        #
        # nb: The startup order of things would need to be changed,
        # and the brittle test cases will probably break in mysterious
        # ways.
        if [ "${EKIDEN_UNSAFE_SKIP_AVR_VERIFY}" == "" ]; then
            # TODO: Ensure that IAS credentials are configured.
            ${EKIDEN_NODE} \
                ias proxy \
                --datadir ${ias_dir} \
                --ias.use_genesis \
                --genesis.file ${genesis_file} \
                --ias.auth.cert ${EKIDEN_IAS_CERT} \
                --ias.auth.cert.ca ${EKIDEN_IAS_CERT} \
                --ias.auth.cert.key ${EKIDEN_IAS_KEY} \
                --ias.spid ${EKIDEN_IAS_SPID} \
                --metrics.mode none \
                --log.level debug \
                --log.file ${committee_dir}/ias-proxy.log \
                &
        else
            # Mock, with a high-quality random SPID from random.org
            ${EKIDEN_NODE} \
                ias proxy \
                --datadir ${ias_dir} \
                --ias.use_genesis \
                --genesis.file ${genesis_file} \
                --ias.debug.mock \
                --ias.spid 9b3085a55a5863f7cc66b380dcad0082 \
                --debug.allow_test_keys \
                --metrics.mode none \
                --log.level debug \
                --log.file ${committee_dir}/ias-proxy.log \
                &
        fi

        EKIDEN_IAS_PROXY_ENABLED=1
        EKIDEN_IAS_PROXY_PORT=${ias_proxy_port}
        EKIDEN_IAS_PROXY_CERT=${ias_dir}/ias_proxy_cert.pem
    fi

    # Export some variables so compute workers can find them.
    EKIDEN_COMMITTEE_DIR=${committee_dir}
    EKIDEN_VALIDATOR_SOCKET=${base_datadir}-1/internal.sock
    EKIDEN_GENESIS_FILE=${genesis_file}
    EKIDEN_EPOCHTIME_BACKEND=${epochtime_backend}
    EKIDEN_ENTITY_DESCRIPTOR=${entity_dir}/entity.json
    EKIDEN_ENTITY_PRIVATE_KEY=${entity_dir}/entity.pem

    # Run the seed node.
    if [[ ! -z "${restore_genesis_file}" ]]; then
        run_seed_node 1
    else
        run_seed_node 0
    fi

    # Run the key manager node.
    if [[ ! -z "${restore_genesis_file}" ]]; then
        run_keymanager_node 1
    else
        run_keymanager_node 0
    fi

    # Run the validator nodes.
    for idx in $(seq 1 $nodes); do
        local datadir=${base_datadir}-${idx}

        let tm_port=(idx-1)+26656
        let grpc_debug_port=tm_port+36656

        ${EKIDEN_NODE} \
            --log.level debug \
            --log.format JSON \
            --log.file ${committee_dir}/validator-${idx}.log \
            --grpc.log.verbose_debug \
            --grpc.debug.port ${grpc_debug_port} \
            --epochtime.backend ${epochtime_backend} \
            --epochtime.tendermint.interval 30 \
            ${EKIDEN_BEACON_DETERMINISTIC:+--beacon.debug.deterministic} \
            --metrics.mode none \
            --storage.backend client \
            --consensus.backend tendermint \
            --genesis.file ${genesis_file} \
            --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
            --tendermint.consensus.timeout_commit 250ms \
            --tendermint.debug.addr_book_lenient \
            --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
            --datadir ${datadir} \
            --debug.allow_test_keys \
            ${EKIDEN_TEE_HARDWARE:+--ias.debug.skip_verify} \
            &

        # HACK HACK HACK HACK HACK
        #
        # If you don't attempt to start the Tendermint Prometheus HTTP server
        # (even if it is doomed to fail due to ekiden already listening on the
        # port), and you launch all the validatiors near simultaniously, there
        # is a high chance that at least one of the validators will get upset
        # and start refusing connections.
        sleep 3
    done

    # Run the client node.
    run_client_node 1
}

# Get the runtime binary path.
#
# Optional named arguments:
#   runtime - name of the runtime to use
#
# Sets:
#   EKIDEN_RUNTIME_BINARY
#   EKIDEN_RUNTIME_MRENCLAVE
get_runtime_binary() {
    local runtime=simple-keyvalue
    # Load named arguments that override defaults.
    local "${@}"

    local runtime_target=""
    local runtime_ext=""
    if [[ "${EKIDEN_TEE_HARDWARE}" == "intel-sgx" ]]; then
        runtime_target="x86_64-fortanix-unknown-sgx"
        runtime_ext=".sgxs"
    fi

    EKIDEN_RUNTIME_BINARY=${EKIDEN_ROOT_PATH}/target/${runtime_target}/debug/${runtime}${runtime_ext}
    EKIDEN_RUNTIME_MRENCLAVE=($(sha256sum ${EKIDEN_RUNTIME_BINARY}))
}

# Run a compute node.
#
# Requires that EKIDEN_GENESIS_FILE is set.
# Exits with an error otherwise.
#
# Arguments:
#   id - compute node index
#   runtime - name of the runtime to use
#
# Any additional arguments are passed to the Go node.
run_compute_node() {
    local id=$1
    shift || true
    local runtime=$1
    shift || true
    local extra_args=$*

    # Ensure the genesis file is available.
    if [[ "${EKIDEN_GENESIS_FILE:-}" == "" ]]; then
        echo "ERROR: Genesis file not configured. Did you use run_backend_tendermint_committee?"
        exit 1
    fi

    local data_dir=${EKIDEN_COMMITTEE_DIR}/worker-$id
    rm -rf ${data_dir}
    local log_file=${EKIDEN_COMMITTEE_DIR}/worker-$id.log
    rm -rf ${log_file}
    local out_file=${EKIDEN_COMMITTEE_DIR}/worker-out-$id.log
    rm -rf ${out_file}

    # Prepare keys to ensure deterministic committees.
    if [[ -f "${WORKDIR}/tests/identities/worker-${id}.pem" ]]; then
        mkdir -p ${data_dir}
        chmod 700 ${data_dir}
        cp ${WORKDIR}/tests/identities/worker-${id}.pem ${data_dir}/identity.pem
        chmod 600 ${data_dir}/identity.pem
    fi

    if [[ "${EKIDEN_RUNTIME_BINARY:-}" == "" ]]; then
        echo "ERROR: Runtime binary not configured, did you use run_test?"
        exit 1
    fi

    # Generate port number.
    let client_port=id+11000
    let p2p_port=id+12000
    let tm_port=id+13000

    ${EKIDEN_NODE} \
        --log.level debug \
        --log.format JSON \
        --log.file ${log_file} \
        --grpc.log.verbose_debug \
        --storage.backend cachingclient \
        --storage.cachingclient.file ${data_dir}/storage-cache \
        --epochtime.backend ${EKIDEN_EPOCHTIME_BACKEND} \
        --epochtime.tendermint.interval 30 \
        ${EKIDEN_BEACON_DETERMINISTIC:+--beacon.debug.deterministic} \
        --metrics.mode none \
        --consensus.backend tendermint \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
        --tendermint.consensus.timeout_commit 250ms \
        --tendermint.debug.addr_book_lenient \
        ${EKIDEN_IAS_PROXY_ENABLED:+--ias.proxy_addr 127.0.0.1:${EKIDEN_IAS_PROXY_PORT}} \
        ${EKIDEN_IAS_PROXY_ENABLED:+--ias.tls ${EKIDEN_IAS_PROXY_CERT}} \
        ${EKIDEN_TEE_HARDWARE:+--ias.debug.skip_verify} \
        --worker.compute.enabled \
        --worker.compute.backend sandboxed \
        --worker.compute.runtime_loader ${EKIDEN_RUNTIME_LOADER} \
        --worker.compute.runtime.binary ${EKIDEN_RUNTIME_BINARY} \
        ${EKIDEN_TEE_HARDWARE:+--worker.compute.runtime.sgx_ids ${EKIDEN_RUNTIME_ID}} \
        --worker.txnscheduler.enabled \
        --worker.txnscheduler.batching.max_batch_size 1 \
        --worker.merge.enabled \
        --worker.runtime.id ${EKIDEN_RUNTIME_ID} \
        --worker.client.port ${client_port} \
        --worker.p2p.port ${p2p_port} \
        --worker.registration.entity ${EKIDEN_ENTITY_DESCRIPTOR} \
        --worker.registration.private_key ${EKIDEN_ENTITY_PRIVATE_KEY} \
        --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${extra_args} 2>&1 | tee ${out_file} | sed "s/^/[compute-node-${id}] /" &
}

# Run a Byzantine node.
#
# Requires that EKIDEN_GENESIS_FILE is set.
# Exits with an error otherwise.
#
# Arguments:
#   script_name - which Byzantine script to run
#
# Any additional arguments are passed to the Byzantine node.
run_byzantine_node() {
    local script_name=$1
    shift || true
    local extra_args=$*

    # Ensure the genesis file is available.
    if [[ "${EKIDEN_GENESIS_FILE:-}" == "" ]]; then
        echo "ERROR: Genesis file not configured. Did you use run_backend_tendermint_committee?"
        exit 1
    fi

    local data_dir=${EKIDEN_COMMITTEE_DIR}/byzantine
    rm -rf ${data_dir}
    local log_file=${EKIDEN_COMMITTEE_DIR}/worker-byzantine.log
    rm -rf ${log_file}
    local out_file=${EKIDEN_COMMITTEE_DIR}/worker-out-byzantine.log
    rm -rf ${out_file}

    # Prepare keys to ensure deterministic committees.
    if [[ -f "${WORKDIR}/tests/identities/byzantine.pem" ]]; then
        mkdir -p ${data_dir}
        chmod 700 ${data_dir}
        cp ${WORKDIR}/tests/identities/byzantine.pem ${data_dir}/identity.pem
        chmod 600 ${data_dir}/identity.pem
    fi

    # Generate port number.
    let p2p_port=12004
    let tm_port=13004

    ${EKIDEN_NODE} debug byzantine ${script_name} \
        --log.level debug \
        --log.format JSON \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
        --tendermint.consensus.timeout_commit 250ms \
        --tendermint.debug.addr_book_lenient \
        --worker.p2p.port ${p2p_port} \
        --worker.registration.entity ${EKIDEN_ENTITY_DESCRIPTOR} \
        --worker.registration.private_key ${EKIDEN_ENTITY_PRIVATE_KEY} \
        --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${extra_args} 2>&1 | tee ${out_file} | python -u ../private-ops/untracked/color-log.py | sed "s/^/[byzantine] /" &
}

# Run a storage node.
#
# Requires that EKIDEN_GENESIS_FILE is set.
# Exits with an error otherwise.
#
# Arguments:
#   id - storage node index
#
# Optional named arguments:
#   clear_storage - clear storage node dir (default: 1)
#
# Output environment variables:
#   EKIDEN_LAST_NODE_DATA_DIR - the data directory for the last node run
#
run_storage_node() {
    # Process arguments.
    local id=$1
    shift || true

    # Optional arguments with default values.
    local clear_storage=1
    local extra_args=""
    # Load named arguments that override defaults.
    local "$@"

    # Ensure the genesis file is available.
    if [[ "${EKIDEN_GENESIS_FILE:-}" == "" ]]; then
        echo "ERROR: Genesis file not configured. Did you use run_backend_tendermint_committee?"
        exit 1
    fi

    local data_dir=${EKIDEN_COMMITTEE_DIR}/storage-$id
    if [[ $clear_storage == 1 ]]; then
        rm -rf ${data_dir}
    fi
    EKIDEN_LAST_NODE_DATA_DIR="${data_dir}"
    local log_file=${EKIDEN_COMMITTEE_DIR}/storage-$id.log
    rm -rf ${log_file}

    # Generate port numbers.
    let client_port=id+11100
    let p2p_port=id+12100
    let tm_port=id+13100

    ${EKIDEN_NODE} \
        --log.level debug \
        --log.file ${log_file} \
        --grpc.log.verbose_debug \
        --epochtime.backend ${EKIDEN_EPOCHTIME_BACKEND} \
        --epochtime.tendermint.interval 30 \
        ${EKIDEN_BEACON_DETERMINISTIC:+--beacon.debug.deterministic} \
        --metrics.mode none \
        --storage.backend leveldb \
        --consensus.backend tendermint \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --roothash.tendermint.index_blocks \
        --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
        --tendermint.consensus.timeout_commit 250ms \
        --tendermint.debug.addr_book_lenient \
        --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
        --worker.storage.enabled \
        --worker.client.port ${client_port} \
        --worker.p2p.port ${p2p_port} \
        --worker.registration.entity ${EKIDEN_ENTITY_DESCRIPTOR} \
        --worker.registration.private_key ${EKIDEN_ENTITY_PRIVATE_KEY} \
        --worker.runtime.id ${EKIDEN_RUNTIME_ID} \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${EKIDEN_TEE_HARDWARE:+--ias.debug.skip_verify} \
        $extra_args \
        2>&1 | sed "s/^/[storage-node-${id}] /" &
}

# Run a client node.
#
# Requires that EKIDEN_GENESIS_FILE is set.
# Exits with an error otherwise.
#
# Sets:
#   EKIDEN_CLIENT_SOCKET
#
# Arguments:
#   id - client node index
#
run_client_node() {
    # Process arguments.
    local id=$1
    shift || true

    # Ensure the genesis file is available.
    if [[ "${EKIDEN_GENESIS_FILE:-}" == "" ]]; then
        echo "ERROR: Genesis file not configured. Did you use run_backend_tendermint_committee?"
        exit 1
    fi

    local data_dir=${EKIDEN_COMMITTEE_DIR}/client-$id
    rm -rf ${data_dir}
    local log_file=${EKIDEN_COMMITTEE_DIR}/client-$id.log
    rm -rf ${log_file}

    # Export some variables.
    EKIDEN_CLIENT_SOCKET=${data_dir}/internal.sock

    # Generate port numbers.
    let tm_port=id+13200

    ${EKIDEN_NODE} \
        --log.level debug \
        --log.format JSON \
        --log.file ${log_file} \
        --grpc.log.verbose_debug \
        --epochtime.backend ${EKIDEN_EPOCHTIME_BACKEND} \
        --epochtime.tendermint.interval 30 \
        ${EKIDEN_BEACON_DETERMINISTIC:+--beacon.debug.deterministic} \
        --metrics.mode none \
        --storage.backend cachingclient \
        --storage.cachingclient.file ${data_dir}/storage-cache \
        --consensus.backend tendermint \
        --roothash.tendermint.index_blocks \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
        --tendermint.consensus.timeout_commit 250ms \
        --tendermint.debug.addr_book_lenient \
        --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
        --client.indexer.runtimes ${EKIDEN_RUNTIME_ID} \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${EKIDEN_TEE_HARDWARE:+--ias.debug.skip_verify} \
        2>&1 | sed "s/^/[client-node-${id}] /" &
}

# Wait for a number of compute nodes to register.
#
# Arguments:
#   nodes - number of nodes to wait for
wait_nodes() {
    local nodes=$1

    ${EKIDEN_NODE} debug dummy wait-nodes \
        --address unix:${EKIDEN_VALIDATOR_SOCKET} \
        --nodes $nodes
}

# Set epoch.
#
# Arguments:
#   epoch - epoch to set
set_epoch() {
    local epoch=$1

    ${EKIDEN_NODE} debug dummy set-epoch \
        --address unix:${EKIDEN_VALIDATOR_SOCKET} \
        --epoch $epoch
}

# Get the key manager binary path.
#
# Sets:
#   EKIDEN_KM_BINARY
#   EKIDEN_KM_MRENCLAVE
get_keymanager_binary() {
    local runtime_target=""
    local runtime_ext=""
    if [[ "${EKIDEN_TEE_HARDWARE}" == "intel-sgx" ]]; then
        runtime_target="x86_64-fortanix-unknown-sgx"
        runtime_ext=".sgxs"
    fi

    EKIDEN_KM_BINARY=${EKIDEN_ROOT_PATH}/target/${runtime_target}/debug/ekiden-keymanager-runtime${runtime_ext}
    EKIDEN_KM_MRENCLAVE=($(sha256sum ${EKIDEN_KM_BINARY}))
}

# Run a key manager node.
#
# Required arguments:
#   keep_data_dir - Should the data directory be preserved (1) or not (0)
#
# Require variables:
#   EKIDEN_KM_BINARY - Set by get_keymanager_binary
#
# Any arguments are passed to the key manager node.
run_keymanager_node() {
    local keep_data_dir=$1
    shift
    local extra_args=$*

    local data_dir=${EKIDEN_COMMITTEE_DIR}/key-manager
    if [ "${keep_data_dir}" != "1" ]; then
        rm -rf ${data_dir}
    fi
    local log_file=${EKIDEN_COMMITTEE_DIR}/key-manager.log
    rm -rf ${log_file}

    if [[ "${EKIDEN_KM_BINARY:-}" == "" ]]; then
        echo "ERROR: Key manager binary not configured, did you use run_test?"
        exit 1
    fi

    let tm_port=13900

    ${EKIDEN_NODE} \
        --log.level debug \
        --log.format JSON \
        --log.file ${log_file} \
        --grpc.log.verbose_debug \
        --storage.backend cachingclient \
        --storage.cachingclient.file ${data_dir}/storage-cache \
        --epochtime.backend ${EKIDEN_EPOCHTIME_BACKEND} \
        --epochtime.tendermint.interval 30 \
        ${EKIDEN_BEACON_DETERMINISTIC:+--beacon.debug.deterministic} \
        --metrics.mode none \
        --consensus.backend tendermint \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --tendermint.core.listen_address tcp://0.0.0.0:${tm_port} \
        --tendermint.consensus.timeout_commit 250ms \
        --tendermint.debug.addr_book_lenient \
        ${EKIDEN_IAS_PROXY_ENABLED:+--ias.proxy_addr 127.0.0.1:${EKIDEN_IAS_PROXY_PORT}} \
        ${EKIDEN_IAS_PROXY_ENABLED:+--ias.tls ${EKIDEN_IAS_PROXY_CERT}} \
        ${EKIDEN_TEE_HARDWARE:+--ias.debug.skip_verify} \
        ${EKIDEN_TEE_HARDWARE:+--worker.keymanager.tee_hardware ${EKIDEN_TEE_HARDWARE}} \
        --worker.registration.entity ${EKIDEN_ENTITY_DESCRIPTOR} \
        --worker.registration.private_key ${EKIDEN_ENTITY_PRIVATE_KEY} \
        --worker.client.port 9003 \
        --worker.keymanager.enabled \
        --worker.keymanager.runtime.loader ${EKIDEN_RUNTIME_LOADER} \
        --worker.keymanager.runtime.binary ${EKIDEN_KM_BINARY} \
        --worker.keymanager.runtime.id ${EKIDEN_KM_RUNTIME_ID} \
        --worker.keymanager.may_generate \
        --tendermint.seeds "${EKIDEN_SEED_NODE_ID}@127.0.0.1:${EKIDEN_SEED_NODE_PORT}" \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${extra_args} 2>&1 | sed "s/^/[key-manager] /" &
}

# Run a seed node.
#
# Requires that EKIDEN_GENESIS_FILE set.
# Exits with an error otherwise.
#
# Sets:
#   EKIDEN_SEED_NODE_ID
#   EKIDEN_SEED_NODE_PORT
#
# Any arguments are passed to the Go node.
run_seed_node() {
    local keep_data_dir=$1
    shift
    local extra_args=$*

    # Ensure the genesis file is available.
    if [[ "${EKIDEN_GENESIS_FILE:-}" == "" ]]; then
        echo "ERROR: Tendermint genesis and/or storage port file not configured. Did you use run_backend_tendermint_committee?"
        exit 1
    fi

    local data_dir=${EKIDEN_COMMITTEE_DIR}/seed-$id
    if [ "${keep_data_dir}" != "1" ]; then
        rm -rf ${data_dir}
    fi
    local log_file=${EKIDEN_COMMITTEE_DIR}/seed-$id.log
    rm -rf ${log_file}

    # Generate port number.
    let EKIDEN_SEED_NODE_PORT=id+23000

    ${EKIDEN_NODE} \
        --log.level info \
        --log.format JSON \
        --log.file ${log_file} \
        --metrics.mode none \
        --genesis.file ${EKIDEN_GENESIS_FILE} \
        --tendermint.core.listen_address tcp://0.0.0.0:${EKIDEN_SEED_NODE_PORT} \
        --tendermint.seed_mode \
        --tendermint.debug.addr_book_lenient \
        --datadir ${data_dir} \
        --debug.allow_test_keys \
        ${extra_args} 2>&1 | sed "s/^/[seed-node-${id}] /" &

    # 'show-node-id' relies on key file to be present.
    while [ ! -f "${data_dir}/identity_pub.pem" ]
    do
      echo "Waiting for seed node to start..."
      sleep 2
    done

    EKIDEN_SEED_NODE_ID=$(${EKIDEN_NODE} debug tendermint show-node-id \
        --datadir ${data_dir})
    export EKIDEN_SEED_NODE_ID
    export EKIDEN_SEED_NODE_PORT
}

# Run a basic client.
#
# Sets EKIDEN_CLIENT_PID to the PID of the client process.
#
# Required arguments:
#   runtime        - name of the runtime enclave to use (without .so); the
#                    enclave must be available under target/enclave
#   client         - name of the client binary to use (without -client)
#
# All remaining arguments are passed to the client unchanged.
run_basic_client() {
    local runtime=$1
    shift
    local client=$1
    shift
    local extra_args=$*

    local log_file=${EKIDEN_COMMITTEE_DIR}/client.log
    rm -rf ${log_file}

    # Wait for the socket to appear.
    while [ ! -S "${EKIDEN_CLIENT_SOCKET}" ]
    do
      echo "Waiting for internal Ekiden node socket to appear..."
      sleep 1
    done

    ${WORKDIR}/target/debug/${client}-client \
        --node-address unix:${EKIDEN_CLIENT_SOCKET} \
        --runtime-id ${EKIDEN_RUNTIME_ID} \
        ${extra_args} 2>&1 | tee ${log_file} | sed "s/^/[client] /" &
    EKIDEN_CLIENT_PID=$!
}

# Global test counter used for parallelizing jobs.
E2E_TEST_COUNTER=0

# Run a specific test scenario.
#
# Required named arguments:
#
#   name           - unique test name
#   scenario       - function that will start the compute nodes; see the
#                    scenario function section below for details
#   backend_runner - function that will prepare and run the backend services
#   runtime        - name of the runtime enclave to use (without .so); the
#                    enclave must be available under target/enclave
#
# Optional named arguments:
#
#   post_km_hook    - function that will run after the key manager node
#                     has been started
#   on_success_hook - function that will run after the client successfully
#                     exits (default: assert_basic_success)
#   client_runner   - function that will run the client (default: run_basic_client)
#   client          - name of the client binary to use, without -client (default: none)
#
# Scenario function:
#
# The scenario function defines what will be executed during the test. It will
# receive the following arguments when called:
#
#   runtime - the name of the runtime to use
#
run_test() {
    # Required arguments.
    local name scenario backend_runner runtime
    # Optional arguments with default values.
    local pre_init_hook=""
    local post_km_hook=""
    local on_success_hook="assert_basic_success"
    local client_runner=run_basic_client
    local client_extra_args=""
    local client="none"
    local beacon_deterministic=""
    local restore_genesis_file=""
    local epochtime_backend="tendermint"
    # Load named arguments that override defaults.
    local "${@}"

    # Export EKIDEN_BEACON_DETERMINISTIC setting.
    EKIDEN_BEACON_DETERMINISTIC=${beacon_deterministic}

    # Check if we should run this test.
    if [[ "${TEST_FILTER:-}" == "" ]]; then
        local test_index=$E2E_TEST_COUNTER
        let E2E_TEST_COUNTER+=1 1

        if [[ -n ${BUILDKITE_PARALLEL_JOB+x} ]]; then
            let test_index%=BUILDKITE_PARALLEL_JOB_COUNT 1

            if [[ $BUILDKITE_PARALLEL_JOB != $test_index ]]; then
                echo "Skipping test '${name}' (assigned to different parallel build)."
                return
            fi
        fi
    elif [[ "${TEST_FILTER}" != "${name}" ]]; then
        return
    fi

    echo -e "\n\e[36;7;1mRUNNING TEST:\e[27m ${name}\e[0m\n"

    # Get the paths (and enclave identities) of the various enclaves.
    get_keymanager_binary
    get_runtime_binary runtime=${runtime}

    if [[ "${pre_init_hook}" != "" ]]; then
        $pre_init_hook
    fi

    # Start backend.
    $backend_runner \
        restore_genesis_file=${restore_genesis_file} \
        epochtime_backend=${epochtime_backend}
    sleep 1

    # Run the client.
    $client_runner $runtime $client $client_extra_args
    local client_pid=${EKIDEN_CLIENT_PID:-""}

    # Run post key-manager startup hook.
    if [[ "$post_km_hook" != "" ]]; then
        $post_km_hook
    fi

    # Run scenario.
    $scenario $runtime

    # Wait on the client and check its exit status.
    if [ "${client_pid}" != "" ]; then
        wait ${client_pid}
    fi

    # Run on success hook.
    if [[ "$on_success_hook" != "" ]]; then
        $on_success_hook
    fi

    # Cleanup.
    cleanup
}

####################
# Common assertions.
####################

_assert_worker_logs_contain() {
    set +ex
    required_code=$1
    pattern=$2
    msg=$3

    grep -q "${pattern}" ${EKIDEN_COMMITTEE_DIR}/worker-*.log
    if [[ $? != $required_code ]]; then
        echo -e "\e[31;1mTEST ASSERTION FAILED: ${msg}\e[0m"
        set -ex
        exit 1
    fi
    set -ex
}

assert_worker_logs_contain() {
    _assert_worker_logs_contain 0 "$1" "$2"
}

assert_worker_logs_not_contain() {
    _assert_worker_logs_contain 1 "$1" "$2"
}

# Assert that there were no panics.
assert_no_panics() {
    assert_worker_logs_not_contain "panic:" "Panics detected during run."
    assert_worker_logs_not_contain "CONSENSUS FAILURE" "Consensus failure detected during run."
}

# Assert that there are were no round timeouts.
assert_no_round_timeouts() {
    assert_worker_logs_not_contain "FireTimer" "Round timeouts detected during run."
    assert_worker_logs_not_contain "round failed" "Failed rounds detected during run."
}

# Assert that there are were round timeouts.
assert_round_timeouts() {
    assert_worker_logs_contain "FireTimer" "Round timeouts NOT detected during run."
    assert_worker_logs_not_contain "round failed" "Failed rounds detected during run."
}

# Assert that there were no discrepancies.
assert_no_discrepancies() {
    assert_worker_logs_not_contain "discrepancy detected" "Discrepancy detected during run."
}

# Assert that there were no compute discrepancies.
assert_no_compute_discrepancies() {
    assert_worker_logs_not_contain "compute discrepancy detected" "Compute discrepancy detected during run."
}

# Assert that there were compute discrepancies.
assert_compute_discrepancies() {
    assert_worker_logs_contain "compute discrepancy detected" "Compute discrepancy NOT detected during run."
}

# Assert that there were no merge discrepancies.
assert_no_merge_discrepancies() {
    assert_worker_logs_not_contain "merge discrepancy detected" "Merge discrepancy detected during run."
}

# Assert that there were merge discrepancies.
assert_merge_discrepancies() {
    assert_worker_logs_contain "merge discrepancy detected" "Merge discrepancy NOT detected during run."
}

# Assert that all computations ran successfully without hiccups.
assert_basic_success() {
    assert_no_panics
    assert_no_round_timeouts
    assert_no_discrepancies
}
