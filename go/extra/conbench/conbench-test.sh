#!/usr/bin/env bash
#
# Set-up the default oasis-net-runner network and run conbench-plot on it.
#
# You might want to set the cpufreq governor to 'performance' before running
# any benchmarks:
#
#     sudo sh -c 'for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo "performance" > $cpu; done'
#     ./conbench-test.sh
#     sudo sh -c 'for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do echo "powersave" > $cpu; done'
#

set -o nounset -o pipefail -o errexit
trap "exit 1" INT

# Get the root directory of the repository.
ROOT="$(cd $(dirname $0)/../../../; pwd -P)"

# ANSI escape codes to brighten up the output.
GRN=$'\e[32;1m'
OFF=$'\e[0m'

# Paths to various binaries and config files that we need.
OASIS_NET_RUNNER="${ROOT}/go/oasis-net-runner/oasis-net-runner"
OASIS_NODE="${ROOT}/go/oasis-node/oasis-node"

# Kill all dangling processes on exit.
cleanup() {
	printf "${OFF}"
	pkill -P $$ || true
	wait || true
}
trap "cleanup" EXIT

# The base directory for all the node and test env cruft.
# Note: We don't make this under /tmp to prevent running out of RAM.
# Note2: There's a dumb limit to the path length for the UNIX socket, so run
# this script somewhere close to the root of the filesystem.
TEST_BASE_DIR=$(cd `mktemp -p . -d oasis-conbench-XXXXXXXXXX`; pwd -P)

# The oasis-node binary must be in the path for the oasis-net-runner to find it.
export PATH="${PATH}:${ROOT}/go/oasis-node"

# Make sure the open file limit is big enough.
ulimit -n 10240

printf "${GRN}### Starting the test network...${OFF}\n"
${OASIS_NET_RUNNER} \
	--fixture.default.setup_runtimes=false \
	--fixture.default.num_entities=1 \
	--fixture.default.num_validators=4 \
	--fixture.default.disable_supplementary_sanity_checks=true \
	--fixture.default.timeout_commit=100ms \
	--basedir.no_temp_dir \
	--basedir "${TEST_BASE_DIR}" &

export OASIS_NODE_GRPC_ADDR="unix:${TEST_BASE_DIR}/net-runner/network/validator-0/internal.sock"

printf "${GRN}### Waiting for all nodes to register...${OFF}\n"
${OASIS_NODE} debug control wait-nodes \
	--address ${OASIS_NODE_GRPC_ADDR} \
	--nodes 1 \
	--wait

printf "${GRN}### Running benchmark...${OFF}\n"
${ROOT}/go/extra/conbench/conbench-plot.sh --use_test_entity

# Clean up after a successful run.
cleanup
rm -rf "${TEST_BASE_DIR}"

printf "${GRN}### Tests finished.${OFF}\n"
