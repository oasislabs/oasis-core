#!/usr/bin/env bash
#
# Run consensus benchmark and plot results.
#
# Outputs are saved to the current working directory:
#     conbench-data.TS.txt -- raw output with benchmark results from conbench
#     conbench-tps.TS.png  -- transactions per second
#     conbench-avg-submit-time.TS.png -- average time required to submit txn
#     conbench-both.TS.png -- both TPS and avg time on same graph
#     conbench-block-size.TS.png -- block sizes (min/avg/max)
#     conbench-block-sizes.TS.png -- 3D graph of block sizes per num of accounts
#     conbench-block-sizes-bytes.TS.png -- 3D graph of block sizes in bytes
#     conbench-block-times.TS.png -- 3D graph of time between blocks per #accts
#     conbench-N.prof -- if profiling enabled, profile output for N accounts
#     conbench-N.block.prof -- if profiling enabled, blocking profile output
#     conbench-N.mutex.prof -- if profiling enabled, mutex contention profile
#
# The TS in filenames above represents the timestamp when the script was run.
#
# Most interesting output files are probably conbench-both.TS.png,
# conbench-block-size.TS.png, and conbench-block-sizes.TS.png.
#
# The environment variable OASIS_NODE_GRPC_ADDR should be set to the node's
# GRPC address, e.g. "unix:/tmp/foo/net-runner/network/client-0/internal.sock".
#
# If you want to run this script with a net runner network:
#     conbench-plot.sh --use_test_entity
# Alternatively, you can run the "conbench-test.sh" script, which also sets up
# a default net runner network for you.
#
# If you want to run this script on a real network:
#     conbench-plot.sh --signer.dir /path/to/your/entity/files/dir
#
# If you want to profile runs, edit the PROF variable below and make sure the
# node is run with the additional argument `--pprof.bind 127.0.0.1:10101`.
# If using the test runner and the conbench-test.sh, add the argument to the
# list in consensusValidator() in go/oasis-test-runner/oasis/args.go.
#

set -o errexit -o nounset -o pipefail
trap "exit 1" INT

# Output file names.
NOW=`date +%Y%m%d-%H%M%S`
RAW_DATA="conbench-data.${NOW}.txt"
TPS_PLOT="conbench-tps.${NOW}.png"
ST_PLOT="conbench-avg-submit-time.${NOW}.png"
BOTH_PLOT="conbench-both.${NOW}.png"
BS_PLOT="conbench-block-size.${NOW}.png"
BSS_PLOT="conbench-block-sizes.${NOW}.png"
BSSB_PLOT="conbench-block-sizes-bytes.${NOW}.png"
BTS_PLOT="conbench-block-times.${NOW}.png"
MATPS_PLOT="conbench-max-avg-tps.${NOW}.png"

# Get the root directory of the repository.
ROOT="$(cd $(dirname $0)/../../../; pwd -P)"

# ANSI escape codes to brighten up the output.
RED=$'\e[31;1m'
GRN=$'\e[32;1m'
OFF=$'\e[0m'


CONBENCH="${ROOT}/go/extra/conbench/conbench"

# Check if we have all the tools we need.
if [[ "$(which gnuplot)" == "" ]]; then
	printf "${RED}ERROR: gnuplot not installed.  Install it and try again.${OFF}\n"
	exit 1
fi
if [[ ! -x "${CONBENCH}" ]]; then
	printf "${RED}ERROR: conbench command isn't built.  Run 'make' in '${ROOT}/go' and try again.${OFF}\n"
fi

TPS_DATA_FILE="$(mktemp -t oasis-conbench-tps-plot-XXXXXXXXXX)"
ST_DATA_FILE="$(mktemp -t oasis-conbench-st-plot-XXXXXXXXXX)"
BS_DATA_FILE="$(mktemp -t oasis-conbench-bs-plot-XXXXXXXXXX)"
BSS_DATA_FILE="$(mktemp -t oasis-conbench-bss-plot-XXXXXXXXXX)"
BSSB_DATA_FILE="$(mktemp -t oasis-conbench-bssb-plot-XXXXXXXXXX)"
BTS_DATA_FILE="$(mktemp -t oasis-conbench-bts-plot-XXXXXXXXXX)"
MATPS_DATA_FILE="$(mktemp -t oasis-conbench-max-avg-tps-plot-XXXXXXXXXX)"

ARGS="$@"

rm -f "${RAW_DATA}"
touch "${RAW_DATA}"

conbench() {
	${CONBENCH} conbench conbench \
		--address ${OASIS_NODE_GRPC_ADDR} \
		${ARGS} \
		--log.level INFO \
		--num_samples 30 \
		$@
}

run_bench() {
	local num_accounts=$1
	local no_plot=$2
	local output="$(mktemp -t oasis-conbench-output-${num_accounts}-XXXXXXXXXX)"

	# Run benchmark.
	printf "${GRN}*** Running benchmark for ${num_accounts} accounts...${OFF}\n"
	conbench --skip_funding --no_wait=60s --num_accounts ${num_accounts} > "${output}"

	local results=$(fgrep 'msg="benchmark finished"' "${output}")
	echo "${results}" | tee -a "${RAW_DATA}"

	local tps=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="transactions_per_second"{print $2}')

	local st=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="avg_submit_time_s"{print $2}')

	local min_bs=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="min_txs_per_block"{print $2}')
	local avg_bs=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="avg_txs_per_block"{print $2}')
	local max_bs=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="max_txs_per_block"{print $2}')

	local bss=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="block_sizes"{print $2}' | tr -d '"')

	local bssb=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="block_sizes_bytes"{print $2}' | tr -d '"')

	local bts=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="block_delta_t_s"{print $2}' | tr -d '"')

	local matps=$(echo "${results}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="max_avg_tps"{print $2}')

	rm "${output}"

	if [[ "${no_plot}" == "no_plot" ]]; then
		return
	fi

	echo "${num_accounts} ${tps}" >> "${TPS_DATA_FILE}"
	echo "${num_accounts} ${st}" >> "${ST_DATA_FILE}"
	echo "${num_accounts} ${min_bs} ${avg_bs} ${max_bs}" >> "${BS_DATA_FILE}"

	local blk=0
	for bs in ${bss}
	do
		echo "${num_accounts} ${blk} ${bs}" >> "${BSS_DATA_FILE}"
		blk=$((blk+1))
	done

	blk=0
	for bsb in ${bssb}
	do
		echo "${num_accounts} ${blk} ${bsb}" >> "${BSSB_DATA_FILE}"
		blk=$((blk+1))
	done

	blk=0
	for bt in ${bts}
	do
		echo "${num_accounts} ${blk} ${bt}" >> "${BTS_DATA_FILE}"
		blk=$((blk+1))
	done

	echo "${num_accounts} ${matps}" >> "${MATPS_DATA_FILE}"
}

ACCT="10, 50, 100, 175, 250, 325, 425, 500, 650, 800, 900"

ACCTN=$(echo "${ACCT}" | tr -d ',')
MAX_ACCTS=$(echo "${ACCTN}" | tr ' ' '\n' | sort -nr | head -1)
NUM_ACCT_RUNS=$(echo "${ACCTN}" | wc -w)

# Set this to a list of runs you wish to profile (e.g. "175 500 800")
# or to the string "no" to disable.
PROF="no"
if [[ "${PROF}" != "no" ]]; then
	NUM_PROF_RUNS=$(echo "${PROF}" | wc -w)
else
	NUM_PROF_RUNS=0
fi

NUM_RUNS=$(( NUM_ACCT_RUNS + NUM_PROF_RUNS ))

printf "${GRN}*** Funding ${MAX_ACCTS} accounts for ${NUM_RUNS} runs...${OFF}\n"
# The gas price is artificially inflated to provide enough tokens to pay fees
# for all the runs.
conbench --num_accounts ${MAX_ACCTS} --gas_price ${NUM_RUNS} --fund_and_exit

for a in ${ACCTN}
do
	run_bench $a plot
	if grep -Fow "$a" <<< "${PROF}"; then
		# Go's pprof server doesn't seem to have a way to start/stop profiling,
		# it can only do a N second run, which is unfortunate.
		#
		# To work around this, we do a normal run first, then parse how many
		# seconds it needed, round it up to the nearest integer, and finally
		# do a second run, profiling for that number of seconds.  Ugh.

		seconds=$(fgrep 'msg="benchmark finished" num_accounts='$a "${RAW_DATA}" | sed -r 's/[[:alnum:]_]+=/\n&/g' | awk -F= '$1=="bench_duration_s"{print $2}' | awk '{print int($1+0.5)}')

		printf "${GRN}*** Re-running benchmark for ${a} accounts with profiling (${seconds} s)...${OFF}\n"

		curl -so conbench-$a.prof 'http://127.0.0.1:10101/debug/pprof/profile?seconds='${seconds} &
		run_bench $a no_plot
		curl -so conbench-$a.block.prof 'http://127.0.0.1:10101/debug/pprof/block'
		go tool pprof -png -lines -noinlines conbench-$a.block.prof > conbench-$a.block.png
		curl -so conbench-$a.mutex.prof 'http://127.0.0.1:10101/debug/pprof/mutex'
		go tool pprof -png -lines -noinlines conbench-$a.mutex.prof > conbench-$a.mutex.png
	fi
done


# Plot TPS graph.
gnuplot <<- EOF
set title "Transactions per second"
set xlabel "Number of parallel accounts"
set xtics (${ACCT})
set ylabel "transactions/s" textcolor lt 1
set autoscale y
set grid
set term png
set output "${TPS_PLOT}"
plot '${TPS_DATA_FILE}' using 1:2 with linespoint notitle
EOF

# Plot avg submit time graph.
gnuplot <<- EOF
set title "Average SubmitTx time"
set xlabel "Number of parallel accounts"
set xtics (${ACCT})
set ylabel "SubmitTx time [s]" textcolor lt 1
set autoscale y
set grid
set term png
set output "${ST_PLOT}"
plot '${ST_DATA_FILE}' using 1:2 with linespoint notitle
EOF

# Plot both on a single graph.
gnuplot <<- EOF
set title "Transactions per second and average SubmitTx time"
set xlabel "Number of parallel accounts"
set xtics (${ACCT})
set ylabel "transactions/s" textcolor lt 1
set y2label "SubmitTx time [s]" textcolor lt 2
set y2tics nomirror
set autoscale y
set autoscale y2
set grid
set term png
set output "${BOTH_PLOT}"
plot '${TPS_DATA_FILE}' using 1:2 axes x1y1 with linespoint notitle, '${ST_DATA_FILE}' using 1:2 axes x1y2 with linespoint notitle
EOF

# Plot block sizes (min/avg/max).
gnuplot <<- EOF
set title "Block size"
set xlabel "Number of parallel accounts"
set xtics (${ACCT})
set ylabel "Block size [number of transactions]"
set autoscale y
set grid
set key left top
set term png
set output "${BS_PLOT}"
plot '${BS_DATA_FILE}' using 1:2 with linespoint title "min", '${BS_DATA_FILE}' using 1:3 with linespoint title "avg", '${BS_DATA_FILE}' using 1:4 with linespoint title "max"
EOF

# Plot block sizes (number of transactions per block per number of accounts).
gnuplot <<- EOF
set title "Block size"
set xlabel "Number of parallel accounts" rotate parallel offset 0,-2,0
set xtics (${ACCT}) offset 0,-1,0
set ylabel "Block number" rotate parallel offset 0,-1,0
set zlabel "Block size [number of transactions]" rotate parallel offset 1,0,0
set ticslevel 0
set autoscale y
set autoscale z
set grid
set term png
set output "${BSS_PLOT}"
set palette defined (0 "red", 1 "yellow", 2 "cyan", 3 "blue", 4 "dark-violet")
splot '${BSS_DATA_FILE}' with impulses lw 2 lc palette notitle
EOF

# Plot block sizes in bytes.
gnuplot <<- EOF
set title "Block size in bytes"
set xlabel "Number of parallel accounts" rotate parallel offset 0,-2,0
set xtics (${ACCT}) offset 0,-1,0
set ylabel "Block number" rotate parallel offset 0,-1,0
set zlabel "Block size [bytes]" rotate parallel
set ticslevel 0
set autoscale y
set autoscale z
set grid
set term png
set output "${BSSB_PLOT}"
set palette defined (0 "red", 1 "yellow", 2 "cyan", 3 "blue", 4 "dark-violet")
splot '${BSSB_DATA_FILE}' with impulses lw 2 lc palette notitle
EOF

# Plot time between blocks.
gnuplot <<- EOF
set title "Time between blocks"
set xlabel "Number of parallel accounts" rotate parallel offset 0,-2,0
set xtics (${ACCT}) offset 0,-1,0
set ylabel "Block number" rotate parallel offset 0,-1,0
set zlabel "Time delta to previous block [s]" rotate parallel offset 1,0,0
set ticslevel 0
set autoscale y
set autoscale z
set grid
set term png
set output "${BTS_PLOT}"
set palette defined (0 "dark-violet", 1 "blue", 2 "cyan", 3 "yellow", 4 "red")
splot '${BTS_DATA_FILE}' with impulses lw 2 lc palette notitle
EOF

# Plot max avg TPS graph.
gnuplot <<- EOF
set title "Maximum average transactions per second"
set xlabel "Number of parallel accounts"
set xtics (${ACCT})
set ylabel "transactions/s" textcolor lt 1
set autoscale y
set grid
set term png
set output "${MATPS_PLOT}"
plot '${MATPS_DATA_FILE}' using 1:2 with linespoint notitle
EOF


rm "${TPS_DATA_FILE}" "${ST_DATA_FILE}" "${BS_DATA_FILE}" "${BSS_DATA_FILE}" "${BSSB_DATA_FILE}" "${BTS_DATA_FILE}" "${MATPS_DATA_FILE}"

printf "${GRN}*** Refunding original funding account...${OFF}\n"
conbench --num_accounts ${MAX_ACCTS} --refund_and_exit

printf "${GRN}*** Benchmarks completed.${OFF}\n"
