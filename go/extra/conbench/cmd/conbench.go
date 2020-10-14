package cmd

import (
	"context"
	"crypto"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/drbg"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/mathrand"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	memorySigner "github.com/oasisprotocol/oasis-core/go/common/crypto/signature/signers/memory"
	"github.com/oasisprotocol/oasis-core/go/common/entity"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/quantity"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/control/api"
	"github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common"
	cmdFlags "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	cmdGrpc "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/grpc"
	cmdSigner "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/signer"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
)

const (
	// Number of test accounts to create.
	// This also controls the number of parallel transfers.
	CfgNumAccounts = "num_accounts"

	// Number of samples (transfers) per account.
	CfgNumSamples = "num_samples"

	// Timeout for SubmitTx.
	CfgSubmitTxTimeout = "submit_timeout"

	// Use test entity for funding?
	CfgUseTestEntity = "use_test_entity"

	// CfgNoWait uses SubmitTxNoWait instead of SubmitTx, submits txns for the
	// given amount of time, then stops.
	CfgNoWait = "no_wait"

	// Gas price (should be set to the minimum gas price of validators).
	CfgGasPrice = "gas_price"

	// Only fund accounts and exit (useful for multiple runs of the benchmark,
	// since funding is the slowest part).
	CfgFundAndExit = "fund_and_exit"

	// Only refund funding account and exit (counterpart of the above).
	CfgRefundAndExit = "refund_and_exit"

	// Skip funding accounts.
	CfgSkipFunding = "skip_funding"

	// Seed to use for the DRBG.
	CfgSeed = "seed"

	// Placeholder value for cachedNonce and cachedGas in localAccount struct
	// when they haven't been initialized yet.
	notYetCached = uint64(math.MaxUint64)
)

var (
	logger      = logging.GetLogger("cmd/conbench")
	conbenchCmd = &cobra.Command{
		Use:   "conbench",
		Short: "benchmark consensus layer",
		Long:  "Runs a consensus layer benchmark.",
		RunE:  doRun,
	}
)

type localAccount struct {
	signer      signature.Signer
	addr        staking.Address
	cachedNonce uint64
	cachedGas   uint64
	errorCount  map[error]uint64
}

func transfer(ctx context.Context, cc consensus.ClientBackend, from *localAccount, toAddr staking.Address, amount uint64, noCache, noWait bool) error {
	var err error

	// Get sender's nonce if not yet cached (or if we're ignoring cache).
	nonce := from.cachedNonce
	if nonce == notYetCached || noCache {
		nonce, err = cc.GetSignerNonce(ctx, &consensus.GetSignerNonceRequest{
			AccountAddress: from.addr,
			Height:         consensus.HeightLatest,
		})
		if err != nil {
			from.errorCount[err]++
			return fmt.Errorf("unable to get sender's nonce: %w", err)
		}
		atomic.StoreUint64(&from.cachedNonce, nonce)
	}

	// Construct transfer transaction.
	transfer := staking.Transfer{
		To: toAddr,
	}
	if err = transfer.Amount.FromUint64(amount); err != nil {
		from.errorCount[err]++
		return fmt.Errorf("unable to convert given amount from uint64: %w", err)
	}

	var fee transaction.Fee
	tx := staking.NewTransferTx(nonce, &fee, &transfer)

	// Estimate gas if not yet cached (or if we're ignoring cache).
	gas := from.cachedGas
	if gas == notYetCached || noCache {
		estGas, grr := cc.EstimateGas(ctx, &consensus.EstimateGasRequest{
			Signer:      from.signer.Public(),
			Transaction: tx,
		})
		if grr != nil {
			from.errorCount[grr]++
			return fmt.Errorf("unable to estimate gas: %w", grr)
		}
		gas = uint64(estGas)
		atomic.StoreUint64(&from.cachedGas, gas)
	}

	tx.Fee.Gas = transaction.Gas(gas)
	if err = tx.Fee.Amount.FromUint64(gas * viper.GetUint64(CfgGasPrice)); err != nil {
		from.errorCount[err]++
		return fmt.Errorf("unable to convert fee amount from uint64: %w", err)
	}

	signedTx, err := transaction.Sign(from.signer, tx)
	if err != nil {
		from.errorCount[err]++
		return fmt.Errorf("unable to sign transfer transaction: %w", err)
	}

	// Increment cached nonce.
	atomic.AddUint64(&from.cachedNonce, 1)

	if noWait {
		// Submit transaction, but don't wait for it to be included in a block.
		grr := cc.SubmitTxNoWait(ctx, signedTx)
		if grr != nil {
			from.errorCount[grr]++
		}
		return grr
	}

	// Otherwise, submit and wait for the txn to be included in a block.
	// Submit with timeout to avoid blocking forever if the client node
	// is skipping CheckTx checks.  The timeout should be set large enough
	// for the network to handle the submission.
	timeout := viper.GetDuration(CfgSubmitTxTimeout)
	submissionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err = cc.SubmitTx(submissionCtx, signedTx); err != nil {
		from.errorCount[err]++
		return err
	}
	return nil
}

func refund(ctx context.Context, cc consensus.ClientBackend, sc staking.Backend, from *localAccount, toAddr staking.Address) error {
	// Fetch account info.
	acct, err := sc.Account(ctx, &staking.OwnerQuery{
		Height: consensus.HeightLatest,
		Owner:  from.addr,
	})
	if err != nil {
		return fmt.Errorf("unable to fetch account balance: %w", err)
	}

	// Since we're dealing with tiny amounts, we can afford this hack.
	amount := acct.General.Balance.ToBigInt().Uint64()

	if amount == 0 {
		// Nothing to refund.
		return nil
	}

	// We don't want refunds to fail, so disable caching.
	if err = transfer(ctx, cc, from, toAddr, amount, true, false); err != nil {
		return fmt.Errorf("unable to refund from account: %w", err)
	}

	return nil
}

func refundMultiple(ctx context.Context, cc consensus.ClientBackend, sc staking.Backend, account []localAccount, toAddr staking.Address) {
	// Do the refunds in parallel.
	doneCh := make(chan bool, len(account))
	for a := range account {
		go func(a int) {
			if err := refund(ctx, cc, sc, &account[a], toAddr); err != nil {
				// Tough luck.
				logger.Error("unable to refund from account",
					"account_address", account[a].addr,
					"err", err,
				)
			}
			doneCh <- true
		}(a)
	}

	// Wait for all goroutines to finish.
	for range account {
		<-doneCh
	}
}

func doRun(cmd *cobra.Command, args []string) error { // nolint: gocyclo
	cmd.SilenceUsage = true

	if err := common.Init(); err != nil {
		common.EarlyLogAndExit(err)
	}

	numAccounts := viper.GetUint64(CfgNumAccounts)
	numSamples := viper.GetUint64(CfgNumSamples)

	if numAccounts < 1 {
		return fmt.Errorf("number of accounts must be >= 1")
	}
	if numSamples < 3 {
		return fmt.Errorf("number of samples must be >= 3")
	}

	fundAndExit := viper.GetBool(CfgFundAndExit)
	refundAndExit := viper.GetBool(CfgRefundAndExit)
	skipFunding := viper.GetBool(CfgSkipFunding)

	if fundAndExit && refundAndExit {
		return fmt.Errorf("cannot both fund and refund at the same time")
	}
	if fundAndExit && skipFunding {
		return fmt.Errorf("exiting")
	}
	if refundAndExit && skipFunding {
		return fmt.Errorf("--skip_funding has no effect with --refund_and_exit")
	}

	ctx := context.Background()

	// Connect to node.
	logger.Debug("dialing node", "addr", viper.GetString(cmdGrpc.CfgAddress))
	conn, err := cmdGrpc.NewClient(cmd)
	if err != nil {
		return fmt.Errorf("unable to connect to node: %w", err)
	}
	defer conn.Close()

	cc := consensus.NewConsensusClient(conn)
	sc := staking.NewStakingClient(conn)
	ncc := api.NewNodeControllerClient(conn)

	// Set chain context from genesis document obtained from the node.
	genDoc, err := cc.GetGenesisDocument(ctx)
	if err != nil {
		return fmt.Errorf("unable to obtain genesis document from node: %w", err)
	}
	genDoc.SetChainContext()

	// Create new DRBG.
	src, err := drbg.New(crypto.SHA512, []byte(viper.GetString(CfgSeed)), nil, []byte("consensus benchmark"))
	if err != nil {
		return fmt.Errorf("unable to create deterministic random generator: %w", err)
	}
	rng := rand.New(mathrand.New(src))

	// Wait for the node to sync.
	logger.Debug("waiting for node sync")
	if err = ncc.WaitSync(context.Background()); err != nil {
		return fmt.Errorf("unable to wait for node sync: %w", err)
	}
	logger.Debug("node synced")

	// Create multiple accounts.
	account := make([]localAccount, numAccounts)
	msf := memorySigner.NewFactory()
	for a := range account {
		signer, grr := msf.Generate(signature.SignerEntity, rng)
		if grr != nil {
			return fmt.Errorf("unable to generate account %d: %w", a, grr)
		}
		account[a].signer = signer
		account[a].addr = staking.NewAddress(signer.Public())
		account[a].cachedNonce = notYetCached
		account[a].cachedGas = notYetCached
		account[a].errorCount = make(map[error]uint64)
	}

	var fundingSigner signature.Signer

	if !skipFunding {
		if viper.GetBool(CfgUseTestEntity) {
			// Use test entity for funding.
			_, fundingSigner, _ = entity.TestEntity()
		} else {
			// Use given signer for funding.
			signerDir, grr := cmdSigner.CLIDirOrPwd()
			if grr != nil {
				return fmt.Errorf("failed to retrieve signer dir: %w", grr)
			}
			signerFactory, grr := cmdSigner.NewFactory(cmdSigner.Backend(), signerDir, signature.SignerEntity)
			if grr != nil {
				return fmt.Errorf("failed to create signer factory: %w", grr)
			}
			fundingSigner, grr = signerFactory.Load(signature.SignerEntity)
			if grr != nil {
				return fmt.Errorf("failed to load signer: %w", grr)
			}
		}
	} else {
		// We won't need a signer, since we're not funding, fake it
		// with the test entity instead.
		_, fundingSigner, _ = entity.TestEntity()
	}

	fundingAddr := staking.NewAddress(fundingSigner.Public())

	if refundAndExit {
		logger.Info("refunding money")
		refundMultiple(ctx, cc, sc, account, fundingAddr)
		logger.Info("money refunded")
		return nil
	}

	if !skipFunding {
		fundingAcct := localAccount{
			signer:      fundingSigner,
			addr:        fundingAddr,
			cachedNonce: notYetCached,
			cachedGas:   notYetCached,
		}

		// Check if funding account has enough funds.
		logger.Debug("checking if funding account has enough funds")
		var fundingAcctInfo *staking.Account
		fundingAcctInfo, err = sc.Account(ctx, &staking.OwnerQuery{
			Height: consensus.HeightLatest,
			Owner:  fundingAddr,
		})
		if err != nil {
			return fmt.Errorf("unable to fetch funding account balance: %w", err)
		}
		// Estimate gas.
		dummyXfer := &staking.Transfer{To: fundingAddr}
		if err = dummyXfer.Amount.FromUint64(1); err != nil {
			return fmt.Errorf("unable to convert uint64 to amount: %w", err)
		}
		var estGas transaction.Gas
		estGas, err = cc.EstimateGas(ctx, &consensus.EstimateGasRequest{
			Signer:      fundingSigner.Public(),
			Transaction: staking.NewTransferTx(fundingAcctInfo.General.Nonce, nil, dummyXfer),
		})
		if err != nil {
			return fmt.Errorf("unable to estimate gas: %w", err)
		}
		// Each account needs additional tokens for fees.
		// An additional fee is allocated for the refund at the end.
		perAccountFunds := numSamples + (numSamples+1)*(viper.GetUint64(CfgGasPrice)*uint64(estGas))
		requiredFunds := quantity.NewFromUint64(numAccounts * perAccountFunds)
		availableFunds := fundingAcctInfo.General.Balance
		if availableFunds.Cmp(requiredFunds) < 0 {
			return fmt.Errorf("funding account has insufficient funds (%s required, %s available)", requiredFunds.String(), availableFunds.String())
		}
		logger.Debug("funding account has enough funds",
			"required", requiredFunds.String(),
			"available", availableFunds.String(),
		)

		// Fund all accounts from the funding account.
		logger.Info("funding test accounts",
			"num_accounts", numAccounts,
		)
		for a := range account {
			// Populate cached gas estimates.
			account[a].cachedGas = uint64(estGas)

			// Each account gets perAccountFunds tokens.
			if errr := transfer(ctx, cc, &fundingAcct, account[a].addr, perAccountFunds, true, false); errr != nil {
				// An error has happened while funding, make sure to refund the
				// funding account from the accounts funded until this point.
				logger.Error("error while funding, attempting to refund account")
				refundMultiple(ctx, cc, sc, account[0:a], fundingAddr)
				return fmt.Errorf("unable to fund account %d: %w", a, errr)
			}
		}
		if fundAndExit {
			return nil
		}
	}

	noWait := viper.IsSet(CfgNoWait)
	noWaitDuration := viper.GetDuration(CfgNoWait)

	logger.Info("starting benchmark", "num_accounts", numAccounts)
	startStatus, err := cc.GetStatus(ctx)
	if err != nil {
		if !skipFunding {
			logger.Info("refunding money")
			refundMultiple(ctx, cc, sc, account, fundingAddr)
			logger.Info("money refunded")
		}
		return fmt.Errorf("unable to get status: %w", err)
	}
	benchmarkStartHeight := startStatus.LatestHeight
	benchmarkStartT := time.Now()

	// Submit time is the time required to submit the transaction and
	// wait for it to be included in a block.
	var (
		totalSubmitTimeNs uint64
		numSubmitSamples  uint64
		numSubmitErrors   uint64
		gottaStopFast     uint32
	)

	// Perform benchmark in parallel, one goroutine per account.
	doneCh := make(chan bool, numAccounts*numSamples)
	for a := range account {
		go func(idx uint64) {
			var noCache bool
			for s := uint64(0); s < numSamples; s++ {
				if atomic.LoadUint32(&gottaStopFast) > 0 {
					// Terminate.
					return
				}
				if noWait {
					// Send transactions until terminated.
					// Ignore cache because it results in too many errors.
					s = 0
					noCache = true
				}

				fromIdx := idx
				toIdx := idx
				toAddr := account[toIdx].addr

				startT := time.Now()
				if err = transfer(ctx, cc, &account[fromIdx], toAddr, 1, noCache, noWait); err != nil {
					atomic.AddUint64(&numSubmitErrors, 1)
					// Disable cache for the next sample, just in case
					// we messed up the nonce or if the gas cost changed.
					if !noWait {
						noCache = true
						doneCh <- true
					}
					continue
				}
				atomic.AddUint64(&totalSubmitTimeNs, uint64(time.Since(startT).Nanoseconds()))
				atomic.AddUint64(&numSubmitSamples, 1)
				if !noWait {
					noCache = false
					doneCh <- true
				}
			}
		}(uint64(a))
	}

	if !noWait {
		// Wait for all goroutines to finish.
		for i := uint64(0); i < numAccounts*numSamples; i++ {
			<-doneCh
		}
	} else {
		time.Sleep(noWaitDuration)
		atomic.StoreUint32(&gottaStopFast, 1)
	}

	benchmarkDuration := time.Since(benchmarkStartT)
	stopStatus, err := cc.GetStatus(ctx)
	if err != nil {
		if !skipFunding {
			logger.Info("refunding money")
			refundMultiple(ctx, cc, sc, account, fundingAddr)
			logger.Info("money refunded")
		}
		return fmt.Errorf("unable to get status: %w", err)
	}
	benchmarkStopHeight := stopStatus.LatestHeight

	// Go through all transactions from benchmarkStartHeight to
	// benchmarkStopHeight and calculate the average number of
	// transactions per second and other stats.
	// Note that we count all transactions, not just the ones made
	// by this benchmark.
	//
	// In addition, do a sliding window for the max avg tps.
	var totalTxs uint64
	var maxTxs uint64
	minTxs := uint64(18446744073709551615)
	txsPerBlock := make([]uint64, 0)
	txBytesPerBlock := make([]uint64, 0)
	blockDeltaT := make([]float64, 0)
	blockT := make([]time.Time, 0)
	var prevBlockT time.Time

	for height := benchmarkStartHeight; height <= benchmarkStopHeight; height++ {
		// Count number of transactions.
		txs, grr := cc.GetTransactions(ctx, height)
		if grr != nil {
			logger.Error("GetTransactions failed", "err", grr, "height", height)
			continue
		}
		lenTxs := uint64(len(txs))
		totalTxs += lenTxs
		txsPerBlock = append(txsPerBlock, lenTxs)
		if lenTxs > maxTxs {
			maxTxs = lenTxs
		}
		if lenTxs < minTxs {
			minTxs = lenTxs
		}

		// Count size of transactions in bytes.
		var blkSizeBytes uint64
		for _, tx := range txs {
			blkSizeBytes += uint64(len(tx))
		}
		txBytesPerBlock = append(txBytesPerBlock, blkSizeBytes)

		// Calculate time between blocks.
		blk, grr := cc.GetBlock(ctx, height)
		if grr != nil {
			logger.Error("GetBlock failed", "err", grr, "height", height)
			continue
		}
		if prevBlockT.IsZero() {
			prevBlockT = blk.Time
		}
		blockDeltaT = append(blockDeltaT, blk.Time.Sub(prevBlockT).Seconds())
		prevBlockT = blk.Time
		blockT = append(blockT, blk.Time)
	}

	tps := float64(totalTxs) / benchmarkDuration.Seconds()

	// Calculate median number of transactions.
	sort.Slice(txsPerBlock, func(i, j int) bool { return txsPerBlock[i] < txsPerBlock[j] })
	medianTxs := txsPerBlock[len(txsPerBlock)/2]

	avgSubmitTimeNs := float64(totalSubmitTimeNs) / float64(numSubmitSamples)

	// Do a sliding window over the block size array to get the max avg tps.
	var bestAvgTps float64
	for slidingWindowSize := 1; slidingWindowSize <= 32; slidingWindowSize++ {
		for i := range txsPerBlock {
			var curAvgTps float64
			j := i
			// Gather transactions from up to slidingWindowSize blocks or
			// up to as many blocks as needed for the block timestamp to change.
			// The block timestamp has a granularity of only 1s, so this can be
			// an issue with fast CommitTimeouts (e.g. less than 1s), as it
			// can cause a divide by zero in the average tps calculation below
			// (since the blocks are too close together).
			// Increasing the window size to encompass blocks with different
			// times fixes this.
			for ; j < len(txsPerBlock) && (blockT[j] == blockT[i] || j < i+slidingWindowSize); j++ {
				curAvgTps += float64(txsPerBlock[j])
			}
			curAvgTps /= blockT[j-1].Sub(blockT[i]).Seconds()
			// Despite the workaround above, the above can still divide by zero
			// at the very end of the run, so make sure we don't count that.
			if curAvgTps > bestAvgTps && !math.IsInf(curAvgTps, 0) {
				bestAvgTps = curAvgTps
			}
		}
	}

	// Collect number of transfer errors from all accounts.
	errCounts := make(map[string]uint64)
	for a := range account {
		for e, c := range account[a].errorCount {
			er := strings.ReplaceAll(e.Error(), " ", "_")
			er = strings.ReplaceAll(er, "\"", "'")
			errCounts[er] += c
		}
	}
	// Output the results sorted by key.
	keys := make([]string, 0, len(errCounts))
	for k := range errCounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	errCountsString := ""
	for _, k := range keys {
		if errCountsString != "" {
			errCountsString += " "
		}
		errCountsString += k + "#" + fmt.Sprintf("%v", errCounts[k])
	}

	logger.Info("benchmark finished",
		// Number of accounts involved in benchmark (level of parallelism).
		"num_accounts", numAccounts,
		// Average time (in seconds) required to submit a transaction and wait
		// for it to be included in a block.
		"avg_submit_time_s", avgSubmitTimeNs/1.0e9,
		// Transactions per second (this includes all transactions that
		// appeared on the network during the time of the benchmark).
		"transactions_per_second", tps,
		// Number of successful SubmitTx calls (i.e. transfer transactions).
		"submit_samples", numSubmitSamples,
		// Number of unsuccessful SubmitTx calls.
		"submit_errors", numSubmitErrors,
		// Duration of the entire benchmark (in seconds).
		"bench_duration_s", benchmarkDuration.Seconds(),
		// Number of blocks seen on the network during the benchmark.
		"num_blocks", len(txsPerBlock),
		// Minimum number of transactions per block (during the benchmark).
		"min_txs_per_block", minTxs,
		// Maximum number of transactions per block (during the benchmark).
		"max_txs_per_block", maxTxs,
		// Average number of transactions per block (during the benchmark).
		"avg_txs_per_block", float64(totalTxs)/float64(len(txsPerBlock)),
		// Median number of transactions per block (during the benchmark).
		"median_txs_per_block", medianTxs,
		// Total number of transactions observed during the benchmark.
		"total_txs", totalTxs,
		// Number of transactions in each block (block size).
		"block_sizes", strings.Trim(fmt.Sprint(txsPerBlock), "[]"),
		// Size of all transactions in each block (in bytes).
		"block_sizes_bytes", strings.Trim(fmt.Sprint(txBytesPerBlock), "[]"),
		// Time delta between blocks (in seconds).
		"block_delta_t_s", strings.Trim(fmt.Sprint(blockDeltaT), "[]"),
		// Maximum average tps over a sliding window.
		"max_avg_tps", bestAvgTps,
		// Map of errors that occurred during benchmarking (if any).
		// These are sorted by key.
		"error_counts", errCountsString,
	)

	// Refund money into original funding account.
	if !skipFunding {
		logger.Info("refunding money")
		refundMultiple(ctx, cc, sc, account, fundingAddr)
		logger.Info("money refunded")
	}

	return nil
}

// Register registers the conbench sub-command.
func RegisterConbenchCmd(parentCmd *cobra.Command) {
	parentCmd.AddCommand(conbenchCmd)
}

func init() {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Uint64(CfgNumAccounts, 10, "Number of accounts to create for benchmarking (also level of parallelism)")
	fs.Uint64(CfgNumSamples, 30, "Number of samples (transfers) per account")
	fs.Duration(CfgSubmitTxTimeout, 10*time.Second, "Timeout for SubmitTx (set this based on network parameters)")
	fs.Duration(CfgNoWait, 10*time.Second, "Use SubmitTxNoWait instead of SubmitTx (spam transactions) for given amount of time")
	fs.Bool(CfgUseTestEntity, false, "Use test entity for funding (only for testing)")
	fs.Uint64(CfgGasPrice, 1, "Gas price (should be set to the minimum gas price of validators)")
	fs.Bool(CfgFundAndExit, false, "Only fund accounts and exit")
	fs.Bool(CfgRefundAndExit, false, "Only refund funding account and exit")
	fs.Bool(CfgSkipFunding, false, "Skip funding accounts")
	fs.String(CfgSeed, "consensus benchmark random seeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeed", "Seed to use for the DRBG (change this if you're running multiple benchmarks in parallel)")
	_ = viper.BindPFlags(fs)
	conbenchCmd.Flags().AddFlagSet(fs)

	conbenchCmd.Flags().AddFlagSet(cmdGrpc.ClientFlags)
	conbenchCmd.Flags().AddFlagSet(cmdFlags.DebugTestEntityFlags)
	conbenchCmd.Flags().AddFlagSet(cmdFlags.DebugDontBlameOasisFlag)
	conbenchCmd.Flags().AddFlagSet(cmdSigner.CLIFlags)
}
