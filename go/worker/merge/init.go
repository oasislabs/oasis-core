package merge

import (
	"time"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

	workerCommon "github.com/oasislabs/ekiden/go/worker/common"
	"github.com/oasislabs/ekiden/go/worker/merge/committee"
	"github.com/oasislabs/ekiden/go/worker/registration"
)

const (
	// CfgWorkerEnabled enables the merge worker.
	CfgWorkerEnabled = "worker.merge.enabled"

	cfgStorageCommitTimeout         = "worker.merge.storage_commit_timeout"
	cfgByzantineInjectDiscrepancies = "worker.merge.byzantine.inject_discrepancies"
)

// Flags has the configuration flags.
var Flags = flag.NewFlagSet("", flag.ContinueOnError)

// Enabled reads our enabled flag from viper.
func Enabled() bool {
	return viper.GetBool(CfgWorkerEnabled)
}

// New creates a new worker.
func New(
	commonWorker *workerCommon.Worker,
	registration *registration.Registration,
) (*Worker, error) {
	cfg := Config{
		Committee: committee.Config{
			StorageCommitTimeout:         viper.GetDuration(cfgStorageCommitTimeout),
			ByzantineInjectDiscrepancies: viper.GetBool(cfgByzantineInjectDiscrepancies),
		},
	}

	return newWorker(Enabled(), commonWorker, registration, cfg)
}

func init() {
	Flags.Bool(CfgWorkerEnabled, false, "Enable merge worker process")
	Flags.Duration(cfgStorageCommitTimeout, 5*time.Second, "Storage commit timeout")

	Flags.Bool(cfgByzantineInjectDiscrepancies, false, "BYZANTINE: Inject discrepancies")
	_ = Flags.MarkHidden(cfgByzantineInjectDiscrepancies)

	_ = viper.BindPFlags(Flags)
}
