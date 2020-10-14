package oasis

import (
	"fmt"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/sgx"
	epochtime "github.com/oasisprotocol/oasis-core/go/epochtime/api"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/env"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/log"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	storage "github.com/oasisprotocol/oasis-core/go/storage/api"
)

// NetworkFixture describes configuration for the test Oasis network and
// all the spawned nodes.
type NetworkFixture struct {
	TEE                TEEFixture                `json:"tee,omitempty"`
	Network            NetworkCfg                `json:"network,omitempty"`
	Entities           []EntityCfg               `json:"entities,omitempty"`
	Runtimes           []RuntimeFixture          `json:"runtimes,omitempty"`
	Validators         []ValidatorFixture        `json:"validators,omitempty"`
	Keymanagers        []KeymanagerFixture       `json:"keymanagers,omitempty"`
	KeymanagerPolicies []KeymanagerPolicyFixture `json:"keymanager_policies,omitempty"`
	StorageWorkers     []StorageWorkerFixture    `json:"storage_workers,omitempty"`
	ComputeWorkers     []ComputeWorkerFixture    `json:"compute_workers,omitempty"`
	Sentries           []SentryFixture           `json:"sentries,omitempty"`
	Clients            []ClientFixture           `json:"clients,omitempty"`
	Seeds              []SeedFixture             `json:"seeds,omitempty"`
	ByzantineNodes     []ByzantineFixture        `json:"byzantine_nodes,omitempty"`
}

// Create instantiates the network described by the fixture.
func (f *NetworkFixture) Create(env *env.Env) (*Network, error) {
	// Use default MRSIGNER if not provided.
	if f.TEE.Hardware == node.TEEHardwareIntelSGX && f.TEE.MrSigner == nil {
		f.TEE.MrSigner = &sgx.FortanixDummyMrSigner
	}

	// Create the top level Oasis network.
	var net *Network
	var err error
	if net, err = New(env, &f.Network); err != nil {
		return nil, err
	}

	// Provision entities.
	for _, entCfg := range f.Entities {
		if _, err = net.NewEntity(&entCfg); err != nil { // nolint: gosec
			return nil, fmt.Errorf("failed to provision entity: %w", err)
		}
	}

	// Provision runtimes.
	for _, fx := range f.Runtimes {
		if _, err = fx.Create(f, net); err != nil {
			return nil, err
		}
	}

	// Provision the sentry nodes.
	for _, fx := range f.Sentries {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	for _, fx := range f.Seeds {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision validators.
	for _, fx := range f.Validators {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision key manager policies.
	for _, fx := range f.KeymanagerPolicies {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision key managers.
	for _, fx := range f.Keymanagers {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision the storage workers.
	for _, fx := range f.StorageWorkers {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision the compute workers.
	for _, fx := range f.ComputeWorkers {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision the client nodes.
	for _, fx := range f.Clients {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	// Provision the Byzantine nodes.
	for _, fx := range f.ByzantineNodes {
		if _, err = fx.Create(net); err != nil {
			return nil, err
		}
	}

	return net, nil
}

// ConsensusFixture is a fixture containing consensus-related configuration.
type ConsensusFixture struct { // nolint: maligned
	// MinGasPrice specifies the minimum gas price accepted by a validator node.
	MinGasPrice uint64 `json:"min_gas_price"`
	// SubmissionGasPrice is the gas price to use when submitting consensus transactions.
	SubmissionGasPrice uint64 `json:"submission_gas_price"`

	// DisableCheckTx causes the consensus layer to skip transaction checks.
	DisableCheckTx bool `json:"disable_check_tx"`

	// PruneNumKept is the number of blocks to keep (zero disables pruning).
	PruneNumKept uint64 `json:"prune_num_kept"`

	// TendermintRecoverCorruptedWAL enables automatic recovery of corrupted Tendermint's WAL.
	TendermintRecoverCorruptedWAL bool `json:"tendermint_recover_corrupted_wal"`

	// EnableConsensusRPCWorker enables the public consensus RPC services worker.
	EnableConsensusRPCWorker bool `json:"enable_consensusrpc_worker,omitempty"`

	// DisableSupplementarySanityChecks disables the supplementary sanity checks.
	DisableSupplementarySanityChecks bool `json:"disable_supplementary_sanity_checks,omitempty"`
}

// TEEFixture is a TEE configuration fixture.
type TEEFixture struct {
	Hardware node.TEEHardware `json:"hardware"`
	MrSigner *sgx.MrSigner    `json:"mr_signer"`
}

// ValidatorFixture is a validator fixture.
type ValidatorFixture struct { // nolint: maligned
	AllowEarlyTermination bool `json:"allow_early_termination"`
	AllowErrorTermination bool `json:"allow_error_termination"`

	NoAutoStart bool `json:"no_auto_start,omitempty"`

	Entity int `json:"entity"`

	LogWatcherHandlerFactories []log.WatcherHandlerFactory `json:"-"`

	Sentries []int `json:"sentries,omitempty"`

	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`
}

// Create instantiates the validator described by the fixture.
func (f *ValidatorFixture) Create(net *Network) (*Validator, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}
	sentries, err := resolveSentries(net, f.Sentries)
	if err != nil {
		return nil, err
	}

	return net.NewValidator(&ValidatorCfg{
		NodeCfg: NodeCfg{
			AllowEarlyTermination:      f.AllowEarlyTermination,
			AllowErrorTermination:      f.AllowErrorTermination,
			LogWatcherHandlerFactories: f.LogWatcherHandlerFactories,
			Consensus:                  f.Consensus,
			NoAutoStart:                f.NoAutoStart,
		},
		Entity:   entity,
		Sentries: sentries,
	})
}

// RuntimeFixture is a runtime fixture.
type RuntimeFixture struct { // nolint: maligned
	ID         common.Namespace     `json:"id"`
	Kind       registry.RuntimeKind `json:"kind"`
	Entity     int                  `json:"entity"`
	Keymanager int                  `json:"keymanager"`

	Binaries         []string         `json:"binaries"`
	GenesisState     storage.WriteLog `json:"genesis_state,omitempty"`
	GenesisStatePath string           `json:"genesis_state_path,omitempty"`
	GenesisRound     uint64           `json:"genesis_round,omitempty"`

	Executor     registry.ExecutorParameters     `json:"executor"`
	TxnScheduler registry.TxnSchedulerParameters `json:"txn_scheduler"`
	Storage      registry.StorageParameters      `json:"storage"`

	AdmissionPolicy registry.RuntimeAdmissionPolicy   `json:"admission_policy"`
	Staking         registry.RuntimeStakingParameters `json:"staking,omitempty"`

	Pruner RuntimePrunerCfg `json:"pruner,omitempty"`

	ExcludeFromGenesis bool `json:"exclude_from_genesis,omitempty"`
}

// Create instantiates the runtime described by the fixture.
func (f *RuntimeFixture) Create(netFixture *NetworkFixture, net *Network) (*Runtime, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}

	var km *Runtime
	if f.Keymanager != -1 {
		switch f.Kind {
		case registry.KindCompute:
			if km, err = resolveRuntimeOfKind(net, f.Keymanager, registry.KindKeyManager); err != nil {
				return nil, err
			}
		case registry.KindKeyManager:
			return nil, fmt.Errorf("key manager runtime cannot have a key manager")
		}
	}

	return net.NewRuntime(&RuntimeCfg{
		ID:                 f.ID,
		Kind:               f.Kind,
		Entity:             entity,
		Keymanager:         km,
		TEEHardware:        netFixture.TEE.Hardware,
		MrSigner:           netFixture.TEE.MrSigner,
		Executor:           f.Executor,
		TxnScheduler:       f.TxnScheduler,
		Storage:            f.Storage,
		AdmissionPolicy:    f.AdmissionPolicy,
		Staking:            f.Staking,
		Binaries:           f.Binaries,
		GenesisState:       f.GenesisState,
		GenesisStatePath:   f.GenesisStatePath,
		GenesisRound:       f.GenesisRound,
		Pruner:             f.Pruner,
		ExcludeFromGenesis: f.ExcludeFromGenesis,
	})
}

// KeymangerPolicyFixgure is a key manager policy fixture.
type KeymanagerPolicyFixture struct {
	Runtime int `json:"runtime"`
	Serial  int `json:"serial"`
}

// Create instantiates the key manager policy described in the fixture.
func (f *KeymanagerPolicyFixture) Create(net *Network) (*KeymanagerPolicy, error) {
	runtime, err := resolveRuntimeOfKind(net, f.Runtime, registry.KindKeyManager)
	if err != nil {
		return nil, err
	}

	return net.NewKeymanagerPolicy(&KeymanagerPolicyCfg{
		Runtime: runtime,
		Serial:  f.Serial,
	})
}

// KeymanagerFixture is a key manager fixture.
type KeymanagerFixture struct {
	Runtime int `json:"runtime"`
	Entity  int `json:"entity"`
	Policy  int `json:"policy"`

	AllowEarlyTermination bool `json:"allow_early_termination"`
	AllowErrorTermination bool `json:"allow_error_termination"`

	NoAutoStart bool `json:"no_auto_start,omitempty"`

	Sentries []int `json:"sentries,omitempty"`

	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`

	LogWatcherHandlerFactories []log.WatcherHandlerFactory `json:"-"`
}

// Create instantiates the key manager described by the fixture.
func (f *KeymanagerFixture) Create(net *Network) (*Keymanager, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}
	runtime, err := resolveRuntimeOfKind(net, f.Runtime, registry.KindKeyManager)
	if err != nil {
		return nil, err
	}
	policy, err := resolveKMPolicy(net, f.Policy)
	if err != nil {
		return nil, err
	}

	return net.NewKeymanager(&KeymanagerCfg{
		NodeCfg: NodeCfg{
			AllowEarlyTermination:      f.AllowEarlyTermination,
			AllowErrorTermination:      f.AllowErrorTermination,
			LogWatcherHandlerFactories: f.LogWatcherHandlerFactories,
			Consensus:                  f.Consensus,
			NoAutoStart:                f.NoAutoStart,
		},
		Runtime:       runtime,
		Entity:        entity,
		Policy:        policy,
		SentryIndices: f.Sentries,
	})
}

// StorageWorkerFixture is a storage worker fixture.
type StorageWorkerFixture struct { // nolint: maligned
	Backend string `json:"backend"`
	Entity  int    `json:"entity"`

	AllowEarlyTermination bool `json:"allow_early_termination"`
	AllowErrorTermination bool `json:"allow_error_termination"`

	NoAutoStart bool `json:"no_auto_start,omitempty"`

	DisableCertRotation bool `json:"disable_cert_rotation"`

	LogWatcherHandlerFactories []log.WatcherHandlerFactory `json:"-"`

	Sentries []int `json:"sentries,omitempty"`

	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`

	CheckpointCheckInterval time.Duration `json:"checkpoint_check_interval,omitempty"`
	IgnoreApplies           bool          `json:"ignore_applies,omitempty"`
	CheckpointSyncEnabled   bool          `json:"checkpoint_sync_enabled,omitempty"`

	// Runtimes contains the indexes of the runtimes to enable. Leave
	// empty or nil for the default behaviour (i.e. include all runtimes).
	Runtimes []int `json:"runtimes,omitempty"`
}

// Create instantiates the storage worker described by the fixture.
func (f *StorageWorkerFixture) Create(net *Network) (*Storage, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}

	return net.NewStorage(&StorageCfg{
		NodeCfg: NodeCfg{
			AllowEarlyTermination:      f.AllowEarlyTermination,
			AllowErrorTermination:      f.AllowErrorTermination,
			NoAutoStart:                f.NoAutoStart,
			LogWatcherHandlerFactories: f.LogWatcherHandlerFactories,
			Consensus:                  f.Consensus,
		},
		Backend:                 f.Backend,
		Entity:                  entity,
		SentryIndices:           f.Sentries,
		CheckpointCheckInterval: f.CheckpointCheckInterval,
		IgnoreApplies:           f.IgnoreApplies,
		// The checkpoint syncing flas is intentionally flipped here.
		// Syncing should normally be enabled, but normally disabled in tests.
		CheckpointSyncDisabled: !f.CheckpointSyncEnabled,
		DisableCertRotation:    f.DisableCertRotation,
		Runtimes:               f.Runtimes,
	})
}

// ComputeWorkerFixture is a compute worker fixture.
type ComputeWorkerFixture struct {
	Entity int `json:"entity"`

	RuntimeProvisioner string `json:"runtime_provisioner"`

	AllowEarlyTermination bool `json:"allow_early_termination"`
	AllowErrorTermination bool `json:"allow_error_termination"`

	NoAutoStart bool `json:"no_auto_start,omitempty"`

	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`

	LogWatcherHandlerFactories []log.WatcherHandlerFactory `json:"-"`

	// Runtimes contains the indexes of the runtimes to enable.
	Runtimes []int `json:"runtimes,omitempty"`
}

// Create instantiates the compute worker described by the fixture.
func (f *ComputeWorkerFixture) Create(net *Network) (*Compute, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}

	return net.NewCompute(&ComputeCfg{
		NodeCfg: NodeCfg{
			AllowEarlyTermination:      f.AllowEarlyTermination,
			AllowErrorTermination:      f.AllowErrorTermination,
			NoAutoStart:                f.NoAutoStart,
			LogWatcherHandlerFactories: f.LogWatcherHandlerFactories,
			Consensus:                  f.Consensus,
		},
		Entity:             entity,
		RuntimeProvisioner: f.RuntimeProvisioner,
		Runtimes:           f.Runtimes,
	})
}

// SeedFixture is a seed node fixture.
type SeedFixture struct {
	DisableAddrBookFromGenesis bool `json:"disable_addr_book_from_genesis"`
}

// Create instantiates the seed node described by the fixture.
func (f *SeedFixture) Create(net *Network) (*Seed, error) {
	return net.NewSeed(&SeedCfg{
		DisableAddrBookFromGenesis: f.DisableAddrBookFromGenesis,
	})
}

// SentryFixture is a sentry node fixture.
type SentryFixture struct {
	LogWatcherHandlerFactories []log.WatcherHandlerFactory `json:"-"`

	Validators        []int `json:"validators"`
	StorageWorkers    []int `json:"storage_workers"`
	KeymanagerWorkers []int `json:"keymanager_workers"`
}

// Create instantiates the sentry node described by the fixture.
func (f *SentryFixture) Create(net *Network) (*Sentry, error) {
	return net.NewSentry(&SentryCfg{
		NodeCfg: NodeCfg{
			LogWatcherHandlerFactories: f.LogWatcherHandlerFactories,
		},
		ValidatorIndices:  f.Validators,
		StorageIndices:    f.StorageWorkers,
		KeymanagerIndices: f.KeymanagerWorkers,
	})
}

// ClientFixture is a client node fixture.
type ClientFixture struct {
	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`
}

// Create instantiates the client node described by the fixture.
func (f *ClientFixture) Create(net *Network) (*Client, error) {
	return net.NewClient(&ClientCfg{
		NodeCfg: NodeCfg{
			Consensus: f.Consensus,
		},
	})
}

// ByzantineFixture is a byzantine node fixture.
type ByzantineFixture struct { // nolint: maligned
	Script       string `json:"script"`
	IdentitySeed string `json:"identity_seed"`
	Entity       int    `json:"entity"`

	ExecutorIsScheduler bool `json:"executor_is_scheduler"`

	ActivationEpoch epochtime.EpochTime `json:"activation_epoch"`

	// Consensus contains configuration for the consensus backend.
	Consensus ConsensusFixture `json:"consensus"`

	EnableDefaultLogWatcherHandlerFactories bool                        `json:"enable_default_log_fac"`
	LogWatcherHandlerFactories              []log.WatcherHandlerFactory `json:"-"`
}

// Create instantiates the byzantine node described by the fixture.
func (f *ByzantineFixture) Create(net *Network) (*Byzantine, error) {
	entity, err := resolveEntity(net, f.Entity)
	if err != nil {
		return nil, err
	}

	return net.NewByzantine(&ByzantineCfg{
		NodeCfg: NodeCfg{
			DisableDefaultLogWatcherHandlerFactories: !f.EnableDefaultLogWatcherHandlerFactories,
			LogWatcherHandlerFactories:               f.LogWatcherHandlerFactories,
			Consensus:                                f.Consensus,
		},
		Script:              f.Script,
		IdentitySeed:        f.IdentitySeed,
		Entity:              entity,
		ExecutorIsScheduler: f.ExecutorIsScheduler,
		ActivationEpoch:     f.ActivationEpoch,
	})
}

func resolveEntity(net *Network, index int) (*Entity, error) {
	entities := net.Entities()
	if index < 0 || index >= len(entities) {
		return nil, fmt.Errorf("invalid entity index: %d", index)
	}
	return entities[index], nil
}

func resolveValidators(net *Network, indices []int) ([]*Validator, error) {
	allValidators := net.Validators()
	var validators []*Validator
	for _, index := range indices {
		if index < 0 || index >= len(allValidators) {
			return nil, fmt.Errorf("invalid validator index: %d", index)
		}
		validators = append(validators, allValidators[index])
	}
	return validators, nil
}

func resolveStorageWorkers(net *Network, indices []int) ([]*Storage, error) {
	allStorageWorkers := net.StorageWorkers()
	var storageWorkers []*Storage
	for _, index := range indices {
		if index < 0 || index >= len(allStorageWorkers) {
			return nil, fmt.Errorf("invalid storage index: %d", index)
		}
		storageWorkers = append(storageWorkers, allStorageWorkers[index])
	}
	return storageWorkers, nil
}

func resolveKeymanagerWorkers(net *Network, indices []int) ([]*Keymanager, error) {
	allKeymanagerWorkers := net.Keymanagers()
	var keymanagerWorkers []*Keymanager
	for _, index := range indices {
		if index < 0 || index >= len(allKeymanagerWorkers) {
			return nil, fmt.Errorf("invalid keymanager index: %d", index)
		}
		keymanagerWorkers = append(keymanagerWorkers, allKeymanagerWorkers[index])
	}
	return keymanagerWorkers, nil
}

func resolveRuntime(net *Network, index int) (*Runtime, error) {
	runtimes := net.Runtimes()
	if index < 0 || index >= len(runtimes) {
		return nil, fmt.Errorf("invalid runtime index: %d", index)
	}
	return runtimes[index], nil
}

func resolveRuntimeOfKind(net *Network, index int, kind registry.RuntimeKind) (*Runtime, error) {
	runtime, err := resolveRuntime(net, index)
	if err != nil {
		return nil, err
	}
	if runtime.kind != kind {
		return nil, fmt.Errorf("runtime %d has an incorrect kind (expected: %s got: %s)",
			index,
			kind,
			runtime.kind,
		)
	}
	return runtime, nil
}

func resolveSentries(net *Network, indices []int) ([]*Sentry, error) {
	allSentries := net.Sentries()
	var sentries []*Sentry
	for _, index := range indices {
		if index < 0 || index >= len(allSentries) {
			return nil, fmt.Errorf("invalid sentry index: %d", index)
		}
		sentries = append(sentries, allSentries[index])
	}
	return sentries, nil
}

func resolveKMPolicy(net *Network, index int) (*KeymanagerPolicy, error) {
	policies := net.keymanagerPolicies
	if index < 0 || index >= len(policies) {
		return nil, fmt.Errorf("invalid policy index: %d", index)
	}
	return policies[index], nil
}
