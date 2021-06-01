package seed

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/pex"
	"github.com/tendermint/tendermint/types"
	tmversion "github.com/tendermint/tendermint/version"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/identity"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/pubsub"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	consensus "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	tmcommon "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/common"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/crypto"
	genesis "github.com/oasisprotocol/oasis-core/go/genesis/api"
	governance "github.com/oasisprotocol/oasis-core/go/governance/api"
	keymanager "github.com/oasisprotocol/oasis-core/go/keymanager/api"
	cmflags "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	registry "github.com/oasisprotocol/oasis-core/go/registry/api"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	runtimeHistory "github.com/oasisprotocol/oasis-core/go/runtime/history/api"
	scheduler "github.com/oasisprotocol/oasis-core/go/scheduler/api"
	staking "github.com/oasisprotocol/oasis-core/go/staking/api"
	"github.com/oasisprotocol/oasis-core/go/storage/mkvs/syncer"
)

const (
	// This should ideally be dynamically configured internally by tendermint:
	// https://github.com/tendermint/tendermint/issues/3523
	// This is set to the same value as in tendermint.
	tendermintSeedDisconnectWaitPeriod = 28 * time.Hour

	// CfgDebugDisableAddrBookFromGenesis disables populating seed node address book from genesis.
	// This flag is used to disable initial addr book population from genesis in some E2E tests to
	// test the seed node functionality.
	CfgDebugDisableAddrBookFromGenesis = "consensus.tendermint.seed.debug.disable_addr_book_from_genesis"
)

// Flags has the configuration flags.
var Flags = flag.NewFlagSet("", flag.ContinueOnError)

type seedService struct {
	identity *identity.Identity

	doc *genesis.Document

	addr      *p2p.NetAddress
	transport *p2p.MultiplexTransport
	addrBook  pex.AddrBook
	p2pSwitch *p2p.Switch

	stopOnce sync.Once
	quitCh   chan struct{}
}

// Name returns the service name.
func (srv *seedService) Name() string {
	return "tendermint/seed"
}

// Start starts the service.
func (srv *seedService) Start() error {
	if err := srv.transport.Listen(*srv.addr); err != nil {
		return fmt.Errorf("tendermint/seed: failed to listen on transport: %w", err)
	}

	// Start switch.
	if err := srv.p2pSwitch.Start(); err != nil {
		return fmt.Errorf("tendermint/seed: failed to start P2P switch: %w", err)
	}

	return nil
}

// Stop halts the service.
func (srv *seedService) Stop() {
	srv.stopOnce.Do(func() {
		close(srv.quitCh)
		// Save the address book.
		if srv.addrBook != nil {
			srv.addrBook.Save()
		}

		// Stop the switch.
		if srv.p2pSwitch != nil {
			_ = srv.p2pSwitch.Stop()
			srv.p2pSwitch.Wait()
		}
	})
}

// Quit returns a channel that will be closed when the service terminates.
func (srv *seedService) Quit() <-chan struct{} {
	return srv.quitCh
}

// Cleanup performs the service specific post-termination cleanup.
func (srv *seedService) Cleanup() {
	// No cleanup in particular.
}

// Implements Backend.
func (srv *seedService) Synced() <-chan struct{} {
	// Seed is always considered synced.
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Implements Backend.
func (srv *seedService) SupportedFeatures() consensus.FeatureMask {
	return consensus.FeatureMask(0)
}

// Implements Backend.
func (srv *seedService) GetStatus(ctx context.Context) (*consensus.Status, error) {
	status := &consensus.Status{
		Version:  version.ConsensusProtocol,
		Backend:  api.BackendName,
		Features: srv.SupportedFeatures(),
	}

	// List of consensus peers.
	tmpeers := srv.p2pSwitch.Peers().List()
	peers := make([]string, 0, len(tmpeers))
	for _, tmpeer := range tmpeers {
		p := string(tmpeer.ID()) + "@" + tmpeer.RemoteAddr().String()
		peers = append(peers, p)
	}
	status.NodePeers = peers

	return status, nil
}

// Implements Backend.
func (srv *seedService) GetGenesisDocument(ctx context.Context) (*genesis.Document, error) {
	return srv.doc, nil
}

// Implements Backend.
func (srv *seedService) GetChainContext(ctx context.Context) (string, error) {
	return srv.doc.ChainContext(), nil
}

// Implements Backend.
func (srv *seedService) GetAddresses() ([]node.ConsensusAddress, error) {
	u, err := tmcommon.GetExternalAddress()
	if err != nil {
		return nil, err
	}

	var addr node.ConsensusAddress
	if err = addr.Address.UnmarshalText([]byte(u.Host)); err != nil {
		return nil, fmt.Errorf("tendermint: failed to parse external address host: %w", err)
	}
	addr.ID = srv.identity.P2PSigner.Public()

	return []node.ConsensusAddress{addr}, nil
}

// Implements Backend.
func (srv *seedService) SubmitEvidence(ctx context.Context, evidence *consensus.Evidence) error {
	return consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) SubmitTx(ctx context.Context, tx *transaction.SignedTransaction) error {
	return consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) StateToGenesis(ctx context.Context, height int64) (*genesis.Document, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) EstimateGas(ctx context.Context, req *consensus.EstimateGasRequest) (transaction.Gas, error) {
	return 0, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetBlock(ctx context.Context, height int64) (*consensus.Block, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetTransactions(ctx context.Context, height int64) ([][]byte, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetTransactionsWithResults(ctx context.Context, height int64) (*consensus.TransactionsWithResults, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetUnconfirmedTransactions(ctx context.Context) ([][]byte, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) WatchBlocks(ctx context.Context) (<-chan *consensus.Block, pubsub.ClosableSubscription, error) {
	return nil, nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetSignerNonce(ctx context.Context, req *consensus.GetSignerNonceRequest) (uint64, error) {
	return 0, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetLightBlock(ctx context.Context, height int64) (*consensus.LightBlock, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) GetParameters(ctx context.Context, height int64) (*consensus.Parameters, error) {
	return nil, consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) State() syncer.ReadSyncer {
	return syncer.NopReadSyncer
}

// Implements Backend.
func (srv *seedService) ConsensusKey() signature.PublicKey {
	return srv.identity.ConsensusSigner.Public()
}

// Implements Backend.
func (srv *seedService) SubmitTxNoWait(ctx context.Context, tx *transaction.SignedTransaction) error {
	return consensus.ErrUnsupported
}

// Implements Backend.
func (srv *seedService) RegisterHaltHook(consensus.HaltHook) {
	panic(consensus.ErrUnsupported)
}

// Note: SupportedFeatures() indicates that the backend does not support
// consensus services so the caller is at fault for not adhering to the
// SupportedFeatures flag, in case any of the following methods is called.

// Implements Backend.
func (srv *seedService) Beacon() beacon.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) KeyManager() keymanager.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) Registry() registry.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) RootHash() roothash.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) Staking() staking.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) Scheduler() scheduler.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) Governance() governance.Backend {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) SubmissionManager() consensus.SubmissionManager {
	panic(consensus.ErrUnsupported)
}

// Implements Backend.
func (srv *seedService) TrackRuntime(history runtimeHistory.BlockHistory) error {
	panic(consensus.ErrUnsupported)
}

// New creates a new seed-only consensus service.
func New(dataDir string, identity *identity.Identity, genesisProvider genesis.Provider) (consensus.Backend, error) {
	var err error

	// This is heavily inspired by https://gitlab.com/polychainlabs/tenderseed
	// and reaches into tendermint to spin up the minimum components required
	// to get the PEX reactor to operate in seed mode.

	srv := &seedService{
		quitCh:   make(chan struct{}),
		identity: identity,
	}

	seedDataDir := filepath.Join(dataDir, "tendermint-seed")
	if err = tmcommon.InitDataDir(seedDataDir); err != nil {
		return nil, fmt.Errorf("tendermint/seed: failed to initialize data dir: %w", err)
	}

	p2pCfg := config.DefaultP2PConfig()
	p2pCfg.SeedMode = true
	p2pCfg.Seeds = strings.ToLower(strings.Join(viper.GetStringSlice(tmcommon.CfgP2PSeed), ","))
	p2pCfg.ExternalAddress = viper.GetString(tmcommon.CfgCoreExternalAddress)
	p2pCfg.MaxNumInboundPeers = viper.GetInt(tmcommon.CfgP2PMaxNumInboundPeers)
	p2pCfg.MaxNumOutboundPeers = viper.GetInt(tmcommon.CfgP2PMaxNumOutboundPeers)
	p2pCfg.SendRate = viper.GetInt64(tmcommon.CfgP2PSendRate)
	p2pCfg.RecvRate = viper.GetInt64(tmcommon.CfgP2PRecvRate)
	p2pCfg.AddrBookStrict = !(viper.GetBool(tmcommon.CfgDebugP2PAddrBookLenient) && cmflags.DebugDontBlameOasis())
	p2pCfg.AllowDuplicateIP = viper.GetBool(tmcommon.CfgDebugP2PAllowDuplicateIP) && cmflags.DebugDontBlameOasis()

	nodeKey := &p2p.NodeKey{PrivKey: crypto.SignerToTendermint(identity.P2PSigner)}

	doc, err := genesisProvider.GetGenesisDocument()
	if err != nil {
		return nil, fmt.Errorf("tendermint/seed: failed to get genesis document: %w", err)
	}
	srv.doc = doc

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(
			tmversion.P2PProtocol,
			tmversion.BlockProtocol,
			version.TendermintAppVersion,
		),
		DefaultNodeID: nodeKey.ID(),
		ListenAddr:    viper.GetString(tmcommon.CfgCoreListenAddress),
		Network:       doc.ChainContext()[:types.MaxChainIDLen],
		Version:       tmversion.TMCoreSemVer,
		Channels:      []byte{pex.PexChannel},
		Moniker:       "oasis-seed-" + identity.P2PSigner.Public().String(),
	}

	// Carve out all of the services.
	logger := tmcommon.NewLogAdapter(!viper.GetBool(tmcommon.CfgLogDebug))
	if srv.addr, err = p2p.NewNetAddressString(p2p.IDAddressString(nodeInfo.DefaultNodeID, nodeInfo.ListenAddr)); err != nil {
		return nil, fmt.Errorf("tendermint/seed: failed to create seed address: %w", err)
	}
	srv.transport = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(p2pCfg))

	addrBookPath := filepath.Join(seedDataDir, tmcommon.ConfigDir, "addrbook.json")
	srv.addrBook = pex.NewAddrBook(addrBookPath, p2pCfg.AddrBookStrict)
	srv.addrBook.SetLogger(logger.With("module", "book"))
	if err = srv.addrBook.Start(); err != nil {
		return nil, fmt.Errorf("tendermint/seed: failed to start address book: %w", err)
	}

	if !(viper.GetBool(CfgDebugDisableAddrBookFromGenesis) && cmflags.DebugDontBlameOasis()) {
		if err = populateAddrBookFromGenesis(srv.addrBook, doc, srv.addr); err != nil {
			return nil, fmt.Errorf("tendermint/seed: failed to populate address book from genesis: %w", err)
		}
	}

	// Use p2pCfg.Seeds since there the IDs are already lowercased.
	// Use FieldsFunc so that empty string is handled correctly.
	seeds := strings.FieldsFunc(
		p2pCfg.Seeds,
		func(c rune) bool {
			return c == ','
		},
	)
	pexReactor := pex.NewReactor(srv.addrBook, &pex.ReactorConfig{
		SeedMode:                 p2pCfg.SeedMode,
		Seeds:                    seeds,
		SeedDisconnectWaitPeriod: tendermintSeedDisconnectWaitPeriod,
	})
	pexReactor.SetLogger(logger.With("module", "pex"))

	srv.p2pSwitch = p2p.NewSwitch(p2pCfg, srv.transport)
	srv.p2pSwitch.SetLogger(logger.With("module", "switch"))
	srv.p2pSwitch.SetNodeKey(nodeKey)
	srv.p2pSwitch.SetAddrBook(srv.addrBook)
	srv.p2pSwitch.AddReactor("pex", pexReactor)
	srv.p2pSwitch.SetNodeInfo(nodeInfo)

	return srv, nil
}

func populateAddrBookFromGenesis(addrBook p2p.AddrBook, doc *genesis.Document, ourAddr *p2p.NetAddress) error {
	logger := logging.GetLogger("consensus/tendermint/seed")

	// Convert to a representation suitable for address book population.
	var addrs []*p2p.NetAddress
	for _, v := range doc.Registry.Nodes {
		var openedNode node.Node
		if err := v.Open(registry.RegisterGenesisNodeSignatureContext, &openedNode); err != nil {
			return fmt.Errorf("tendermint/seed: failed to verify validator: %w", err)
		}
		// TODO: This should cross check that the entity is valid.
		if !openedNode.HasRoles(node.RoleValidator) {
			continue
		}

		var tmvAddr *p2p.NetAddress
		tmvAddr, err := api.NodeToP2PAddr(&openedNode)
		if err != nil {
			logger.Error("failed to reformat genesis validator address",
				"err", err,
			)
			continue
		}

		addrs = append(addrs, tmvAddr)
	}

	// Populate the address book with the genesis validators.
	addrBook.AddOurAddress(ourAddr) // Required or AddrBook.AddAddress will fail.
	for _, v := range addrs {
		// Remove the address first as otherwise Tendermint's address book
		// may not actually add the new address.
		addrBook.RemoveAddress(v)

		if err := addrBook.AddAddress(v, ourAddr); err != nil {
			logger.Error("failed to add genesis validator to address book",
				"err", err,
			)
		}
	}

	return nil
}

func init() {
	Flags.Bool(CfgDebugDisableAddrBookFromGenesis, false, "disable populating address book with genesis validators")

	_ = Flags.MarkHidden(CfgDebugDisableAddrBookFromGenesis)

	_ = viper.BindPFlags(Flags)
}
