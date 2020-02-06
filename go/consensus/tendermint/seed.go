package tendermint

import (
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/pex"
	"github.com/tendermint/tendermint/types"
	"github.com/tendermint/tendermint/version"

	"github.com/oasislabs/oasis-core/go/common/cbor"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/identity"
	"github.com/oasislabs/oasis-core/go/common/node"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/api"
	"github.com/oasislabs/oasis-core/go/consensus/tendermint/crypto"
	genesis "github.com/oasislabs/oasis-core/go/genesis/api"
)

// SeedService is a Tendermint seed service.
type SeedService struct {
	addr      *p2p.NetAddress
	transport *p2p.MultiplexTransport
	addrBook  pex.AddrBook
	p2pSwitch *p2p.Switch

	stopOnce sync.Once
	quitCh   chan struct{}
}

// Name returns the service name.
func (srv *SeedService) Name() string {
	return "tendermint/seed"
}

// Start starts the service.
func (srv *SeedService) Start() error {
	if err := srv.transport.Listen(*srv.addr); err != nil {
		return errors.Wrap(err, "tendermint/seed: failed to listen on transport")
	}

	// Start switch.
	if err := srv.p2pSwitch.Start(); err != nil {
		return errors.Wrap(err, "tendermint/seed: failed to start P2P switch")
	}

	return nil
}

// Stop halts the service.
func (srv *SeedService) Stop() {
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

// Quit reuturns a channel that will be clsoed when the service terminates.
func (srv *SeedService) Quit() <-chan struct{} {
	return srv.quitCh
}

// Cleanup performs the service specific post-termination cleanup.
func (srv *SeedService) Cleanup() {
	// No cleanup in particular.
}

// NewSeed creates a new Tendermint seed service.
func NewSeed(dataDir string, identity *identity.Identity, genesisProvider genesis.Provider) (*SeedService, error) {
	var err error

	// This is heavily inspired by https://gitlab.com/polychainlabs/tenderseed
	// and reaches into tendermint to spin up the minimum components requried
	// to get the PEX reactor to operate in seed mode.

	srv := &SeedService{
		quitCh: make(chan struct{}),
	}

	seedDataDir := filepath.Join(dataDir, "tendermint-seed")
	if err = initDataDir(seedDataDir); err != nil {
		return nil, errors.Wrap(err, "tendermint/seed: failed to initialize data dir")
	}

	cfg := config.DefaultP2PConfig()
	cfg.AllowDuplicateIP = true
	cfg.SeedMode = true
	cfg.AddrBookStrict = !viper.GetBool(CfgDebugP2PAddrBookLenient)
	// MaxNumInboundPeers/MaxNumOutboundPeers

	unsafeNodeSigner, ok := identity.NodeSigner.(signature.UnsafeSigner)
	if !ok {
		return nil, errors.New("tendermint/seed: node signer does not allow private key access")
	}
	nodeKey := &p2p.NodeKey{PrivKey: crypto.UnsafeSignerToTendermint(unsafeNodeSigner)}

	doc, err := genesisProvider.GetGenesisDocument()
	if err != nil {
		return nil, errors.Wrap(err, "tendermint/seed: failed to get genesis document")
	}

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(
			version.P2PProtocol,
			version.BlockProtocol,
			0,
		),
		ID_:        nodeKey.ID(),
		ListenAddr: viper.GetString(CfgCoreListenAddress),
		Network:    doc.ChainContext()[:types.MaxChainIDLen],
		Version:    "0.0.1",
		Channels:   []byte{pex.PexChannel},
		Moniker:    "oasis-seed-" + identity.NodeSigner.Public().String(),
	}

	// Carve out all of the services.
	logger := newLogAdapter(!viper.GetBool(cfgLogDebug))
	if srv.addr, err = p2p.NewNetAddressString(p2p.IDAddressString(nodeInfo.ID_, nodeInfo.ListenAddr)); err != nil {
		return nil, errors.Wrap(err, "tendermint/seed: failed to create seed address")
	}
	srv.transport = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, p2p.MConnConfig(cfg))

	addrBookPath := filepath.Join(seedDataDir, configDir, "addrbook.json")
	srv.addrBook = pex.NewAddrBook(addrBookPath, cfg.AddrBookStrict)
	srv.addrBook.SetLogger(logger.With("module", "book"))
	if err = srv.addrBook.Start(); err != nil {
		return nil, errors.Wrap(err, "tendermint/seed: failed to start address book")
	}
	if err = populateAddrBookFromGenesis(srv.addrBook, doc, srv.addr); err != nil {
		return nil, errors.Wrap(err, "tendermint/seed: failed to populate address book from genesis")
	}

	pexReactor := pex.NewPEXReactor(srv.addrBook, &pex.PEXReactorConfig{SeedMode: cfg.SeedMode})
	pexReactor.SetLogger(logger.With("module", "pex"))

	srv.p2pSwitch = p2p.NewSwitch(cfg, srv.transport)
	srv.p2pSwitch.SetLogger(logger.With("module", "switch"))
	srv.p2pSwitch.SetNodeKey(nodeKey)
	srv.p2pSwitch.SetAddrBook(srv.addrBook)
	srv.p2pSwitch.AddReactor("pex", pexReactor)
	srv.p2pSwitch.SetNodeInfo(nodeInfo)

	return srv, nil
}

func populateAddrBookFromGenesis(addrBook p2p.AddrBook, doc *genesis.Document, ourAddr *p2p.NetAddress) error {

	// Convert to a representation suitable for address book population.
	var addrs []*p2p.NetAddress
	for _, v := range doc.Registry.Nodes {
		var openedNode node.Node
		if err := cbor.Unmarshal(v.Blob, &openedNode); err != nil {
			return errors.Wrap(err, "tendermint/seed: failed to unmarshal validator")
		}
		// TODO: This should cross check that the entity is valid.
		if !openedNode.HasRoles(node.RoleValidator) {
			continue
		}

		var tmvAddr *p2p.NetAddress
		tmvAddr, err := api.NodeToP2PAddr(&openedNode)
		if err != nil {
			return errors.Wrap(err, "tendermint/seed: failed to reformat genesis validator address")
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
			return errors.Wrap(err, "tendermint/seed: failed to add genesis validator to address book")
		}
	}

	return nil
}
