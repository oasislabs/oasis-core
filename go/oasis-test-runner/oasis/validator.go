package oasis

import (
	"crypto/ed25519"
	"fmt"
	netPkg "net"
	"path/filepath"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/crypto"
	cmdCommon "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common"
	cmdRegNode "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/registry/node"
)

const validatorIdentitySeedTemplate = "ekiden node validator %d"

// Validator is an Oasis validator.
type Validator struct {
	Node

	entity *Entity

	sentries []*Sentry

	tmAddress     string
	sentryPubKey  signature.PublicKey
	consensusPort uint16
	clientPort    uint16
}

// ValidatorCfg is the Oasis validator provisioning configuration.
type ValidatorCfg struct {
	NodeCfg

	Entity *Entity

	Sentries []*Sentry
}

func (val *Validator) toGenesisArgs() []string {
	return []string{
		"--node", val.descriptorPath(),
	}
}

func (val *Validator) descriptorPath() string {
	return filepath.Join(val.dir.String(), "node_genesis.json")
}

// IdentityKeyPath returns the path to the node's identity key.
func (val *Validator) IdentityKeyPath() string {
	return nodeIdentityKeyPath(val.dir)
}

// P2PKeyPath returns the path to the node's P2P key.
func (val *Validator) P2PKeyPath() string {
	return nodeP2PKeyPath(val.dir)
}

// ConsensusKeyPath returns the path to the node's consensus key.
func (val *Validator) ConsensusKeyPath() string {
	return nodeConsensusKeyPath(val.dir)
}

// Exports path returns the path to the node's exports data dir.
func (val *Validator) ExportsPath() string {
	return nodeExportsPath(val.dir)
}

// ExternalGRPCAddress returns the address of the node's external gRPC server.
func (val *Validator) ExternalGRPCAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", val.clientPort)
}

// Start starts an Oasis node.
func (val *Validator) Start() error {
	return val.startNode()
}

func (val *Validator) startNode() error {
	args := newArgBuilder().
		debugDontBlameOasis().
		debugAllowTestKeys().
		workerCertificateRotation(true).
		consensusValidator().
		tendermintCoreAddress(val.consensusPort).
		tendermintMinGasPrice(val.consensus.MinGasPrice).
		tendermintSubmissionGasPrice(val.consensus.SubmissionGasPrice).
		tendermintPrune(val.consensus.PruneNumKept).
		tendermintRecoverCorruptedWAL(val.consensus.TendermintRecoverCorruptedWAL).
		appendNetwork(val.net).
		appendEntity(val.entity)

	if len(val.sentries) > 0 {
		args = args.addSentries(val.sentries).
			tendermintDisablePeerExchange()
	} else {
		args = args.appendSeedNodes(val.net.seeds)
	}
	if val.consensus.EnableConsensusRPCWorker {
		args = args.workerClientPort(val.clientPort).
			workerConsensusRPCEnabled()
	}

	if !val.consensus.DisableSupplementarySanityChecks {
		if len(val.net.validators) >= 1 && val == val.net.validators[0] {
			args = args.tendermintSupplementarySanityEnabled()
		}
	}

	if err := val.net.startOasisNode(&val.Node, nil, args); err != nil {
		return fmt.Errorf("oasis/validator: failed to launch node %s: %w", val.Name, err)
	}

	return nil
}

// NewValidator provisions a new validator and adds it to the network.
func (net *Network) NewValidator(cfg *ValidatorCfg) (*Validator, error) {
	valName := fmt.Sprintf("validator-%d", len(net.validators))

	valDir, err := net.baseDir.NewSubDir(valName)
	if err != nil {
		net.logger.Error("failed to create validator subdir",
			"err", err,
			"validator_name", valName,
		)
		return nil, fmt.Errorf("oasis/validator: failed to create validator subdir: %w", err)
	}

	val := &Validator{
		Node: Node{
			Name:                                     valName,
			net:                                      net,
			dir:                                      valDir,
			termEarlyOk:                              cfg.AllowEarlyTermination,
			termErrorOk:                              cfg.AllowErrorTermination,
			disableDefaultLogWatcherHandlerFactories: cfg.DisableDefaultLogWatcherHandlerFactories,
			logWatcherHandlerFactories:               cfg.LogWatcherHandlerFactories,
			consensus:                                cfg.Consensus,
			noAutoStart:                              cfg.NoAutoStart,
		},
		entity:        cfg.Entity,
		sentries:      cfg.Sentries,
		consensusPort: net.nextNodePort,
		clientPort:    net.nextNodePort + 1,
	}
	val.doStartNode = val.startNode

	var consensusAddrs []interface{ String() string }
	localhost := netPkg.ParseIP("127.0.0.1")
	if len(val.sentries) > 0 {
		for _, sentry := range val.sentries {
			var consensusAddr node.ConsensusAddress
			consensusAddr.ID = sentry.p2pPublicKey
			if err = consensusAddr.Address.FromIP(localhost, sentry.consensusPort); err != nil {
				return nil, fmt.Errorf("oasis/validator: failed to parse IP address: %w", err)
			}
			consensusAddrs = append(consensusAddrs, &consensusAddr)
		}
	} else {
		var consensusAddr node.Address
		if err = consensusAddr.FromIP(localhost, val.consensusPort); err != nil {
			return nil, fmt.Errorf("oasis/validator: failed to parse IP address: %w", err)
		}
		consensusAddrs = append(consensusAddrs, &consensusAddr)
	}

	// Load node's identity, so that we can pass the validator's Tendermint
	// address to sentry node(s) to configure it as a private peer.
	seed := fmt.Sprintf(validatorIdentitySeedTemplate, len(net.validators))
	valNodeKey, valP2PKey, sentryClientCert, err := net.provisionNodeIdentity(valDir, seed, false)
	if err != nil {
		return nil, fmt.Errorf("oasis/validator: failed to provision node identity: %w", err)
	}
	copy(val.NodeID[:], valNodeKey[:])
	val.tmAddress = crypto.PublicKeyToTendermint(&valP2PKey).Address().String()
	if err = cfg.Entity.addNode(val.NodeID); err != nil {
		return nil, err
	}

	// Sentry client cert.
	pk, ok := sentryClientCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("oasis/validator: bad sentry client public key type (expected: Ed25519 got: %T)", sentryClientCert.PublicKey)
	}
	if err = val.sentryPubKey.UnmarshalBinary(pk[:]); err != nil {
		return nil, fmt.Errorf("oasis/validator: sentry client public key unmarshal failure: %w", err)
	}

	args := []string{
		"registry", "node", "init",
		"--" + cmdCommon.CfgDataDir, val.dir.String(),
		"--" + cmdRegNode.CfgExpiration, "1",
		"--" + cmdRegNode.CfgRole, "validator",
	}
	for _, v := range consensusAddrs {
		args = append(args, []string{"--" + cmdRegNode.CfgConsensusAddress, v.String()}...)
	}
	args = append(args, cfg.Entity.toGenesisArgs()...)

	w, err := val.dir.NewLogWriter("provision.log")
	if err != nil {
		return nil, err
	}
	defer w.Close()

	if err = net.runNodeBinary(w, args...); err != nil {
		net.logger.Error("failed to provision validator",
			"err", err,
			"validator_name", valName,
		)
		return nil, fmt.Errorf("oasis/validator: failed to provision validator: %w", err)
	}

	net.validators = append(net.validators, val)
	net.nextNodePort += 2

	if err := net.AddLogWatcher(&val.Node); err != nil {
		net.logger.Error("failed to add log watcher",
			"err", err,
			"validator_name", valName,
		)
		return nil, fmt.Errorf("oasis/validator: failed to add log watcher for %s: %w", valName, err)
	}

	return val, nil
}
