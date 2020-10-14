package oasis

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	fileSigner "github.com/oasisprotocol/oasis-core/go/common/crypto/signature/signers/file"
	"github.com/oasisprotocol/oasis-core/go/common/entity"
	"github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common"
	"github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/flags"
	cmdSigner "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common/signer"
	cmdEntity "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/registry/entity"
	"github.com/oasisprotocol/oasis-core/go/oasis-test-runner/env"
)

const entityIdentitySeedTemplate = "oasis entity %d"

var entityArgsDebugTest = []string{
	"--" + flags.CfgDebugDontBlameOasis,
	"--" + flags.CfgDebugTestEntity,
	"--" + common.CfgDebugAllowTestKeys,
}

// Entity is an Oasis entity.
type Entity struct {
	net *Network
	dir *env.Dir

	entity       *entity.Entity
	entitySigner signature.Signer

	isDebugTestEntity bool

	nodes []signature.PublicKey
}

// EntityCfg is the Oasis entity provisioning configuration.
type EntityCfg struct {
	IsDebugTestEntity bool `json:"is_debug_test_entity"`
	Restore           bool `json:"restore"`
}

// Inner returns the actual Oasis entity and it's signer.
func (ent *Entity) Inner() (*entity.Entity, signature.Signer) {
	return ent.entity, ent.entitySigner
}

// EntityKeyPath returns the path to the entity private key.
func (ent *Entity) EntityKeyPath() string {
	if ent.isDebugTestEntity {
		return ""
	}
	return filepath.Join(ent.dir.String(), fileSigner.FileEntityKey)
}

// DescriptorPath returns the path to the entity descriptor.
func (ent *Entity) DescriptorPath() string {
	if ent.isDebugTestEntity {
		return ""
	}
	return filepath.Join(ent.dir.String(), "entity.json")
}

// Signer returns the entity signer.
func (ent *Entity) Signer() signature.Signer {
	return ent.entitySigner
}

func (ent *Entity) toGenesisArgs() []string {
	if ent.dir != nil {
		return []string{"--" + cmdSigner.CfgSigner, fileSigner.SignerName, "--" + cmdSigner.CfgCLISignerDir, ent.dir.String()}
	} else if ent.isDebugTestEntity {
		return entityArgsDebugTest
	}

	return nil
}

func (ent *Entity) toGenesisDescriptorArgs() []string {
	if ent.dir != nil {
		return []string{"--entity", filepath.Join(ent.dir.String(), "entity_genesis.json")}
	} else if ent.isDebugTestEntity {
		return entityArgsDebugTest
	}

	return nil
}

func (ent *Entity) update() error {
	if ent.isDebugTestEntity {
		return nil
	}

	args := []string{
		"registry", "entity", "update",
		"--" + cmdSigner.CfgSigner, fileSigner.SignerName,
		"--" + cmdSigner.CfgCLISignerDir, ent.dir.String(),
	}
	for _, n := range ent.nodes {
		args = append(args, "--"+cmdEntity.CfgNodeID, n.String())
	}

	w, err := ent.dir.NewLogWriter("update.log")
	if err != nil {
		return err
	}
	defer w.Close()

	if err = ent.net.runNodeBinary(w, args...); err != nil {
		ent.net.logger.Error("failed to update entity",
			"err", err,
		)
		return fmt.Errorf("oasis/entity: failed to update entity: %w", err)
	}

	return nil
}

func (ent *Entity) addNode(id signature.PublicKey) error {
	ent.nodes = append(ent.nodes, id)
	return ent.update()
}

// NewEntity provisions a new entity and adds it to the network.
func (net *Network) NewEntity(cfg *EntityCfg) (*Entity, error) {
	var ent *Entity
	if cfg.IsDebugTestEntity {
		ent = &Entity{
			isDebugTestEntity: true,
		}
		ent.entity, ent.entitySigner, _ = entity.TestEntity()
	} else {
		entName := fmt.Sprintf("entity-%d", len(net.entities))
		entityDir, err := net.baseDir.NewSubDir(entName)
		if err != nil {
			net.logger.Error("failed to create entity subdir",
				"err", err,
				"entity_name", entName,
			)
			return nil, fmt.Errorf("oasis/entity: failed to create entity subdir: %w", err)
		}

		var extraArgs []string
		switch {
		case cfg.Restore:
			// Restore an existing entity.
		case net.cfg.DeterministicIdentities:
			// Generate a deterministic entity.
			err = net.generateDeterministicIdentity(
				entityDir,
				fmt.Sprintf(entityIdentitySeedTemplate, len(net.entities)),
				[]signature.SignerRole{signature.SignerEntity},
			)
			if err != nil {
				return nil, fmt.Errorf("oasis/entity: failed to create deterministic identity: %w", err)
			}
			extraArgs = append(extraArgs, "--"+cmdEntity.CfgReuseSigner)

			fallthrough
		default:
			// Generate a fresh new entity.
			args := []string{
				"registry", "entity", "init",
				"--" + cmdSigner.CfgSigner, fileSigner.SignerName,
				"--" + cmdSigner.CfgCLISignerDir, entityDir.String(),
			}
			args = append(args, extraArgs...)

			var w io.WriteCloser
			w, err = entityDir.NewLogWriter("provision.log")
			if err != nil {
				return nil, err
			}
			defer w.Close()

			if err = net.runNodeBinary(w, args...); err != nil {
				net.logger.Error("failed to provision entity",
					"err", err,
					"entity_name", entName,
				)
				return nil, fmt.Errorf("oasis/entity: failed to provision entity: %w", err)
			}
		}

		ent = &Entity{
			net: net,
			dir: entityDir,
		}
		signerFactory, err := fileSigner.NewFactory(entityDir.String(), signature.SignerEntity)
		if err != nil {
			net.logger.Error("failed to create entity file signer factory",
				"err", err,
				"entity_name", entName,
			)
			return nil, fmt.Errorf("oasis/entity: failed to create entity file signer: %w", err)
		}
		ent.entity, ent.entitySigner, err = entity.Load(entityDir.String(), signerFactory)
		if err != nil {
			net.logger.Error("failed to load newly provisoned entity",
				"err", err,
				"entity_name", entName,
			)
			return nil, fmt.Errorf("oasis/entity: failed to load newly provisioned entity: %w", err)
		}
	}

	net.entities = append(net.entities, ent)

	return ent, nil
}
