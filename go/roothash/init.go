// Package roothash implements the root hash backend.
package roothash

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	epochtime "github.com/oasislabs/ekiden/go/epochtime/api"
	pb "github.com/oasislabs/ekiden/go/grpc/roothash"
	registry "github.com/oasislabs/ekiden/go/registry/api"
	"github.com/oasislabs/ekiden/go/roothash/api"
	"github.com/oasislabs/ekiden/go/roothash/api/block"
	"github.com/oasislabs/ekiden/go/roothash/memory"
	"github.com/oasislabs/ekiden/go/roothash/tendermint"
	scheduler "github.com/oasislabs/ekiden/go/scheduler/api"
	storage "github.com/oasislabs/ekiden/go/storage/api"
	"github.com/oasislabs/ekiden/go/tendermint/service"
)

const (
	cfgBackend       = "roothash.backend"
	cfgGenesisBlocks = "roothash.genesis-blocks"
)

var (
	flagBackend       string
	flagGenesisBlocks string
)

// New constructs a new Backend based on the configuration flags.
func New(
	cmd *cobra.Command,
	timeSource epochtime.Backend,
	scheduler scheduler.Backend,
	storage storage.Backend,
	registry registry.Backend,
	tmService service.TendermintService,
) (api.Backend, error) {
	backend, _ := cmd.Flags().GetString(cfgBackend)

	genesisBlocks := make(map[signature.MapKey]*block.Block)
	genesisBlocksFilename, _ := cmd.Flags().GetString(cfgGenesisBlocks)
	if genesisBlocksFilename != "" {
		genesisBlocksRaw, err := ioutil.ReadFile(genesisBlocksFilename)
		if err != nil {
			return nil, err
		}
		pbGenesisBlocks := &pb.GenesisBlocks{}
		if err := proto.Unmarshal(genesisBlocksRaw, pbGenesisBlocks); err != nil {
			return nil, err
		}
		for _, genesisBlock := range pbGenesisBlocks.GenesisBlocks {
			var id signature.PublicKey
			if err := id.UnmarshalBinary(genesisBlock.GetRuntimeId()); err != nil {
				return nil, err
			}
			var apiBlock block.Block
			if err := apiBlock.FromProto(genesisBlock.GetBlock()); err != nil {
				return nil, err
			}
			genesisBlocks[id.ToMapKey()] = &apiBlock
		}
	}

	var impl api.Backend
	var err error

	switch strings.ToLower(backend) {
	case memory.BackendName:
		impl = memory.New(scheduler, storage, registry, genesisBlocks)
	case tendermint.BackendName:
		impl, err = tendermint.New(timeSource, scheduler, storage, tmService, genesisBlocks)
	default:
		return nil, fmt.Errorf("roothash: unsupported backend: '%v'", backend)
	}
	if err != nil {
		return nil, err
	}

	return newMetricsWrapper(impl), nil
}

// RegisterFlags registers the configuration flags with the provided
// command.
func RegisterFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&flagBackend, cfgBackend, memory.BackendName, "Root hash backend")
	cmd.Flags().StringVar(&flagGenesisBlocks, cfgGenesisBlocks, "", "File with serialized genesis blocks")

	for _, v := range []string{
		cfgBackend,
		cfgGenesisBlocks,
	} {
		viper.BindPFlag(v, cmd.Flags().Lookup(v)) // nolint: errcheck
	}
}
