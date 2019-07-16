package genesis

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oasislabs/ekiden/go/common"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	fileSigner "github.com/oasislabs/ekiden/go/common/crypto/signature/signers/file"
	"github.com/oasislabs/ekiden/go/common/entity"
	"github.com/oasislabs/ekiden/go/common/identity"
	"github.com/oasislabs/ekiden/go/common/json"
	cmdCommon "github.com/oasislabs/ekiden/go/ekiden/cmd/common"
	"github.com/oasislabs/ekiden/go/ekiden/cmd/common/flags"
	genesis "github.com/oasislabs/ekiden/go/genesis/api"
)

const (
	cfgNodeName      = "node_name"
	cfgNodeAddr      = "node_addr"
	cfgValidatorFile = "validator_file"
)

var (
	provisionValidatorCmd = &cobra.Command{
		Use:   "provision_validator",
		Short: "provision a validator node",
		PreRun: func(cmd *cobra.Command, args []string) {
			registerProvisionValidatorFlags(cmd)
		},
		Run: doProvisionValidator,
	}
)

func doProvisionValidator(cmd *cobra.Command, args []string) {
	if err := cmdCommon.Init(); err != nil {
		cmdCommon.EarlyLogAndExit(err)
	}

	dataDir := cmdCommon.DataDir()
	if dataDir == "" {
		logger.Error("data directory not configured")
		os.Exit(1)
	}

	// TODO/hsm: Configure factory dynamically.
	nodeSignerFactory := fileSigner.NewFactory(dataDir, signature.SignerNode, signature.SignerP2P)
	id, err := identity.LoadOrGenerate(dataDir, nodeSignerFactory)
	if err != nil {
		logger.Error("failed to load or generate node identity",
			"err", err,
		)
		os.Exit(1)
	}

	ent, signer, _, err := loadEntity(viper.GetString(cfgEntity))
	if err != nil {
		logger.Error("failed to load owning entity",
			"err", err,
		)
		os.Exit(1)
	}

	validator := genesis.Validator{
		EntityID: ent.ID,
		PubKey:   id.NodeSigner.Public(),
		Power:    10,
	}

	// Validate the command line args.
	nodeName := viper.GetString(cfgNodeName)
	if err = common.IsFQDN(nodeName); err != nil {
		logger.Error("malformed node name",
			"err", err,
			"node_name", nodeName,
		)
		os.Exit(1)
	}

	nodeAddr := viper.GetString(cfgNodeAddr)
	if err = common.IsAddrPort(nodeAddr); err != nil {
		logger.Error("malformed node address",
			"err", err,
			"node_addr", nodeAddr,
		)
		os.Exit(1)
	}

	// Populate the validator struct.
	validator.CoreAddress = nodeAddr
	validator.Name = common.NormalizeFQDN(nodeName)

	// Sign the validator.
	// TODO: This should use subSigners[entity.SubkeyNodeRegistration]
	signedValidator, err := genesis.SignValidator(signer, &validator)
	if err != nil {
		logger.Error("failed to sign validator",
			"err", err,
		)
		os.Exit(1)
	}

	// Write out the validator json to disk.
	f := viper.GetString(cfgValidatorFile)
	if f == "" {
		f = "validator-" + id.NodeSigner.Public().String() + ".json"
	}
	if !filepath.IsAbs(f) {
		f = filepath.Join(dataDir, f)
	}
	b := json.Marshal(signedValidator)
	if err = ioutil.WriteFile(f, b, 0600); err != nil {
		logger.Error("failed to write validator identity file",
			"err", err,
		)
		os.Exit(1)
	}
}

func loadEntity(dataDir string) (*entity.Entity, signature.Signer, map[entity.SubkeyRole]signature.Signer, error) {
	if flags.DebugTestEntity() {
		return entity.TestEntity()
	}

	// TODO/hsm: Configure factory dynamically.
	entitySignerFactory := fileSigner.NewFactory(dataDir, signature.SignerEntity, signature.SignerEntityNodeRegistration)
	return entity.Load(dataDir, entitySignerFactory)
}

func registerProvisionValidatorFlags(cmd *cobra.Command) {
	if !cmd.Flags().Parsed() {
		cmd.Flags().String(cfgNodeName, "", "validator node name")
		cmd.Flags().String(cfgNodeAddr, "", "validator node core address")
		cmd.Flags().String(cfgValidatorFile, "", "validator identity file")
		cmd.Flags().String(cfgEntity, "", "Path to directory containing entity private key and descriptor")
	}

	for _, v := range []string{
		cfgNodeAddr,
		cfgNodeName,
		cfgValidatorFile,
		cfgEntity,
	} {
		_ = viper.BindPFlag(v, cmd.Flags().Lookup(v))
	}
}

func initProvisionValidatorCmd(parentCmd *cobra.Command) {
	registerProvisionValidatorFlags(provisionValidatorCmd)
	flags.RegisterDebugTestEntity(provisionValidatorCmd)

	parentCmd.AddCommand(provisionValidatorCmd)
}
