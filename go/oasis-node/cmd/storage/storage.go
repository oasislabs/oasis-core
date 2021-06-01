// Package storage implements the storage sub-commands.
package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	cmdCommon "github.com/oasisprotocol/oasis-core/go/oasis-node/cmd/common"
	roothash "github.com/oasisprotocol/oasis-core/go/roothash/api"
	"github.com/oasisprotocol/oasis-core/go/runtime/history"
	"github.com/oasisprotocol/oasis-core/go/runtime/registry"
	db "github.com/oasisprotocol/oasis-core/go/storage/mkvs/db/api"
	"github.com/oasisprotocol/oasis-core/go/storage/mkvs/db/badger"
	"github.com/oasisprotocol/oasis-core/go/storage/mkvs/node"
	workerStorage "github.com/oasisprotocol/oasis-core/go/worker/storage"
)

var (
	storageCmd = &cobra.Command{
		Use:   "storage",
		Short: "storage node utilities",
	}

	storageMigrateCmd = &cobra.Command{
		Use:   "migrate",
		Short: "perform node database migration",
		RunE:  doMigrate,
	}

	storageCheckCmd = &cobra.Command{
		Use:   "check",
		Short: "check node databases for consistency",
		RunE:  doCheck,
	}

	storageRenameNsCmd = &cobra.Command{
		Use:   "rename-ns <src-ns> <dst-ns>",
		Short: "change the namespace of a runtime database",
		RunE:  doRenameNs,
	}

	logger = logging.GetLogger("cmd/storage")

	pretty = cmdCommon.Isatty(1)
)

type displayHelper struct {
	lastTime     time.Time
	lastStatus   string
	lastProgress bool
}

func (dh *displayHelper) display(base, format string, args ...interface{}) {
	dh.lastTime = time.Time{}
	if pretty {
		if dh.lastProgress {
			fmt.Printf("\n"+format, args...)
		} else {
			fmt.Printf(format, args...)
		}
	} else {
		logger.Info(base)
	}
	dh.lastProgress = false
	dh.lastStatus = base
}

func (dh *displayHelper) Display(msg string) {
	dh.display(msg, "- %s\n", msg)
}

func (dh *displayHelper) DisplayStepBegin(msg string) {
	dh.display(msg, "- %s... ", msg)
}

func (dh *displayHelper) DisplayStepEnd(msg string) {
	dh.display(msg, "\r- %s: %s\n", dh.lastStatus, msg)
}

func (dh *displayHelper) DisplayStep(msg string) {
	dh.display(msg, "- %s...\n", msg)
}

func (dh *displayHelper) DisplayProgress(msg string, current, total uint64) {
	if pretty {
		if time.Since(dh.lastTime).Seconds() < 0.1 && current < total {
			return
		}
		dh.lastTime = time.Now()

		var leadin string
		if len(dh.lastStatus) > 0 {
			leadin = fmt.Sprintf("- %s:", dh.lastStatus)
		} else {
			leadin = "-"
		}
		fmt.Printf("\r%s %s %.2f%% (%d / %d)\033[K", leadin, msg, (float64(current)/float64(total))*100.0, current, total)
		dh.lastProgress = true
	}
}

type migrateHelper struct {
	displayHelper

	ctx     context.Context
	history history.History
	roots   map[hash.Hash]node.RootType
}

func (mh *migrateHelper) GetRootForHash(root hash.Hash, version uint64) ([]node.Root, error) {
	block, err := mh.history.GetBlock(mh.ctx, version)
	if err != nil {
		if errors.Is(err, roothash.ErrNotFound) {
			return nil, badger.ErrVersionNotFound
		}
		return nil, err
	}

	var roots []node.Root
	for _, blockRoot := range block.Block.Header.StorageRoots() {
		if blockRoot.Hash.Equal(&root) {
			roots = append(roots, blockRoot)
		}
	}
	return roots, nil
}

func doMigrate(cmd *cobra.Command, args []string) error {
	dataDir := cmdCommon.DataDir()
	ctx := context.Background()

	runtimes, err := registry.ParseRuntimeMap(viper.GetStringSlice(registry.CfgSupported))
	if err != nil {
		logger.Error("unable to enumerate configured runtimes", "err", err)
		return fmt.Errorf("unable to enumerate configured runtimes: %w", err)
	}

	for rt := range runtimes {
		if pretty {
			fmt.Printf(" ** Upgrading storage database for runtime %v...\n", rt)
		}
		err := func() error {
			runtimeDir := registry.GetRuntimeStateDir(dataDir, rt)

			history, err := history.New(runtimeDir, rt, nil)
			if err != nil {
				return fmt.Errorf("error creating history provider: %w", err)
			}
			defer history.Close()

			nodeCfg := &db.Config{
				DB:        workerStorage.GetLocalBackendDBDir(runtimeDir, viper.GetString(workerStorage.CfgBackend)),
				Namespace: rt,
			}

			helper := &migrateHelper{
				ctx:     ctx,
				history: history,
				roots:   map[hash.Hash]node.RootType{},
			}

			newVersion, err := badger.Migrate(nodeCfg, helper)
			if err != nil {
				return fmt.Errorf("node database migrator returned error: %w", err)
			}
			logger.Info("successfully migrated node database", "new_version", newVersion)
			return nil
		}()
		if err != nil {
			logger.Error("error upgrading runtime", "rt", rt, "err", err)
			if pretty {
				fmt.Printf("error upgrading runtime %v: %v\n", rt, err)
			}
			return fmt.Errorf("error upgrading runtime %v: %w", rt, err)
		}
	}
	return nil
}

func doCheck(cmd *cobra.Command, args []string) error {
	dataDir := cmdCommon.DataDir()
	ctx := context.Background()

	runtimes, err := registry.ParseRuntimeMap(viper.GetStringSlice(registry.CfgSupported))
	if err != nil {
		logger.Error("unable to enumerate configured runtimes", "err", err)
		return fmt.Errorf("unable to enumerate configured runtimes: %w", err)
	}

	for rt := range runtimes {
		if pretty {
			fmt.Printf("Checking storage database for runtime %v...\n", rt)
		}
		err := func() error {
			runtimeDir := registry.GetRuntimeStateDir(dataDir, rt)

			nodeCfg := &db.Config{
				DB:        workerStorage.GetLocalBackendDBDir(runtimeDir, viper.GetString(workerStorage.CfgBackend)),
				Namespace: rt,
			}

			display := &displayHelper{}

			err := badger.CheckSanity(ctx, nodeCfg, display)
			if err != nil {
				return fmt.Errorf("node database checker returned error: %w", err)
			}
			logger.Info("node database seems to be error-free", "rt", rt)
			return nil
		}()
		if err != nil {
			logger.Error("error checking node database", "rt", rt, "err", err)
			if pretty {
				fmt.Printf("error checking node database for runtime %v: %v\n", rt, err)
			}
			return fmt.Errorf("error checking node database for runtime %v: %w", rt, err)
		}
	}
	return nil
}

func doRenameNs(cmd *cobra.Command, args []string) error {
	dataDir := cmdCommon.DataDir()

	if len(args) != 2 {
		return fmt.Errorf("need exactly two arguments (source and destination runtime IDs)")
	}

	var srcID, dstID common.Namespace
	if err := srcID.UnmarshalHex(args[0]); err != nil {
		return fmt.Errorf("malformed source runtime ID: %s", args[0])
	}
	if err := dstID.UnmarshalHex(args[1]); err != nil {
		return fmt.Errorf("malformed source runtime ID: %s", args[0])
	}
	if pretty {
		fmt.Printf("Renaming storage database for runtime from %s to %s...\n", srcID, dstID)
	}

	srcDir := registry.GetRuntimeStateDir(dataDir, srcID)
	dstDir := registry.GetRuntimeStateDir(dataDir, dstID)

	nodeCfg := &db.Config{
		DB:        workerStorage.GetLocalBackendDBDir(srcDir, viper.GetString(workerStorage.CfgBackend)),
		Namespace: srcID,
	}

	err := badger.RenameNamespace(nodeCfg, dstID)
	if err != nil {
		return fmt.Errorf("failed to rename namespace: %w", err)
	}

	if err = os.Rename(srcDir, dstDir); err != nil {
		return fmt.Errorf("failed to move directory: %w", err)
	}

	// Remove history directory as that will be invalid now.
	if err = os.RemoveAll(filepath.Join(dstDir, history.DbFilename)); err != nil {
		return fmt.Errorf("failed to remove history directory: %w", err)
	}

	return nil
}

// Register registers the client sub-command and all of its children.
func Register(parentCmd *cobra.Command) {
	storageMigrateCmd.Flags().AddFlagSet(registry.Flags)
	storageCheckCmd.Flags().AddFlagSet(registry.Flags)
	storageCmd.AddCommand(storageMigrateCmd)
	storageCmd.AddCommand(storageCheckCmd)
	storageCmd.AddCommand(storageRenameNsCmd)
	parentCmd.AddCommand(storageCmd)
}
