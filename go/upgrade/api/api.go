// Package api defines the interface exporting the upgrade infrastructure's functionality.
package api

import (
	"context"
	"fmt"
	"io"

	beacon "github.com/oasisprotocol/oasis-core/go/beacon/api"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/prettyprint"
	"github.com/oasisprotocol/oasis-core/go/common/version"
)

const (
	// ModuleName is the upgrade module name.
	ModuleName = "upgrade"

	// LogEventIncompatibleBinary is a log event value that signals the currently running version
	// of the binary is incompatible with the upgrade.
	LogEventIncompatibleBinary = "upgrade/incompatible-binary"
	// LogEventStartupUpgrade is a log event value that signals the startup upgrade handler was
	// called.
	LogEventStartupUpgrade = "upgrade/startup-upgrade"
	// LogEventConsensusUpgrade is a log event value that signals the consensus upgrade handler was
	// called.
	LogEventConsensusUpgrade = "upgrade/consensus-upgrade"
)

// UpgradeStage is used in the upgrade descriptor to store completed stages.
type UpgradeStage uint64

const (
	// UpgradeStageStartup is the startup upgrade stage, executed at the beginning of node startup.
	UpgradeStageStartup UpgradeStage = 1

	// UpgradeStageConsensus is the upgrade stage carried out during consensus events.
	UpgradeStageConsensus UpgradeStage = 2

	upgradeStageLast = UpgradeStageConsensus

	// InvalidUpgradeHeight means the upgrade epoch hasn't been reached yet.
	InvalidUpgradeHeight = int64(0)

	// LatestDescriptorVersion is the latest upgrade descriptor version that should be used for
	// descriptors.
	LatestDescriptorVersion = 1

	// MinDescriptorVersion is the minimum descriptor version that is allowed.
	MinDescriptorVersion = 1
	// MaxDescriptorVersion is the maximum descriptor version that is allowed.
	MaxDescriptorVersion = LatestDescriptorVersion

	// LatestPendingUpgradeVersion is the latest pending upgrade struct version.
	LatestPendingUpgradeVersion = 1

	// MinUpgradeHandlerLength is the minimum length of upgrade handler's name.
	MinUpgradeHandlerLength = 3
	// MaxUpgradeHandlerLength is the maximum length of upgrade handler's name.
	MaxUpgradeHandlerLength = 32

	// MinUpgradeEpoch is the minimum upgrade epoch.
	MinUpgradeEpoch = beacon.EpochTime(1)
	// MaxUpgradeEpoch is the maximum upgrade epoch.
	MaxUpgradeEpoch = beacon.EpochInvalid - 1
)

var (
	// ErrStopForUpgrade is the error returned by the consensus upgrade function when it detects that
	// the consensus layer has reached the scheduled shutdown epoch and should be interrupted.
	ErrStopForUpgrade = errors.New(ModuleName, 1, "upgrade: reached upgrade epoch")

	// ErrAlreadyPending is the error returned from SubmitDescriptor when the specific upgrade is already pending.
	ErrAlreadyPending = errors.New(ModuleName, 5, "upgrade: submitted upgrade is already pending, can not resubmit descriptor")

	// ErrUpgradeInProgress is the error returned from CancelUpgrade when the upgrade being cancelled is already in progress.
	ErrUpgradeInProgress = errors.New(ModuleName, 6, "upgrade: can not cancel upgrade in progress")

	// ErrUpgradeNotFound is the error returned when the upgrade in question cannot be found.
	ErrUpgradeNotFound = errors.New(ModuleName, 7, "upgrade: not found")

	_ prettyprint.PrettyPrinter = (*Descriptor)(nil)
)

// Descriptor describes an upgrade.
type Descriptor struct { // nolint: maligned
	cbor.Versioned

	// Handler is the name of the upgrade handler.
	Handler string `json:"handler"`
	// Target is upgrade's target version.
	Target version.ProtocolVersions `json:"target"`
	// Epoch is the epoch at which the upgrade should happen.
	Epoch beacon.EpochTime `json:"epoch"`
}

// Equals compares descriptors for equality.
func (d *Descriptor) Equals(other *Descriptor) bool {
	if d.V != other.V {
		return false
	}
	if d.Handler != other.Handler {
		return false
	}
	if d.Target != other.Target {
		return false
	}
	if d.Epoch != other.Epoch {
		return false
	}
	return true
}

// ValidateBasic does basic validation checks of the upgrade descriptor.
func (d Descriptor) ValidateBasic() error {
	if d.V < MinDescriptorVersion || d.V > MaxDescriptorVersion {
		return fmt.Errorf("invalid upgrade descriptor version: %d (min: %d max: %d)",
			d.V,
			MinDescriptorVersion,
			MaxDescriptorVersion,
		)
	}
	if len(d.Handler) < MinUpgradeHandlerLength || len(d.Handler) > MaxUpgradeHandlerLength {
		return fmt.Errorf("invalid upgrade descriptor handler length: %d (min: %d max: %d)",
			len(d.Handler),
			MinUpgradeHandlerLength,
			MaxUpgradeHandlerLength,
		)
	}
	if err := d.Target.ValidateBasic(); err != nil {
		return fmt.Errorf("invalid upgrade descriptor target version: %w", err)
	}
	minUpgradeEpoch, maxUpgradeEpoch := MinUpgradeEpoch, MaxUpgradeEpoch // Work-around for incorrect go-fuzz instrumentation.
	if d.Epoch < minUpgradeEpoch || d.Epoch > maxUpgradeEpoch {
		return fmt.Errorf("invalid upgrade descriptor epoch: %d (min: %d max: %d)",
			d.Epoch,
			MinUpgradeEpoch,
			MaxUpgradeEpoch,
		)
	}

	return nil
}

// EnsureCompatible checks if currently running binary is compatible with
// the upgrade descriptor.
func (d *Descriptor) EnsureCompatible() error {
	ownVersion := version.Versions

	if !ownVersion.Compatible(d.Target) {
		return fmt.Errorf("binary version not compatible: own: %s, required: %s", ownVersion, d.Target)
	}
	return nil
}

// PrettyPrint writes a pretty-printed representation of Descriptor to the given
// writer.
func (d Descriptor) PrettyPrint(ctx context.Context, prefix string, w io.Writer) {
	fmt.Fprintf(w, "%sHandler: %s\n", prefix, d.Handler)
	fmt.Fprintf(w, "%sTarget Version:\n", prefix)
	d.Target.PrettyPrint(ctx, prefix+"  ", w)
	fmt.Fprintf(w, "%sEpoch: %d\n", prefix, d.Epoch)
}

// PrettyType returns a representation of Descriptor that can be used for pretty
// printing.
func (d Descriptor) PrettyType() (interface{}, error) {
	return d, nil
}

// PendingUpgrade describes a currently pending upgrade and includes the
// submitted upgrade descriptor.
type PendingUpgrade struct {
	cbor.Versioned

	// Descriptor is the upgrade descriptor describing the upgrade.
	Descriptor *Descriptor `json:"descriptor"`

	// UpgradeHeight is the height at which the upgrade epoch was reached
	// (or InvalidUpgradeHeight if it hasn't been reached yet).
	UpgradeHeight int64 `json:"upgrade_height"`

	// LastCompletedStage is the last upgrade stage that was successfully completed.
	LastCompletedStage UpgradeStage `json:"last_completed_stage"`
}

// IsCompleted checks if all upgrade stages were already completed.
func (pu PendingUpgrade) IsCompleted() bool {
	return pu.LastCompletedStage >= upgradeStageLast
}

// HasAnyStages checks if any stages were completed at all.
func (pu PendingUpgrade) HasAnyStages() bool {
	return pu.LastCompletedStage > 0
}

// HasStage checks if a given stage has been completed or not.
func (pu PendingUpgrade) HasStage(stage UpgradeStage) bool {
	return pu.LastCompletedStage >= stage
}

// PushStage marks the given stage as completed.
func (pu *PendingUpgrade) PushStage(stage UpgradeStage) {
	if pu.LastCompletedStage+1 != stage {
		panic("upgrade: out of order upgrade stage execution")
	}
	pu.LastCompletedStage = stage
}

// Backend defines the interface for upgrade managers.
type Backend interface {
	// SubmitDescriptor submits the serialized descriptor to the upgrade manager
	// which then schedules and manages the upgrade.
	SubmitDescriptor(context.Context, *Descriptor) error

	// PendingUpgrades returns pending upgrades.
	PendingUpgrades(context.Context) ([]*PendingUpgrade, error)

	// HasPendingUpgradeAt returns whether there is a pending upgrade at a specified height.
	HasPendingUpgradeAt(context.Context, int64) (bool, error)

	// CancelUpgrade cancels a specific pending upgrade, unless it is already in progress.
	CancelUpgrade(context.Context, *Descriptor) error

	// GetUpgrade returns the pending upgrade (if any) that has the given descriptor.
	//
	// In case no such upgrade exists, this returns ErrUpgradeNotFound.
	GetUpgrade(context.Context, *Descriptor) (*PendingUpgrade, error)

	// StartupUpgrade performs the startup portion of the upgrade.
	// It is idempotent with respect to the current upgrade descriptor.
	StartupUpgrade() error

	// ConsensusUpgrade performs the consensus portion of the upgrade. Note that this will be called
	// multiple times (in BeginBlock and EndBlock) where the context in the first argument can be
	// used to determine which part it is.
	//
	// It is idempotent with respect to the current upgrade descriptor.
	ConsensusUpgrade(interface{}, beacon.EpochTime, int64) error

	// Close cleans up any upgrader state and database handles.
	Close()
}
