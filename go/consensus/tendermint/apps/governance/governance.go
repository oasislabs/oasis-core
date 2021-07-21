package governance

import (
	"fmt"

	"github.com/tendermint/tendermint/abci/types"

	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/node"
	"github.com/oasisprotocol/oasis-core/go/common/quantity"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	"github.com/oasisprotocol/oasis-core/go/consensus/tendermint/api"
	governanceState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/governance/state"
	registryapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry"
	registryState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/registry/state"
	schedulerapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/scheduler"
	schedulerState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/scheduler/state"
	stakingapp "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/staking"
	stakingState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/apps/staking/state"
	governance "github.com/oasisprotocol/oasis-core/go/governance/api"
	stakingAPI "github.com/oasisprotocol/oasis-core/go/staking/api"
	upgrade "github.com/oasisprotocol/oasis-core/go/upgrade/api"
)

var _ api.Application = (*governanceApplication)(nil)

type governanceApplication struct {
	state api.ApplicationState
}

func (app *governanceApplication) Name() string {
	return AppName
}

func (app *governanceApplication) ID() uint8 {
	return AppID
}

func (app *governanceApplication) Methods() []transaction.MethodName {
	return governance.Methods
}

func (app *governanceApplication) Blessed() bool {
	return false
}

func (app *governanceApplication) Dependencies() []string {
	return []string{registryapp.AppName, schedulerapp.AppName, stakingapp.AppName}
}

func (app *governanceApplication) OnRegister(state api.ApplicationState, md api.MessageDispatcher) {
	app.state = state

	// Subscribe to messages emitted by other apps.
	md.Subscribe(api.MessageStateSyncCompleted, app)
}

func (app *governanceApplication) OnCleanup() {
}

func (app *governanceApplication) ExecuteTx(ctx *api.Context, tx *transaction.Transaction) error {
	state := governanceState.NewMutableState(ctx.State())

	ctx.Logger().Debug("executing governance tx",
		"tx", tx,
	)

	switch tx.Method {
	case governance.MethodSubmitProposal:
		var proposalContent governance.ProposalContent
		if err := cbor.Unmarshal(tx.Body, &proposalContent); err != nil {
			return err
		}
		return app.submitProposal(ctx, state, &proposalContent)
	case governance.MethodCastVote:
		var proposalVote governance.ProposalVote
		if err := cbor.Unmarshal(tx.Body, &proposalVote); err != nil {
			return err
		}
		return app.castVote(ctx, state, &proposalVote)
	default:
		return governance.ErrInvalidArgument
	}
}

func (app *governanceApplication) ExecuteMessage(ctx *api.Context, kind, msg interface{}) error {
	state := governanceState.NewMutableState(ctx.State())

	switch kind {
	case api.MessageStateSyncCompleted:
		// State sync has just completed, check whether there are any pending upgrades to make
		// sure we don't miss them after the sync.
		pendingUpgrades, err := state.PendingUpgrades(ctx)
		if err != nil {
			return fmt.Errorf("tendermint/governance: couldn't get pending upgrades: %w", err)
		}

		// Apply all pending upgrades locally.
		if upgrader := ctx.AppState().Upgrader(); upgrader != nil {
			for _, pu := range pendingUpgrades {
				switch err = upgrader.SubmitDescriptor(ctx, pu); err {
				case nil, upgrade.ErrAlreadyPending:
				default:
					ctx.Logger().Error("failed to locally apply the upgrade descriptor",
						"err", err,
						"descriptor", pu,
					)
				}
			}
		}
		return nil
	default:
		return governance.ErrInvalidArgument
	}
}

func (app *governanceApplication) BeginBlock(ctx *api.Context, request types.RequestBeginBlock) error {
	// Check if epoch has changed.
	epochChanged, epoch := app.state.EpochChanged(ctx)
	if !epochChanged {
		// Nothing to do.
		return nil
	}

	// Check if a pending upgrade is scheduled for current epoch.
	state := governanceState.NewMutableState(ctx.State())
	pendingUpgrades, err := state.PendingUpgrades(ctx)
	if err != nil {
		return fmt.Errorf("tendermint/governance: couldn't get pending upgrades: %w", err)
	}
	var ud *upgrade.Descriptor
	for _, pendingUpgrade := range pendingUpgrades {
		if pendingUpgrade.Epoch == epoch {
			ud = pendingUpgrade
			break
		}
	}
	if ud == nil {
		ctx.Logger().Debug("no pending upgrades scheduled for current epoch",
			"epoch", epoch,
			"pending_upgrades", pendingUpgrades,
		)
		// No upgrade scheduled for current epoch.
		return nil
	}

	ctx.Logger().Info("pending upgrade scheduled for this epoch",
		"epoch", epoch,
		"upgrade", ud,
	)

	// Check if the upgrade descriptor is installed in the node and has been executed.
	if upgrader := ctx.AppState().Upgrader(); upgrader != nil {
		switch pu, err := upgrader.GetUpgrade(ctx, ud); err {
		case nil:
			// Upgrade exists, make sure it is in the process of being applied.
			if !pu.HasStage(upgrade.UpgradeStageStartup) {
				ctx.Logger().Error("upgrade pending but not being applied",
					"handler", ud.Handler,
					"epoch", ud.Epoch,
					"upgrade_height", pu.UpgradeHeight,
					"last_completed_stage", pu.LastCompletedStage,
				)
				return upgrade.ErrStopForUpgrade
			}
		case upgrade.ErrUpgradeNotFound:
			// The upgrade does not exist.
			ctx.Logger().Error("upgrade handler does not exist",
				"handler", ud.Handler,
				"epoch", ud.Epoch,
				"upgrade_height", pu.UpgradeHeight,
				"last_completed_stage", pu.LastCompletedStage,
			)
			return upgrade.ErrStopForUpgrade
		default:
			// Unknown error.
			ctx.Logger().Error("failed to verify pending upgrade status",
				"err", err,
			)
			return upgrade.ErrStopForUpgrade
		}
	}

	ctx.Logger().Info("running version compatible, removing pending upgrade",
		"epoch", epoch,
	)
	// If we are running the correct version, remove the pending upgrade - the upgrader should have
	// handeled the upgrade already.
	if err := state.RemovePendingUpgradesForEpoch(ctx, epoch); err != nil {
		return fmt.Errorf("tendermint/governance: couldn't remove pending upgrades for epoch: %w", err)
	}

	return nil
}

// executeProposal executed the proposal.
//
// The method modifies the passed proposal.
func (app *governanceApplication) executeProposal(ctx *api.Context, state *governanceState.MutableState, proposal *governance.Proposal) error {
	// If proposal execution fails, the proposal's state is changed to StateFailed.
	proposal.State = governance.StateFailed

	switch {
	case proposal.Content.Upgrade != nil:
		params, err := state.ConsensusParameters(ctx)
		if err != nil {
			return fmt.Errorf("failed to query consensus parameters: %w", err)
		}

		// Upgrade is only allowed at the upgrade epoch if there is no pending
		// upgrade UpgradeMinEpochDiff before or after.
		upgrades, err := state.PendingUpgrades(ctx)
		if err != nil {
			return fmt.Errorf("failed to query upgrades: %w", err)
		}
		for _, pu := range upgrades {
			if pu.Epoch.AbsDiff(proposal.Content.Upgrade.Descriptor.Epoch) < params.UpgradeMinEpochDiff {
				return fmt.Errorf("upgrade already scheduled at epoch: %v: %w", pu.Epoch, governance.ErrUpgradeAlreadyPending)
			}
		}

		// Execute upgrade proposal.
		err = state.SetPendingUpgrade(ctx, proposal.ID, &proposal.Content.Upgrade.Descriptor)
		if err != nil {
			return fmt.Errorf("failed to set pending upgrade: %w", err)
		}

		// Locally apply the upgrade proposal.
		if upgrader := ctx.AppState().Upgrader(); upgrader != nil {
			if err = upgrader.SubmitDescriptor(ctx, &proposal.Content.Upgrade.Descriptor); err != nil {
				ctx.Logger().Error("failed to locally apply the upgrade descriptor",
					"err", err,
					"descriptor", proposal.Content.Upgrade.Descriptor,
				)
			}
		}
	case proposal.Content.CancelUpgrade != nil:
		cancelingProposal, err := state.Proposal(ctx, proposal.Content.CancelUpgrade.ProposalID)
		if err != nil {
			return fmt.Errorf("failed to query proposal: %w", err)
		}
		if cancelingProposal.Content.Upgrade == nil {
			return fmt.Errorf("%w: canceling proposal needs to be an upgrade proposal", governance.ErrNoSuchUpgrade)
		}
		upgradeProposal, err := state.PendingUpgradeProposal(ctx, cancelingProposal.ID)
		if err != nil {
			return fmt.Errorf("failed to get pending upgrade: %w", err)
		}
		err = state.RemovePendingUpgrade(ctx, cancelingProposal.Content.Upgrade.Epoch, cancelingProposal.ID)
		if err != nil {
			return fmt.Errorf("failed to remove pending upgrade: %w", err)
		}

		// Locally cancel the upgrade proposal.
		if upgrader := ctx.AppState().Upgrader(); upgrader != nil {
			if err = upgrader.CancelUpgrade(ctx, &upgradeProposal.Descriptor); err != nil {
				ctx.Logger().Error("failed to locally cancel the upgrade",
					"err", err,
					"descriptor", upgradeProposal.Descriptor,
				)
			}
		}
	default:
		return governance.ErrInvalidArgument
	}

	proposal.State = governance.StatePassed
	// If successful, emit Proposal executed event.
	ctx.EmitEvent(api.NewEventBuilder(app.Name()).TypedAttribute(&governance.ProposalExecutedEvent{
		ID: proposal.ID,
	}))

	return nil
}

func (app *governanceApplication) validatorsEscrow(
	ctx *api.Context,
	stakingState *stakingState.MutableState,
	registryState *registryState.MutableState,
	schedulerState *schedulerState.MutableState,
) (*quantity.Quantity, map[stakingAPI.Address]*quantity.Quantity, error) {
	currentValidators, err := schedulerState.CurrentValidators(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query current validators: %w", err)
	}

	totalVotingStake := quantity.NewQuantity()
	validatorEntitiesEscrow := make(map[stakingAPI.Address]*quantity.Quantity)

	for valID := range currentValidators {
		var node *node.Node
		node, err = registryState.NodeBySubKey(ctx, valID)
		// XXX: in unit tests where DebugStaticValidators is used, the node
		// might actually not exist in registry - causing a consensus panic here.
		// Make sure all validators are registered in governance unit tests.
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query validator node: %w", err)
		}
		entityAddr := stakingAPI.NewAddress(node.EntityID)

		var escrow *quantity.Quantity
		escrow, err = stakingState.EscrowBalance(ctx, entityAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query validator escrow: %w", err)
		}

		// If there are multiple nodes in the validator set belonging to the same entity,
		// only count the entity escrow once.
		if validatorEntitiesEscrow[entityAddr] != nil {
			continue
		}
		validatorEntitiesEscrow[entityAddr] = escrow
		if err := totalVotingStake.Add(escrow); err != nil {
			return nil, nil, fmt.Errorf("failed to add to totalVotingStake: %w", err)
		}
	}
	return totalVotingStake, validatorEntitiesEscrow, nil
}

// closeProposal closes an active proposal.
//
// This method modifies the passed proposal.
func (app *governanceApplication) closeProposal(
	ctx *api.Context,
	state *governanceState.MutableState,
	totalVotingStake quantity.Quantity,
	validatorEntitiesEscrow map[stakingAPI.Address]*quantity.Quantity,
	proposal *governance.Proposal,
) error {
	params, err := state.ConsensusParameters(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch consensus parameters: %w", err)
	}

	proposal.Results = make(map[governance.Vote]quantity.Quantity)
	votes, err := state.Votes(ctx, proposal.ID)
	if err != nil {
		return fmt.Errorf("failed to query votes: %w", err)
	}

	ctx.Logger().Debug("tallying votes",
		"proposal", proposal,
		"total_voting_stake", totalVotingStake,
		"validator_entities_escrow", validatorEntitiesEscrow,
		"votes", votes,
	)
	// Tally the votes.
	for _, vote := range votes {
		escrow, ok := validatorEntitiesEscrow[vote.Voter]
		if !ok {
			// Voter not in current validator set - invalid vote.
			proposal.InvalidVotes++
			continue
		}

		currentVotes := proposal.Results[vote.Vote]
		newVotes := escrow.Clone()
		if err := newVotes.Add(&currentVotes); err != nil {
			return fmt.Errorf("failed to add votes: %w", err)
		}
		proposal.Results[vote.Vote] = *newVotes
	}

	ctx.Logger().Debug("close proposal",
		"total_voting_state", totalVotingStake,
		"results", proposal.Results,
		"invalid_votes", proposal.InvalidVotes,
		"quorum", params.Quorum,
		"threshold", params.Threshold,
	)
	if err := proposal.CloseProposal(totalVotingStake, params.Quorum, params.Threshold); err != nil {
		return err
	}

	return nil
}

func (app *governanceApplication) EndBlock(ctx *api.Context, request types.RequestEndBlock) (types.ResponseEndBlock, error) {
	// Check if epoch has changed.
	epochChanged, epoch := app.state.EpochChanged(ctx)
	if !epochChanged {
		// Nothing to do.
		return types.ResponseEndBlock{}, nil
	}

	state := governanceState.NewMutableState(ctx.State())
	params, err := state.ConsensusParameters(ctx)
	if err != nil {
		return types.ResponseEndBlock{}, fmt.Errorf("failed to fetch consensus parameters: %w", err)
	}

	activeProposals, err := state.ActiveProposals(ctx)
	if err != nil {
		return types.ResponseEndBlock{}, fmt.Errorf("tendermint/governance: couldn't get active proposals: %w", err)
	}
	// Get proposals that are closed this epoch.
	var closingProposals []*governance.Proposal
	for _, proposal := range activeProposals {
		if proposal.ClosesAt != epoch {
			continue
		}
		closingProposals = append(closingProposals, proposal)
	}

	// No proposals closing this epoch.
	if len(closingProposals) == 0 {
		ctx.Logger().Debug("no proposals scheduled to be closed this epoch")
		return types.ResponseEndBlock{}, nil
	}

	ctx.Logger().Debug("proposals scheduled to be closed this epoch",
		"n_proposals", len(closingProposals),
	)

	// Prepare validator set entities state.
	stakingState := stakingState.NewMutableState(ctx.State())
	totalVotingStake, validatorEntitiesEscrow, err := app.validatorsEscrow(
		ctx,
		stakingState,
		registryState.NewMutableState(ctx.State()),
		schedulerState.NewMutableState(ctx.State()),
	)
	if err != nil {
		return types.ResponseEndBlock{}, fmt.Errorf("consensus/governance: failed to compute validators escrow: %w", err)
	}

	if totalVotingStake.IsZero() {
		return types.ResponseEndBlock{}, fmt.Errorf("consensus/governance: total voting stake is zero")
	}

	for _, proposal := range closingProposals {
		ctx.Logger().Debug("closing proposal",
			"proposal", proposal,
		)
		if err = app.closeProposal(
			ctx,
			state,
			*totalVotingStake,
			validatorEntitiesEscrow,
			proposal,
		); err != nil {
			ctx.Logger().Error("proposal closing failure",
				"err", err,
				"proposal", proposal,
				"params", params,
				"total_voting_stake", totalVotingStake,
				"len_validator_entities_escrow", len(validatorEntitiesEscrow),
			)
			return types.ResponseEndBlock{}, fmt.Errorf("consensus/governance: failed to close a proposal: %w", err)
		}

		ctx.Logger().Debug("proposal closed",
			"proposal", proposal,
			"state", proposal.State,
		)

		// In case the proposal is passed, the proposal content is executed.
		if proposal.State == governance.StatePassed {
			// Execute.
			if err = app.executeProposal(ctx, state, proposal); err != nil {
				ctx.Logger().Error("proposal execution failure",
					"err", err,
					"proposal", proposal,
				)
			}
		}

		// Save the updated proposal.
		if err = state.SetProposal(ctx, proposal); err != nil {
			return types.ResponseEndBlock{}, fmt.Errorf("failed to save proposal: %w", err)
		}
		// Remove proposal from active list.
		if err = state.RemoveActiveProposal(ctx, proposal); err != nil {
			return types.ResponseEndBlock{}, fmt.Errorf("failed to remove active proposal: %w", err)
		}

		// Emit Proposal finalized event.
		ctx.EmitEvent(api.NewEventBuilder(app.Name()).TypedAttribute(&governance.ProposalFinalizedEvent{
			ID:    proposal.ID,
			State: proposal.State,
		}))

		switch proposal.State {
		case governance.StatePassed, governance.StateFailed:
			// Transfer back proposal deposits.
			if err = stakingState.TransferFromGovernanceDeposits(
				ctx,
				proposal.Submitter,
				&proposal.Deposit,
			); err != nil {
				ctx.Logger().Error("failed to transfer from governance deposits",
					"err", err,
					"submitter", proposal.Submitter,
					"deposit", proposal.Deposit,
				)
				return types.ResponseEndBlock{},
					fmt.Errorf("consensus/governance: failed to reclaim proposal deposit: %w", err)
			}
		case governance.StateRejected:
			// Proposal rejected, deposit is transferred into the common pool.
			if err = stakingState.DiscardGovernanceDeposit(
				ctx,
				&proposal.Deposit,
			); err != nil {
				return types.ResponseEndBlock{},
					fmt.Errorf("consensus/governance: failed to discard proposal deposit: %w", err)
			}
		default:
			// Should not ever happen.
			return types.ResponseEndBlock{},
				fmt.Errorf("consensus/governance: invalid closed proposal state: %v", proposal.State)
		}
	}

	return types.ResponseEndBlock{}, nil
}

// New constructs a new governance application instance.
func New() api.Application {
	return &governanceApplication{}
}
