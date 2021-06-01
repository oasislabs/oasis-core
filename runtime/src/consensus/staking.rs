//! Consensus staking structures.
//!
//! # Note
//!
//! This **MUST** be kept in sync with go/staking/api.
//!
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_repr::*;

use crate::{
    common::{crypto::hash::Hash, quantity::Quantity},
    consensus::{address::Address, beacon::EpochTime},
};

/// A stake transfer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Transfer {
    pub to: Address,
    pub amount: Quantity,
}

/// A withdrawal from an account.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Withdraw {
    pub from: Address,
    pub amount: Quantity,
}

/// A stake escrow.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Escrow {
    pub account: Address,
    pub amount: Quantity,
}

/// A reclaim escrow.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReclaimEscrow {
    pub account: Address,
    pub shares: Quantity,
}

/// Kind of staking threshold.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize_repr, Deserialize_repr)]
#[repr(i32)]
pub enum ThresholdKind {
    /// Entity staking threshold.
    #[serde(rename = "entity")]
    KindEntity = 0,
    /// Validator node staking threshold.
    #[serde(rename = "node-validator")]
    KindNodeValidator = 1,
    /// Compute node staking threshold.
    #[serde(rename = "node-compute")]
    KindNodeCompute = 2,
    /// Storage node staking threshold.
    #[serde(rename = "node-storage")]
    KindNodeStorage = 3,
    /// Keymanager node staking threshold.
    #[serde(rename = "node-keymanager")]
    KindNodeKeyManager = 4,
    /// Compute runtime staking threshold.
    #[serde(rename = "runtime-compute")]
    KindRuntimeCompute = 5,
    /// Keymanager runtime staking threshold.
    #[serde(rename = "runtime-keymanager")]
    KindRuntimeKeyManager = 6,
}

/// Entry in the staking ledger.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Account {
    #[serde(default)]
    pub general: GeneralAccount,
    #[serde(default)]
    pub escrow: EscrowAccount,
}

/// General purpose account.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GeneralAccount {
    #[serde(default)]
    pub balance: Quantity,
    #[serde(default)]
    pub nonce: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub allowances: Option<BTreeMap<Address, Quantity>>,
}

/// Escrow account.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EscrowAccount {
    #[serde(default)]
    pub active: SharePool,

    #[serde(default)]
    pub debonding: SharePool,

    #[serde(default)]
    pub commission_schedule: CommissionSchedule,

    #[serde(default)]
    pub stake_accumulator: StakeAccumulator,
}

/// Combined balance of serval entries, the relative sizes of which are tracked through shares.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SharePool {
    #[serde(default)]
    pub balance: Quantity,

    #[serde(default)]
    pub total_shares: Quantity,
}

/// Defines a list of commission rates and commission rate bounds with their starting times.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionSchedule {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub rates: Option<Vec<CommissionRateStep>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub bounds: Option<Vec<CommissionRateBoundStep>>,
}

/// Commission rate and its starting time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionRateStep {
    #[serde(default)]
    pub start: EpochTime,

    #[serde(default)]
    pub rate: Quantity,
}

/// Commission rate bound and its starting time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommissionRateBoundStep {
    #[serde(default)]
    pub start: EpochTime,

    #[serde(default)]
    pub rate_min: Quantity,

    #[serde(default)]
    pub rate_max: Quantity,
}

/// Per escrow account stake accumulator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StakeAccumulator {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub claims: Option<BTreeMap<StakeClaim, Vec<StakeThreshold>>>,
}

/// Unique stake claim identifier.
pub type StakeClaim = String;

/// Stake threshold used in the stake accumulator.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StakeThreshold {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub global: Option<ThresholdKind>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(rename = "const")]
    pub constant: Option<Quantity>,
}

/// Delegation descriptor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Delegation {
    pub shares: Quantity,
}

/// Debonding delegation descriptor.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DebondingDelegation {
    pub shares: Quantity,

    #[serde(rename = "debond_end")]
    pub debond_end_time: EpochTime,
}

/// Staking event.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Event {
    pub height: i64,
    pub tx_hash: Hash,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub transfer: Option<TransferEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub burn: Option<BurnEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub escrow: Option<EscrowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub allowance_change: Option<AllowanceChangeEvent>,
}

/// Event emitted when stake is transferred.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransferEvent {
    pub from: Address,
    pub to: Address,
    pub amount: Quantity,
}

/// Event emitted when stake is burned.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BurnEvent {
    pub owner: Address,
    pub amount: Quantity,
}

/// Event emitted on staking operations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EscrowEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub add: Option<AddEscrowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub take: Option<TakeEscrowEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub reclaim: Option<ReclaimEscrowEvent>,
}

/// Event emitted when allowance is changed for a beneficiary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AllowanceChangeEvent {
    pub owner: Address,
    pub beneficiary: Address,
    pub allowance: Quantity,
    #[serde(default)]
    pub negative: bool,
    pub amount_change: Quantity,
}

/// Event emitted when stake is transferred into an escrow account.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AddEscrowEvent {
    pub owner: Address,
    pub escrow: Address,
    pub amount: Quantity,
}

/// Event emitted when stake is taken from an escrow account (i.e. stake is slashed).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TakeEscrowEvent {
    pub owner: Address,
    pub amount: Quantity,
}

/// Event emitted when stake is reclaimed from an escrow account back into owner's
/// general account.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReclaimEscrowEvent {
    pub owner: Address,
    pub escrow: Address,
    pub amount: Quantity,
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::cbor,
        consensus::address::{COMMON_POOL_ADDRESS, GOVERNANCE_DEPOSITS_ADDRESS},
    };

    #[test]
    fn test_consistent_accounts() {
        let tcs = vec![
        ("oA==", Account::default()),
        (
            "oWdnZW5lcmFsomVub25jZRghZ2JhbGFuY2VBCg==",
            Account {
                general: GeneralAccount {
                    balance: Quantity::from(10),
                    nonce: 33,
                    ..Default::default()
                },
                ..Default::default()
            },
        ),
        (
            "oWdnZW5lcmFsoWphbGxvd2FuY2VzolUAdU/0RxQ6XsX0cbMPhna5TVaxV1BBIVUA98Te1iET4sKC6oZyI6VE7VXWum5BZA==",
            {
                Account {
                    general: GeneralAccount {
                        allowances: Some([
                            (COMMON_POOL_ADDRESS.clone(), Quantity::from(100)),
                            (GOVERNANCE_DEPOSITS_ADDRESS.clone(), Quantity::from(33))
                        ].iter().cloned().collect()),
                        ..Default::default()
                        },
                    ..Default::default()
                }
                },
        ),
        (
            "oWZlc2Nyb3ejZmFjdGl2ZaJnYmFsYW5jZUIETGx0b3RhbF9zaGFyZXNBC3FzdGFrZV9hY2N1bXVsYXRvcqFmY2xhaW1zoWZlbnRpdHmCoWVjb25zdEFNoWZnbG9iYWwCc2NvbW1pc3Npb25fc2NoZWR1bGWhZmJvdW5kc4GjZXN0YXJ0GCFocmF0ZV9tYXhCA+hocmF0ZV9taW5BCg==",
            Account {
                escrow: EscrowAccount {
                    active: SharePool{
                        balance: Quantity::from(1100),
                        total_shares: Quantity::from(11),
                    },
                    debonding: SharePool::default(),
                    commission_schedule: CommissionSchedule {
                        bounds: Some(vec![CommissionRateBoundStep{
                            start: 33,
                            rate_min: Quantity::from(10),
                            rate_max: Quantity::from(1000),
                        }]),
                        ..Default::default()
                    },
                    stake_accumulator: StakeAccumulator {
                        claims: Some([
                            (
                                "entity".to_string(),
                                vec![
                                    StakeThreshold{
                                        constant: Some(Quantity::from(77)),
                                        ..Default::default()
                                    },
                                    StakeThreshold{
                                        global: Some(ThresholdKind::KindNodeCompute),
                                        ..Default::default()
                                    },
                                ]
                            )
                        ].iter().cloned().collect())
                    }
                },
                ..Default::default()
            },
        )
    ];
        for (encoded_base64, rr) in tcs {
            let dec: Account = cbor::from_slice(&base64::decode(encoded_base64).unwrap())
                .expect("account should deserialize correctly");
            assert_eq!(dec, rr, "decoded account should match the expected value");
        }
    }

    #[test]
    fn test_consistent_delegations() {
        let tcs = vec![
            ("oWZzaGFyZXNA", Delegation::default()),
            (
                "oWZzaGFyZXNBZA==",
                Delegation {
                    shares: Quantity::from(100),
                },
            ),
        ];
        for (encoded_base64, rr) in tcs {
            let dec: Delegation = cbor::from_slice(&base64::decode(encoded_base64).unwrap())
                .expect("delegation should deserialize correctly");
            assert_eq!(dec, rr, "decoded account should match the expected value");
        }
    }

    #[test]
    fn test_consistent_debonding_delegations() {
        let tcs = vec![
            (
                "omZzaGFyZXNAamRlYm9uZF9lbmQA",
                DebondingDelegation::default(),
            ),
            (
                "omZzaGFyZXNBZGpkZWJvbmRfZW5kFw==",
                DebondingDelegation {
                    shares: Quantity::from(100),
                    debond_end_time: 23,
                },
            ),
        ];
        for (encoded_base64, rr) in tcs {
            let dec: DebondingDelegation =
                cbor::from_slice(&base64::decode(encoded_base64).unwrap())
                    .expect("debonding delegation should deserialize correctly");
            assert_eq!(dec, rr, "decoded account should match the expected value");
        }
    }
}
