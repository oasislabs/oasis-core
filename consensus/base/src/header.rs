//! Block header type.
use std::convert::TryFrom;

use ekiden_consensus_api as api;

use ekiden_common::bytes::{B256, H256};
use ekiden_common::error::Error;
use ekiden_common::hash::EncodedHash;
use ekiden_common::uint::U256;

/// Block header.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Header {
    /// Protocol version number.
    pub version: u16,
    /// Chain namespace.
    pub namespace: B256,
    /// Round.
    pub round: U256,
    /// Hash of the previous block.
    pub previous_hash: H256,
    /// Computation group hash.
    pub group_hash: H256,
    /// Input hash.
    pub input_hash: H256,
    /// Output hash.
    pub output_hash: H256,
    /// State root hash.
    pub state_root: H256,
    /// Reveals hash.
    pub reveals_hash: H256,
}

impl Header {
    /// Check if this header is a parent of a child header.
    pub fn is_parent_of(&self, child: &Header) -> bool {
        self.previous_hash == child.get_encoded_hash()
    }
}

impl TryFrom<api::Header> for Header {
    type Error = Error;
    fn try_from(a: api::Header) -> Result<Self, self::Error> {
        Ok(Header {
            version: a.get_version() as u16,
            namespace: B256::try_from(a.get_namespace())?,
            round: U256::from_little_endian(a.get_round()),
            previous_hash: H256::try_from(a.get_previous_hash())?,
            group_hash: H256::try_from(a.get_group_hash())?,
            input_hash: H256::try_from(a.get_input_hash())?,
            output_hash: H256::try_from(a.get_output_hash())?,
            state_root: H256::try_from(a.get_state_root())?,
            reveals_hash: H256::try_from(a.get_reveals_hash())?,
        })
    }
}

impl Into<api::Header> for Header {
    fn into(self) -> api::Header {
        let mut h = api::Header::new();
        h.set_version(self.version as u32);
        h.set_namespace(self.namespace.to_vec());
        h.set_round(self.round.to_vec());
        h.set_previous_hash(self.previous_hash.to_vec());
        h.set_group_hash(self.group_hash.to_vec());
        h.set_input_hash(self.input_hash.to_vec());
        h.set_output_hash(self.output_hash.to_vec());
        h.set_state_root(self.state_root.to_vec());
        h.set_reveals_hash(self.reveals_hash.to_vec());
        h
    }
}
