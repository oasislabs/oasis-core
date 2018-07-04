//! Optimistic consensus backend.

#[macro_use]
extern crate log;

extern crate ekiden_common;
extern crate ekiden_consensus_base;
extern crate ekiden_scheduler_base;
extern crate ekiden_storage_base;

mod backend;

pub use backend::OptimisticConsensusBackend;
