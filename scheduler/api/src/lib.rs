extern crate ekiden_common_api;
extern crate futures;
extern crate grpcio;
extern crate protobuf;

mod generated;

pub use generated::{scheduler::*, scheduler_grpc::*};
