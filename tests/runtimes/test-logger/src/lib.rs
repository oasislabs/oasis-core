extern crate protobuf;

extern crate ekiden_core;
extern crate ekiden_trusted;

extern crate test_logger_api;

#[macro_use]
extern crate lazy_static;

use std::str::FromStr;
#[cfg(not(target_env = "sgx"))]
use std::sync::Mutex;
#[cfg(target_env = "sgx")]
use std::sync::SgxMutex as Mutex;

use test_logger_api::{with_api, LoggerInitResponse};

use ekiden_core::bytes::H256;
use ekiden_core::enclave::quote::MrEnclave;
use ekiden_core::error::Result;
use ekiden_trusted::db::{DBKeyManagerConfig, Database, DatabaseHandle};
use ekiden_trusted::enclave::enclave_init;
use ekiden_trusted::runtime::create_runtime;
use ekiden_trusted::runtime::dispatcher::RuntimeCallContext;

#[macro_use]
use ekiden_enclave_logger::*;

enclave_init!();

// Create enclave contract interface.
with_api! {
    create_runtime!(api);
}

lazy_static! {
    // Key manager's enclave.
    static ref KM_ENCLAVE: Mutex<MrEnclave> = Mutex::new(MrEnclave::zero());
}

pub fn init() -> Result<LoggerInitResponse> {
    Ok("[test test-logger enclave] Log initialized!")
}

#[cfg(target_env = "sgx")]
pub fn write_error(message: &str) {
    error!("[test test-logger enclave] Received error message: {}", message);
}

#[cfg(target_env = "sgx")]
pub fn write_warn(message: &str) {
    warn!("[test test-logger enclave] Received warn message: {}", message);
}

#[cfg(target_env = "sgx")]
pub fn write_info(message: &str) {
    info!("[test test-logger enclave] Received info message: {}", message);
}

#[cfg(target_env = "sgx")]
pub fn write_debug(message: &str) {
    debug!("[test test-logger enclave] Received debug message: {}", message);
}

#[cfg(target_env = "sgx")]
pub fn write_trace(message: &str) {
    trace!("[test test-logger enclave] Received trace message: {}", message);
}
