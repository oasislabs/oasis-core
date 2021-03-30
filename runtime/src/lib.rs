//! Oasis Core runtime SDK.
//!
//! # Examples
//!
//! To create a minimal runtime that doesn't expose any APIs to the
//! outside world, you need to call the `start_runtime` function:
//! ```rust,ignore
//! oasis_core_runtime::start_runtime(Some(Box::new(reg)));
//! ```
//!
//! This will start the required services needed to communicate with
//! the worker host.

#[macro_use]
extern crate slog;
extern crate anyhow;
extern crate base64;
extern crate bincode;
extern crate chrono;
extern crate crossbeam;
extern crate lazy_static;
extern crate serde_bytes;
extern crate serde_cbor;
extern crate serde_json;
extern crate serde_repr;
extern crate slog_json;
extern crate slog_scope;
extern crate slog_stdlog;
#[macro_use]
extern crate intrusive_collections;
extern crate io_context;
extern crate pem;
extern crate percent_encoding;
extern crate rand;
extern crate rustc_hex;
extern crate snow;
#[cfg(test)]
extern crate tempfile;
extern crate tokio_current_thread;
extern crate tokio_executor;
extern crate webpki;

use lazy_static::lazy_static;
#[cfg(target_env = "sgx")]
use sgx_isa::{AttributesFlags, Report};

#[macro_use]
pub mod common;
pub mod consensus;
pub mod dispatcher;
pub mod enclave_rpc;
pub mod executor;
pub mod init;
pub mod macros;
pub mod protocol;
pub mod rak;
pub mod storage;
pub mod tracing;
pub mod transaction;
pub mod types;

use crate::common::version::{Version, PROTOCOL_VERSION};

#[cfg(target_env = "sgx")]
use self::common::sgx::avr::{EnclaveIdentity, MrSigner};

lazy_static! {
    pub static ref BUILD_INFO: BuildInfo = {
        // Non-SGX builds are insecure by definition.
        #[cfg(not(target_env = "sgx"))]
        let is_secure = false;

        // SGX build security depends on how it was built.
        #[cfg(target_env = "sgx")]
        let is_secure = {
            // Optimistically start out as "it could be secure", and any single
            // insecure build time option will propagate failure.
            let maybe_secure = true;

            // AVR signature verification MUST be enabled.
            let maybe_secure = maybe_secure && option_env!("OASIS_UNSAFE_SKIP_AVR_VERIFY").is_none();

            // Disallow debug enclaves MUST be enabled.
            let maybe_secure = maybe_secure && option_env!("OASIS_UNSAFE_ALLOW_DEBUG_ENCLAVES").is_none();

            // IAS `GROUP_OUT_OF_DATE` and `CONFIGRUATION_NEEDED` responses
            // MUST count as IAS failure.
            //
            // Rationale: This is how IAS signifies that the host environment
            // is insecure (eg: SMT is enabled when it should not be).
            let maybe_secure = maybe_secure && option_env!("OASIS_STRICT_AVR_VERIFY").is_some();

            // The enclave MUST NOT be a debug one.
            let maybe_secure = maybe_secure && !Report::for_self().attributes.flags.contains(AttributesFlags::DEBUG);

            // The enclave MUST NOT be signed by a test key,
            let enclave_identity = EnclaveIdentity::current().unwrap();
            let fortanix_mrsigner = MrSigner::from("9affcfae47b848ec2caf1c49b4b283531e1cc425f93582b36806e52a43d78d1a");
            let maybe_secure = maybe_secure && (enclave_identity.mr_signer != fortanix_mrsigner);

            maybe_secure
        };

        BuildInfo {
            protocol_version: PROTOCOL_VERSION,
            is_secure,
        }
    };
}

/// Runtime build information.
pub struct BuildInfo {
    /// Supported runtime protocol version.
    pub protocol_version: Version,
    /// True iff the build can provide integrity and confidentiality.
    pub is_secure: bool,
}

// Re-exports.
pub use self::{
    enclave_rpc::{demux::Demux as RpcDemux, dispatcher::Dispatcher as RpcDispatcher},
    init::start_runtime,
    protocol::Protocol,
    transaction::dispatcher::{Dispatcher as TxnDispatcher, MethodDispatcher as TxnMethDispatcher},
};
