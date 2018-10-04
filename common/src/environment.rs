//! Ekiden environment.
use std::env;
use std::sync::{Arc, Mutex};
use std::thread;

use clap::value_t_or_exit;
use grpcio;
use tokio;

use super::futures::Future;

/// Ekiden application environment.
///
/// Currently provides things like the used event loop.
pub trait Environment: Sync + Send {
    /// Get the gRPC environment.
    fn grpc(&self) -> Arc<grpcio::Environment>;

    /// Spawn a task onto the environment's executor.
    fn spawn(&self, f: Box<Future<Item = (), Error = ()> + Send>);

    /// Start the environment.
    ///
    /// This method will block until the environment shuts down.
    fn start(&self);
}

/// gRPC-based application environment.
pub struct GrpcEnvironment {
    /// gRPC environment.
    grpc_environment: Arc<grpcio::Environment>,
    /// Tokio runtime.
    pub tokio_runtime: Mutex<tokio::runtime::Runtime>,
}

impl GrpcEnvironment {
    pub fn new(grpc_environment: grpcio::Environment) -> Self {
        // Enable support for ECDSA-based ciphers in gRPC.
        env::set_var("GRPC_SSL_CIPHER_SUITES", "ECDHE-ECDSA-AES256-GCM-SHA384");

        let grpc_environment = Arc::new(grpc_environment);

        Self {
            grpc_environment: grpc_environment.clone(),
            tokio_runtime: Mutex::new(tokio::runtime::Runtime::new().unwrap()),
        }
    }
}

impl Environment for GrpcEnvironment {
    fn grpc(&self) -> Arc<grpcio::Environment> {
        self.grpc_environment.clone()
    }

    fn spawn(&self, f: Box<Future<Item = (), Error = ()> + Send>) {
        let mut runtime = self.tokio_runtime.lock().unwrap();
        runtime.spawn(f);
    }

    fn start(&self) {
        // TODO: Handle shutdown.

        loop {
            thread::park();
        }
    }
}

// Register for dependency injection.
create_component!(
    grpc,
    "environment",
    GrpcEnvironment,
    Environment,
    (|container: &mut Container| -> Result<Box<Any>> {
        let args = container.get_arguments().unwrap();
        let grpc_environment =
            grpcio::Environment::new(value_t_or_exit!(args, "grpc-threads", usize));

        let instance: Arc<Environment> = Arc::new(GrpcEnvironment::new(grpc_environment));
        Ok(Box::new(instance))
    }),
    [Arg::with_name("grpc-threads")
        .long("grpc-threads")
        .help("Number of threads to use for the event loop")
        .default_value("4")
        .takes_value(true)]
);
