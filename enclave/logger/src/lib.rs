extern crate log;

#[macro_use]
pub mod macros;
pub use macros::*;

/// Logger based on the rust's log crate
///
/// To use it, first call init_logger() which hooks it into the rust's
/// log crate and then call log::error!, log::warn!, log::info!,
/// log::debug!, or log::trace! macros.
///
/// # Examples
/// ```
/// extern crate log;
///
/// use ekiden_enclave_logger;
/// use log::{warn, error};
///
/// fn write_sth_to_log() {
///   match ekiden_enclave_logger::init() {
///     Ok(_) => (),
///     Err(e) => println!("init_logger: Error initializing Ekiden logger! {}", e),
///  };
///
///  let a = 404;
///
///  warn!("This is a warning {}!", a);
///  error!("And this is an error {}!", a);
/// }
/// ```
struct EkidenLogger;

static EKIDEN_LOGGER: EkidenLogger = EkidenLogger;

impl log::Log for EkidenLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        println!(" {} {} > {}", record.level(), record.target(), record.args());
    }

    fn flush(&self) {}
}

pub fn init() -> Result<(), log::SetLoggerError> {
    log::set_logger(&EKIDEN_LOGGER)
        .map(|()| log::set_max_level(log::LevelFilter::Info))
}