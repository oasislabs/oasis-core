#![macro_escape]
pub const STATIC_MAX_LEVEL: log::LevelFilter = log::LevelFilter::Debug;

extern crate log; // required for log levels

#[macro_export]
macro_rules! log {
    (target: $target:expr, $lvl:expr, $($arg:tt)+) => ({
        let lvl = $lvl;
        if lvl <= $crate::macros::STATIC_MAX_LEVEL {
            $crate.EKIDEN_LOGGER.log!($($arg)+)
        }
    });
    ($lvl:expr, $($arg:tt)+) => (log!(target: module_path!(), $lvl, $($arg)+))
}

#[macro_export]
macro_rules! error {
    (target: $target:expr, $($arg:tt)*) => (
        log!(target: $target, $crate::log::Level::Error, $($arg)*);
    );
    ($($arg:tt)*) => (
        log!(log::Level::Error, $($arg)*);
    )
}

#[macro_export]
macro_rules! warn {
    (target: $target:expr, $($arg:tt)*) => (
        log!(target: $target, $crate::log::Level::Warn, $($arg)*);
    );
    ($($arg:tt)*) => (
        log!(log::Level::Warn, $($arg)*);
    )
}

#[macro_export]
macro_rules! info {
    (target: $target:expr, $($arg:tt)*) => (
        log!(target: $target, $crate::log::Level::Info, $($arg)*);
    );
    ($($arg:tt)*) => (
        log!(log::Level::Info, $($arg)*);
    )
}

#[macro_export]
macro_rules! debug {
    (target: $target:expr, $($arg:tt)*) => (
        log!(target: $target, $crate::log::Level::Debug, $($arg)*);
    );
    ($($arg:tt)*) => (
        log!(log::Level::Debug, $($arg)*);
    )
}

#[macro_export]
macro_rules! trace {
    (target: $target:expr, $($arg:tt)*) => (
        log!(target: $target, $crate::log::Level::Trace, $($arg)*);
    );
    ($($arg:tt)*) => (
        log!(log::Level::Trace, $($arg)*);
    )
}
