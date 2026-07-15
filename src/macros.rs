#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        if let Some(logger) = $crate::get_logger() {
            logger.log($crate::Level::Error, &$crate::format_args!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        if let Some(logger) = $crate::get_logger() {
            logger.log($crate::Level::Info, &$crate::format_args!($($arg)*));
        }
    };
}