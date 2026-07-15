use buffers;

pub trait Logger: Send + Sync {
    fn log(&self, level: Level, msg: &str);
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }
}

static RING_LOGGER: buffers::SpscRingBuf<u8, 1024> = buffers::SpscRingBuf::new();

pub struct RingLogger;

impl RingLogger {
    pub fn write_bytes(&self, bytes: &[u8]) {
        for &b in bytes {
            let _ = RING_LOGGER.push(b);
        }
    }
}

impl Logger for RingLogger {
    fn log(&self, level: Level, msg: &str) {
        RingLogger.write_bytes(b"[");
        RingLogger.write_bytes(level.as_str().as_bytes());
        RingLogger.write_bytes(b"] ");
        RingLogger.write_bytes(msg.as_bytes());
        RingLogger.write_bytes(b"\n");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read() {
        let logger = RingLogger;
        
        logger.write_bytes(b"[INFO] Hello, World!\n");
        logger.write_bytes(b"[DEBUG] x = 42\n");
        logger.write_bytes(b"[ERROR] Something went wrong\n");
        
        let mut output = std::string::String::new();
        while let Some(b) = RING_LOGGER.pop() {
            output.push(b as char);
        }

        println!("{}", &output);
        
        assert_eq!(output, "[INFO] Hello, World!\n[DEBUG] x = 42\n[ERROR] Something went wrong\n");
    }
}