use log::LevelFilter;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Debug,
    Trace,
    Info,
}

impl LogLevel {
    pub fn is_debug_or_lower(self) -> bool {
        matches!(self, LogLevel::Debug | LogLevel::Trace)
    }
}

impl From<LogLevel> for LevelFilter {
    fn from(val: LogLevel) -> LevelFilter {
        match val {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
            _ => LevelFilter::Info,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Distributed,
    Local,
}

impl DeploymentMode {
    pub fn is_local(self) -> bool {
        matches!(self, DeploymentMode::Local)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogConfig {
    pub log_level: LogLevel,
    pub log_cleanup: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            log_level: LogLevel::Info,
            log_cleanup: !cfg!(debug_assertions),
        }
    }
}
