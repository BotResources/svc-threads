//! Threads' own configuration, beside the engine's ops contract: the title
//! runner type, the stream idle window and the artifact upload window. Each has
//! a default; `schema` and `migrate` read none of it.

use std::time::Duration;

use service_engine::error::EngineError;

/// The runner type of the title Job (`THREADS_TITLE_RUNNER_TYPE`).
pub const ENV_TITLE_RUNNER_TYPE: &str = "THREADS_TITLE_RUNNER_TYPE";
/// How long a writer may push nothing before its `STREAMING` message is
/// failed, in milliseconds (`THREADS_STREAM_IDLE_MS`).
pub const ENV_STREAM_IDLE_MS: &str = "THREADS_STREAM_IDLE_MS";
/// How long an upload access stays valid, and a `PENDING` artifact waits for
/// its message, in milliseconds (`THREADS_UPLOAD_WINDOW_MS`).
pub const ENV_UPLOAD_WINDOW_MS: &str = "THREADS_UPLOAD_WINDOW_MS";

pub const DEFAULT_TITLE_RUNNER_TYPE: &str = "thread_title";
pub const DEFAULT_STREAM_IDLE_WINDOW: Duration = Duration::from_secs(10 * 60);
pub const DEFAULT_UPLOAD_WINDOW: Duration = Duration::from_secs(15 * 60);

/// The configuration of this process, installed once at boot and read by the
/// gestures that need it. Unset (`schema`, unit tests), the defaults hold.
static INSTALLED: std::sync::OnceLock<ThreadsConfig> = std::sync::OnceLock::new();

pub fn install(config: ThreadsConfig) {
    let _ = INSTALLED.set(config);
}

pub fn installed() -> &'static ThreadsConfig {
    INSTALLED.get_or_init(ThreadsConfig::default)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadsConfig {
    pub title_runner_type: String,
    pub stream_idle_window: Duration,
    pub upload_window: Duration,
}

impl Default for ThreadsConfig {
    fn default() -> Self {
        Self {
            title_runner_type: DEFAULT_TITLE_RUNNER_TYPE.to_string(),
            stream_idle_window: DEFAULT_STREAM_IDLE_WINDOW,
            upload_window: DEFAULT_UPLOAD_WINDOW,
        }
    }
}

impl ThreadsConfig {
    pub fn from_env() -> Result<Self, EngineError> {
        let defaults = Self::default();
        Ok(Self {
            title_runner_type: match std::env::var(ENV_TITLE_RUNNER_TYPE) {
                Ok(runner_type) if !runner_type.trim().is_empty() => runner_type,
                Ok(_) => {
                    return Err(EngineError::Config(format!(
                        "{ENV_TITLE_RUNNER_TYPE} is set but blank"
                    )));
                }
                Err(_) => defaults.title_runner_type,
            },
            stream_idle_window: millis(ENV_STREAM_IDLE_MS)?.unwrap_or(defaults.stream_idle_window),
            upload_window: millis(ENV_UPLOAD_WINDOW_MS)?.unwrap_or(defaults.upload_window),
        })
    }

    /// How often the watch looks at a streaming message: a quarter of the idle
    /// window, so a silent writer is failed at most a quarter late.
    pub fn stream_watch_period(&self) -> Duration {
        (self.stream_idle_window / 4).max(Duration::from_millis(100))
    }
}

fn millis(key: &str) -> Result<Option<Duration>, EngineError> {
    match std::env::var(key) {
        Err(_) => Ok(None),
        Ok(raw) => match raw.parse::<u64>() {
            Ok(0) | Err(_) => Err(EngineError::Config(format!(
                "{key} must be a positive whole number of milliseconds, `{raw}` given"
            ))),
            Ok(ms) => Ok(Some(Duration::from_millis(ms))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watch_looks_four_times_per_idle_window() {
        let config = ThreadsConfig {
            stream_idle_window: Duration::from_secs(8),
            ..ThreadsConfig::default()
        };
        assert_eq!(config.stream_watch_period(), Duration::from_secs(2));
        assert_eq!(
            ThreadsConfig::default().stream_watch_period(),
            Duration::from_secs(150)
        );
    }
}
