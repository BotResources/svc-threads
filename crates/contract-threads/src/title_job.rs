//! What the title runner receives. When a thread receives its first user
//! message and has no title, Threads asks Jobs to run one Job of the configured
//! title runner type, with this configuration — the thread and the names of the
//! runner operations, never a URL. The runner reads the beginning of the
//! conversation and sets the title, or declares that it failed, through those
//! operations, which accept only the thread's active title Job.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The GraphQL root that reads a thread's context (`threads:runner`).
pub const CONTEXT_ROOT: &str = "threadsContext";
/// The GraphQL root that sets the title for the active title Job.
pub const TITLE_ROOT: &str = "threadsSetTitle";
/// The GraphQL root that declares the active title Job failed.
pub const FAILURE_ROOT: &str = "threadsFailTitleJob";

/// The `config` of a title Job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleJobConfig {
    /// The service whose operations the runner calls: `threads`.
    pub host: String,
    pub thread_id: Uuid,
    /// The Job's own id, which the runner names on every operation.
    pub job_id: Uuid,
    pub context_root: String,
    pub title_root: String,
    pub failure_root: String,
}

impl TitleJobConfig {
    pub fn new(thread_id: Uuid, job_id: Uuid) -> Self {
        Self {
            host: crate::SERVICE.to_owned(),
            thread_id,
            job_id,
            context_root: CONTEXT_ROOT.to_owned(),
            title_root: TITLE_ROOT.to_owned(),
            failure_root: FAILURE_ROOT.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_title_job_names_its_thread_and_the_runner_operations() {
        let (thread, job) = (Uuid::now_v7(), Uuid::now_v7());
        let config = TitleJobConfig::new(thread, job);
        let wire = serde_json::to_value(&config).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "host": "threads",
                "thread_id": thread,
                "job_id": job,
                "context_root": "threadsContext",
                "title_root": "threadsSetTitle",
                "failure_root": "threadsFailTitleJob"
            })
        );
        assert_eq!(
            serde_json::from_value::<TitleJobConfig>(wire).unwrap(),
            config
        );
    }
}
