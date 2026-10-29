use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CONTEXT_ROOT: &str = "threadsContext";
pub const TITLE_ROOT: &str = "threadsSetTitle";
pub const FAILURE_ROOT: &str = "threadsFailTitleJob";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TitleJobConfig {
    pub host: String,
    pub thread_id: Uuid,
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
