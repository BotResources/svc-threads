//! The GraphQL surface, every root field under the `threads` prefix
//! (`threads…`).
//!
//! `compose_service!` lists each capability once: it generates the slice
//! modules, the merged `QueryRoot` / `MutationRoot` / `SubscriptionRoot` and the
//! `register` that declares the prefix before any slice registers.
//!
//! The capabilities without a subscription compose async-graphql's
//! `EmptySubscription`, which answers every subscription field it is asked with
//! "not configured" instead of passing it on — so they come last, after the two
//! capabilities that stream.
//!
//! Every capability registers its schema fragment under one name, [`FRAGMENT`]:
//! the views share types (`ThreadView` is a member of two delta unions,
//! `MemberView` is read by runners), and the engine accepts shared types only
//! within one fragment name.

/// The name every capability's schema fragment is registered under.
pub const FRAGMENT: &str = "threads";

service_engine::compose_service! {
    principal = crate::principal::ThreadsPrincipal;
    prefix = threads;
    slice thread ["thread"] { query = thread::graphql::ThreadQuery, mutation = thread::graphql::ThreadMutation, subscription = thread::graphql::ThreadSubscription }
    slice message ["message"] { query = message::graphql::MessageQuery, subscription = message::graphql::MessageSubscription }
    slice artifact ["artifact"] { query = artifact::graphql::ArtifactQuery, mutation = artifact::graphql::ArtifactMutation, subscription = async_graphql::EmptySubscription }
    slice runner ["runner"] { query = runner::graphql::RunnerQuery, mutation = runner::graphql::RunnerMutation, subscription = async_graphql::EmptySubscription }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::{MutationRoot, QueryRoot, SubscriptionRoot};

    fn sdl() -> String {
        async_graphql::Schema::build(
            QueryRoot::default(),
            MutationRoot::default(),
            SubscriptionRoot::default(),
        )
        .finish()
        .sdl()
    }

    #[test]
    fn every_root_is_served_under_the_threads_prefix() {
        let sdl = sdl();
        for root in [
            "threadsThreads(",
            "threadsThread(",
            "threadsMessages(",
            "threadsArtifactDownload(",
            "threadsContext(",
            "threadsSetReadMark(",
            "threadsRequestArtifactUpload(",
            "threadsSetTitle(",
            "threadsThreadsDeltas(",
            "threadsThreadDeltas(",
        ] {
            assert!(sdl.contains(root), "{root} is missing from the SDL:\n{sdl}");
        }
    }

    /// The runner sets a title without naming a Job, and has no title Job to
    /// declare failed.
    #[test]
    fn the_runner_sets_a_title_without_a_job_and_the_failure_root_is_gone() {
        let sdl = sdl();
        assert!(
            sdl.contains("threadsSetTitle(threadId: UUID!, title: String!): MutationAck!"),
            "threadsSetTitle takes a thread and a title only:\n{sdl}"
        );
        assert!(
            !sdl.contains("threadsFailTitleJob("),
            "threadsFailTitleJob is still a root:\n{sdl}"
        );
    }

    /// Every subscription root reaches its own resolver — which, with no engine
    /// behind this schema, answers a missing-context error.
    #[tokio::test]
    async fn every_subscription_root_reaches_its_own_resolver() {
        let schema = async_graphql::Schema::build(
            QueryRoot::default(),
            MutationRoot::default(),
            SubscriptionRoot::default(),
        )
        .finish();
        let thread = uuid::Uuid::now_v7();
        for subscription in [
            "subscription { threadsThreadsDeltas(size: 10) { __typename } }".to_string(),
            format!(
                "subscription {{ threadsThreadDeltas(threadId: \"{thread}\", size: 10) {{ __typename }} }}"
            ),
        ] {
            let first = schema
                .execute_stream(subscription.as_str())
                .next()
                .await
                .expect("a subscription answers at least once");
            let messages: Vec<String> = first.errors.iter().map(|e| e.message.clone()).collect();
            assert!(
                !messages.iter().any(|m| m.contains("not configured")),
                "{subscription} was not routed to its resolver: {messages:?}"
            );
        }
    }
}
