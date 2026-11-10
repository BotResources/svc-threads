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
            "threadsChildThreads(",
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

    /// The children of a thread are read like the caller's list, the same
    /// view, by pages after a given child.
    #[test]
    fn the_children_of_a_thread_are_a_page_of_thread_views_after_a_child() {
        let sdl = sdl();
        assert!(
            sdl.contains(
                "threadsChildThreads(threadId: UUID!, size: Int!, after: UUID): [ThreadView!]!"
            ),
            "threadsChildThreads takes a thread, a size and an optional child:\n{sdl}"
        );
        assert!(
            !sdl.contains("threadsChildThreadsDeltas("),
            "the children of a thread are a query only:\n{sdl}"
        );
    }

    /// A runner's context carries the attached artifacts of its conversation,
    /// each with a read access resolved on demand, under the names a runner
    /// library binds to.
    #[test]
    fn the_runner_context_carries_its_artifacts_with_a_read_access_on_demand() {
        let sdl = sdl();
        assert!(
            sdl.contains("\tartifacts: [ContextArtifact!]!\n"),
            "ThreadContext.artifacts is missing:\n{sdl}"
        );
        let start = sdl
            .find("type ContextArtifact {")
            .unwrap_or_else(|| panic!("ContextArtifact is missing:\n{sdl}"));
        let body = &sdl[start..start + sdl[start..].find("\n}").expect("a closed type")];
        let mut fields: Vec<&str> = body
            .lines()
            .skip(1)
            .map(str::trim)
            .filter(|line| {
                line.split_once(": ")
                    .is_some_and(|(field, _)| field.chars().all(char::is_alphanumeric))
            })
            .collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            [
                "downloadUrl: String",
                "id: UUID!",
                "mediaType: String!",
                "messageId: UUID!",
                "name: String!",
                "sha256: String!",
                "size: Int!",
            ],
            "{body}"
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
