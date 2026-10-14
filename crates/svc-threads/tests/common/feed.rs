//! Channel 3 — following a member's list and a thread live, over either
//! transport the engine's ops contract declares: SSE (the gateway's leg) and
//! `graphql-transport-ws` (the direct leg). Every stream opens on a `Reset`
//! snapshot, then carries ordered `Upsert` / `Remove` deltas on a contiguous
//! revision; a reconnect opens on a fresh `Reset`.
//!
//! The harness handles take a bare document, so the ids a subscription names
//! are written into it.

use br_core_auth::Passport;
use br_test_harness::{SseOutcome, SseSubscription, WsError, WsSubscription};
use serde_json::Value;
use uuid::Uuid;

use super::{MESSAGE_FIELDS, PUSH_TIMEOUT, QUIET_WINDOW, THREAD_FIELDS, TestContext};

pub const THREAD_FEED: &str = "threadsThreadDeltas";
pub const THREADS_LIST: &str = "threadsThreadsDeltas";

/// Following `thread` live: the thread and the head page of its active branch.
pub fn thread_feed(thread: Uuid, size: u32) -> String {
    let view = format!(
        "__typename ... on ThreadView {{ {THREAD_FIELDS} }} ... on MessageView {{ {MESSAGE_FIELDS} }}"
    );
    format!(
        "subscription {{ {THREAD_FEED}(threadId: \"{thread}\", size: {size}) {{ __typename \
         ... on ThreadFeedReset {{ revision views {{ {view} }} }} \
         ... on ThreadFeedUpsert {{ revision view {{ {view} }} }} \
         ... on ThreadFeedRemove {{ revision projector key }} }} }}"
    )
}

/// Following one's list of threads live: its head page.
pub fn threads_list(size: u32) -> String {
    let view = format!("__typename ... on ThreadView {{ {THREAD_FIELDS} }}");
    format!(
        "subscription {{ {THREADS_LIST}(size: {size}) {{ __typename \
         ... on ThreadsReset {{ revision views {{ {view} }} }} \
         ... on ThreadsUpsert {{ revision view {{ {view} }} }} \
         ... on ThreadsRemove {{ revision projector key }} }} }}"
    )
}

enum Transport {
    Sse(SseSubscription),
    Ws(Box<WsSubscription>),
}

/// One live view, on one transport, past its opening `Reset`.
pub struct Live {
    root: &'static str,
    transport: Transport,
    revision: Option<i64>,
    /// The snapshot the stream opened on.
    pub reset: Value,
}

impl Live {
    /// Opens `document` over SSE and reads its `Reset`.
    pub async fn sse(
        ctx: &TestContext,
        passport: &Passport,
        root: &'static str,
        document: &str,
    ) -> Self {
        let sse = SseSubscription::open(&ctx.base_url, passport, document).await;
        Self::opened(root, Transport::Sse(sse)).await
    }

    /// Opens `document` over WebSocket and reads its `Reset`.
    pub async fn ws(
        ctx: &TestContext,
        passport: &Passport,
        root: &'static str,
        document: &str,
    ) -> Self {
        let ws = WsSubscription::open(&ctx.base_url, passport, document)
            .await
            .expect("open the WebSocket subscription");
        Self::opened(root, Transport::Ws(Box::new(ws))).await
    }

    /// `passport` follows `thread`, over SSE.
    pub async fn thread(ctx: &TestContext, passport: &Passport, thread: Uuid) -> Self {
        Self::sse(ctx, passport, THREAD_FEED, &thread_feed(thread, 20)).await
    }

    /// `passport` follows their list, over SSE.
    pub async fn list(ctx: &TestContext, passport: &Passport) -> Self {
        Self::sse(ctx, passport, THREADS_LIST, &threads_list(20)).await
    }

    async fn opened(root: &'static str, transport: Transport) -> Self {
        let mut live = Self {
            root,
            transport,
            revision: None,
            reset: Value::Null,
        };
        let reset = live
            .next(PUSH_TIMEOUT)
            .await
            .unwrap_or_else(|| panic!("{root}: no opening frame"));
        assert!(
            reset["__typename"]
                .as_str()
                .is_some_and(|name| name.ends_with("Reset")),
            "{root}: a stream opens on a Reset: {reset}"
        );
        live.revision = Some(reset["revision"].as_i64().expect("a Reset's revision"));
        live.reset = reset;
        live
    }

    /// The next delta, if one arrives within `timeout`. A delta's revision
    /// follows the previous one's, contiguous.
    pub async fn next(&mut self, timeout: std::time::Duration) -> Option<Value> {
        let frame = match &mut self.transport {
            Transport::Sse(sse) => match sse.next_outcome(timeout).await {
                SseOutcome::Event(data) => Some(data),
                SseOutcome::Timeout => None,
                SseOutcome::Closed => panic!("{}: the server closed the stream", self.root),
                other => panic!("{}: {other:?}", self.root),
            },
            Transport::Ws(ws) => match ws.next_data_outcome(timeout).await {
                Ok(data) => Some(data),
                Err(WsError::Timeout) => None,
                Err(other) => panic!("{}: {other}", self.root),
            },
        }?;
        let delta = frame[self.root].clone();
        assert!(
            !delta.is_null(),
            "{}: a frame without its root: {frame}",
            self.root
        );
        if let (Some(revision), Some(previous)) = (delta["revision"].as_i64(), self.revision) {
            assert_eq!(
                revision,
                previous + 1,
                "{}: deltas follow a contiguous revision: {delta}",
                self.root
            );
            self.revision = Some(revision);
        }
        Some(delta)
    }

    /// Reads deltas until one satisfies `holds`; answers it.
    pub async fn until<F>(&mut self, what: &str, holds: F) -> Value
    where
        F: Fn(&Value) -> bool,
    {
        let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
        let mut seen = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(remaining).await {
                Some(delta) if holds(&delta) => return delta,
                Some(delta) => seen.push(delta),
                None => panic!(
                    "{what}: no matching delta on {} within {PUSH_TIMEOUT:?}; seen: {}",
                    self.root,
                    serde_json::to_string(&seen).unwrap_or_default()
                ),
            }
        }
    }

    /// Nothing arrives over the quiet window.
    pub async fn silence(&mut self, what: &str) {
        if let Some(delta) = self.next(QUIET_WINDOW).await {
            panic!("{what}: no delta expected on {}, got {delta}", self.root);
        }
    }

    /// Every delta that arrives over the quiet window.
    pub async fn drain(&mut self) -> Vec<Value> {
        let mut deltas = Vec::new();
        while let Some(delta) = self.next(QUIET_WINDOW).await {
            deltas.push(delta);
        }
        deltas
    }
}

/// The views of a `Reset`.
pub fn views(reset: &Value) -> Vec<Value> {
    reset["views"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| panic!("a Reset carries views: {reset}"))
}

/// The view of an `Upsert` naming `id`, if the delta is one.
pub fn upsert_of(delta: &Value, id: Uuid) -> Option<&Value> {
    let is_upsert = delta["__typename"]
        .as_str()
        .is_some_and(|name| name.ends_with("Upsert"));
    (is_upsert && delta["view"]["id"] == Value::String(id.to_string())).then(|| &delta["view"])
}

/// Whether the delta is a `Remove` whose key names `id`.
pub fn removes(delta: &Value, id: Uuid) -> bool {
    delta["__typename"]
        .as_str()
        .is_some_and(|name| name.ends_with("Remove"))
        && delta["key"].to_string().contains(&id.to_string())
}

/// The view of `id` in a `Reset`, if there.
pub fn in_reset(reset: &Value, id: Uuid) -> Option<Value> {
    views(reset)
        .into_iter()
        .find(|view| view["id"] == Value::String(id.to_string()))
}

/// How a WebSocket subscription to `document` opens, as text, every id of
/// `ids` blanked: two subscriptions refused alike read the same.
pub async fn ws_opening(
    ctx: &TestContext,
    passport: &Passport,
    document: &str,
    ids: &[Uuid],
) -> String {
    let outcome = match WsSubscription::open(&ctx.base_url, passport, document).await {
        Err(refused) => format!("refused at open: {refused}"),
        Ok(mut ws) => match ws.next_data_outcome(PUSH_TIMEOUT).await {
            Ok(data) => format!("data: {data}"),
            Err(error) => format!("error: {error}"),
        },
    };
    ids.iter()
        .fold(outcome, |text, id| text.replace(&id.to_string(), "<id>"))
}
