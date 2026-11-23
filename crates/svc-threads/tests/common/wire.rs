//! The two commands of 0.12.0 that `contract-threads` 0.9.0 does not carry,
//! built in the suite on the wire the registry's offers fix, so that the suite
//! compiles before the contract exposes them:
//!
//! - `integration.cmd.threads.message.store.v3` — `message.store` v2 without
//!   `parent_message_id`: a message names no other message;
//! - `integration.cmd.threads.message.erase.v1` — erase the end of a thread:
//!   every message stored after the named one, and the named one when
//!   `including` is true.
//!
//! Each is a `contract_threads::ThreadsCommand`, so it travels through
//! `flows::command` on its contract coordinates, under the envelope's schema
//! version it declares. Once `contract-threads` exposes its own types for
//! them, the suite switches to those and this file goes.

use contract_threads::coords::MESSAGE_AGGREGATE;
use contract_threads::{MessageTurn, ThreadsCommand};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `message.store` v3: a message stored on an `OPEN` thread, complete or
/// opened `STREAMING`. The same fields as v2, without a parent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreMessageV3 {
    pub message_id: Uuid,
    pub thread_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonator_id: Option<Uuid>,
    pub message: MessageTurn,
}

impl ThreadsCommand for StoreMessageV3 {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "store";
    const VERSION: u8 = 3;

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}

/// `message.erase` v1: every message of `thread_id` stored after
/// `message_id` is removed for good, and `message_id` itself when
/// `including`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EraseMessages {
    pub thread_id: Uuid,
    pub message_id: Uuid,
    pub including: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acting_user_id: Option<Uuid>,
}

impl ThreadsCommand for EraseMessages {
    const AGGREGATE: &'static str = MESSAGE_AGGREGATE;
    const VERB: &'static str = "erase";

    fn thread_id(&self) -> Uuid {
        self.thread_id
    }

    fn acting_user_id(&self) -> Option<Uuid> {
        self.acting_user_id
    }
}
