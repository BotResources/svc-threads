//! The coordinates of Threads' commands and events. Callers hand these to the
//! Fabric (or the engine), which renders and validates the subject — never a
//! hand-built string.

use br_core_integration::{Aggregate, Bc, CommandCoords, EventCoords, PastFact, Verb};
use uuid::Uuid;

/// The service: receiver of its commands, producer of its events, owner of its
/// manifest key and of its `STREAMING_threads` stream.
pub const SERVICE: &str = "threads";

/// The aggregate segment of the thread commands and events.
pub const THREAD_AGGREGATE: &str = "thread";
/// The aggregate segment of the message commands and events.
pub const MESSAGE_AGGREGATE: &str = "message";

/// The schema version of every command and event of this contract.
pub const SCHEMA_VERSION: u8 = 1;

/// `integration.cmd.threads.{aggregate}.{verb}.v1`.
pub fn command_coords(aggregate: &'static str, verb: &'static str) -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("`threads` is a valid bounded-context segment"),
        aggregate: Aggregate::new(aggregate).expect("a static aggregate segment of this contract"),
        verb: Verb::new(verb).expect("a static verb segment of this contract"),
        version: SCHEMA_VERSION,
    }
}

/// `integration.evt.threads.{aggregate}.{fact}.v1`.
pub fn event_coords(aggregate: &'static str, fact: &'static str) -> EventCoords {
    EventCoords {
        producer: Bc::new(SERVICE).expect("`threads` is a valid bounded-context segment"),
        aggregate: Aggregate::new(aggregate).expect("a static aggregate segment of this contract"),
        fact: PastFact::new(fact).expect("a static fact segment of this contract"),
        version: SCHEMA_VERSION,
    }
}

/// A command Threads accepts. Every command names the thread it acts on and,
/// optionally, the user whose gesture led the commanding service to send it —
/// recorded as the actor of the fact the command produces.
pub trait ThreadsCommand {
    /// `thread` or `message`.
    const AGGREGATE: &'static str;
    /// The verb segment, also the second half of the envelope's
    /// `command_type`.
    const VERB: &'static str;

    fn coords() -> CommandCoords {
        command_coords(Self::AGGREGATE, Self::VERB)
    }

    /// The envelope's `command_type`: `{aggregate}.{verb}`.
    fn command_type() -> String {
        format!("{}.{}", Self::AGGREGATE, Self::VERB)
    }

    fn thread_id(&self) -> Uuid;

    fn acting_user_id(&self) -> Option<Uuid>;
}
