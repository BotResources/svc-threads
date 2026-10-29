use br_core_integration::{Aggregate, Bc, CommandCoords, EventCoords, PastFact, Verb};
use uuid::Uuid;

pub const SERVICE: &str = "threads";

pub const THREAD_AGGREGATE: &str = "thread";
pub const MESSAGE_AGGREGATE: &str = "message";

pub const SCHEMA_VERSION: u8 = 1;

pub fn command_coords(aggregate: &'static str, verb: &'static str) -> CommandCoords {
    versioned_command_coords(aggregate, verb, SCHEMA_VERSION)
}

pub fn versioned_command_coords(
    aggregate: &'static str,
    verb: &'static str,
    version: u8,
) -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("`threads` is a valid bounded-context segment"),
        aggregate: Aggregate::new(aggregate).expect("a static aggregate segment of this contract"),
        verb: Verb::new(verb).expect("a static verb segment of this contract"),
        version,
    }
}

pub fn event_coords(aggregate: &'static str, fact: &'static str) -> EventCoords {
    EventCoords {
        producer: Bc::new(SERVICE).expect("`threads` is a valid bounded-context segment"),
        aggregate: Aggregate::new(aggregate).expect("a static aggregate segment of this contract"),
        fact: PastFact::new(fact).expect("a static fact segment of this contract"),
        version: SCHEMA_VERSION,
    }
}

pub trait ThreadsCommand {
    const AGGREGATE: &'static str;
    const VERB: &'static str;
    const VERSION: u8 = SCHEMA_VERSION;

    fn coords() -> CommandCoords {
        versioned_command_coords(Self::AGGREGATE, Self::VERB, Self::VERSION)
    }

    fn command_type() -> String {
        format!("{}.{}", Self::AGGREGATE, Self::VERB)
    }

    fn thread_id(&self) -> Uuid;

    fn acting_user_id(&self) -> Option<Uuid>;
}
