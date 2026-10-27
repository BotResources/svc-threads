//! The published language of `svc-threads`, the shared conversation store.
//!
//! What another service imports to talk to Threads, and nothing else:
//!
//! - [`thread`], [`message`]: the commands Threads accepts on
//!   `integration.cmd.threads.{thread|message}.{verb}.v{N}`, sent by trusted
//!   services that already took the decision;
//! - [`event`]: the one confirmation or refusal Threads publishes per command,
//!   on `integration.evt.threads.{thread|message}.{confirmed|refused}.v1`;
//! - [`members`]: the `threads.members.{thread_id}` KV entries of the
//!   `PUBLISHED_LANGUAGE` bucket;
//! - [`library`]: the library's JSON a message carries — a message is a turn
//!   of the conversation model of `br-llm-messages` 2.1 (schema
//!   [`llm::CONVERSATION_SCHEMA`]), re-exported as [`llm`];
//! - [`annotation`]: Threads' own blocks, placed after an entry of a message;
//! - [`invalid`]: the vocabulary of a `MESSAGE_INVALID` refusal's `error`
//!   param;
//! - [`chunk`]: the content of a `STREAMING` message as its writer pushes it on
//!   the engine's accumulated lane (`stream.threads.{message_id}`), and the seal
//!   digest that completes it;
//! - [`title_job`]: the configuration Threads puts in the title Job it asks Jobs
//!   to run;
//! - [`v1`]: the deprecated 0.4 forms Threads still converts.
//!
//! The contract decodes shapes; Threads judges values. A command whose payload
//! decodes is always answered — a value the conversation model or a business
//! rule refuses is a refusal with its [`ReasonCode`], never a dead letter.
//! Library content therefore travels as the library's JSON ([`LibraryJson`])
//! and is parsed by Threads, not at decode time.

pub mod annotation;
pub mod chunk;
pub mod coords;
pub mod event;
pub mod invalid;
pub mod library;
pub mod members;
pub mod message;
pub mod reason;
pub mod status;
pub mod thread;
pub mod title_job;
pub mod v1;
pub mod value;
mod wire;

/// The conversation model, re-exported so that a commander builds content with
/// the exact library version Threads validates against.
pub use br_llm_messages as llm;

pub use annotation::{
    Annotation, AnnotationBody, AnnotationKind, ArtifactRef, JobRef, Notice, PlacedAnnotation,
    ThreadRef,
};
pub use chunk::{
    Chunk, ChunkFrame, STREAMING_STREAM, TURN_CONTENT_ACCUMULATOR, chunk_subject, seal_hash,
};
pub use coords::{SERVICE, ThreadsCommand};
pub use event::{
    MessageCommandConfirmed, MessageCommandRefused, ThreadCommandConfirmed, ThreadCommandRefused,
    ThreadsEvent,
};
pub use invalid::MessageInvalidError;
pub use library::LibraryJson;
pub use members::{MEMBERS_MANIFEST_KEY, MEMBERS_PREFIX, MEMBERS_VERSION, PublishedThreadMembers};
pub use message::{
    AiDelivery, AiTurn, CancelMessage, CompleteMessage, DeleteMessage, EditMessage, FailMessage,
    MessageTurn, StoreMessageV2, StreamSeal, UserTurn,
};
pub use reason::ReasonCode;
pub use status::{ClosingStatus, MessageStatus, ThreadStatus};
pub use thread::{
    AddMembers, AddResourceIds, CloseThread, CreateThread, DeleteThread, RemoveMembers,
    RemoveResourceIds, ReopenThread, ResolveThread, SetTitle,
};
pub use title_job::TitleJobConfig;
pub use value::{SealDigest, SealDigestError, ThreadTitle, ThreadTitleError, TitleTooLong};
pub use wire::UnknownLiteral;
