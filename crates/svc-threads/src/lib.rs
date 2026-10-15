//! Threads — the shared, passive conversation store. It keeps conversations
//! between people and AIs and serves them, live, to their members; it never
//! decides who may write (trusted services command it), it decides who may
//! read (the members of a thread, and nobody else), and it publishes the members
//! of each thread so that other services decide locally.
//!
//! A fatty service on `br-service-engine` (soft EDA, multi-pod). The published
//! language is the `contract-threads` crate.
//!
//! Layout, by capability:
//!
//! - [`slices::thread`]: a thread, its members, resources and read marks — the
//!   thread views, the members KV offer;
//! - [`slices::message`]: the messages of a thread, their items and blocks, the
//!   accumulated lane their streams fold into, and the thread's live feed;
//! - [`slices::artifact`]: the files runners attach to messages, in object
//!   storage;
//! - [`slices::runner`]: the operations reserved to runners — a thread's
//!   context, and the title Job's.
//!
//! Shared mechanics: [`boot`] (the engine boot kit), [`config`] (Threads' own
//! settings), [`register`] (everything the engine is told), [`principal`] (who
//! is asking), [`access`] (the two admission rules every root asks first),
//! [`directory`] (the local copy of Identity's roster), [`command`] (a command
//! reaction's sender check and its one answer), [`facts`] (the fact log),
//! [`scopes`], [`fault`].

pub mod access;
pub mod boot;
pub mod command;
pub mod config;
pub mod directory;
pub mod facts;
pub mod fault;
mod literal;
pub mod principal;
pub mod register;
pub mod scopes;
pub mod slices;

/// The service name: the engine's service (its `STREAMING_threads` stream and
/// manifest key) and the root prefix of every GraphQL field (`threads…`).
pub const SERVICE: &str = contract_threads::SERVICE;
