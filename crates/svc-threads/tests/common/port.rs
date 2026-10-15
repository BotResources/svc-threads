//! Loopback ports for the processes a scenario spawns (the service, an object store).
//!
//! The suite runs its scenarios IN PARALLEL under cargo-nextest — one process per
//! scenario, several at once — and a port probed free is reserved by nothing: between
//! the probe and the child's `bind()`, a sibling scenario may draw the same number, and
//! so may any outgoing connection (the kernel hands those an ephemeral port). The probe
//! alone is a race, so the draw is made so that nothing else ever draws from the same
//! numbers:
//!
//! * nextest gives every concurrently running scenario a distinct
//!   `NEXTEST_TEST_GLOBAL_SLOT` (0, 1, …, test-threads − 1, reused only once the slot's
//!   previous scenario has finished). A scenario draws only from its slot's own block of
//!   [`BLOCK`] ports, so two scenarios running at the same time never probe the same
//!   port;
//! * every block sits between [`FLOOR`] and 32768 — below the kernel's ephemeral range
//!   on Linux (32768–60999) and on macOS (49152–65535) — so no outgoing connection is
//!   ever given one of them either.
//!
//! What is left — a stray process of the host sitting on a port of the block — is what
//! the probe is for, and a bind that fails anyway is retried on a fresh port by the boot
//! that spawned it. Outside nextest (a plain `cargo test`) there is no slot, and the
//! draw falls back to a kernel-assigned port, as before.
//!
//! This file is copied verbatim into every service's `tests/common/`: each suite is its
//! own crate, and a shared test-support crate for thirty lines is not worth its weight.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};

/// The variable nextest sets on every test process: its slot among the running tests.
const SLOT_ENV: &str = "NEXTEST_TEST_GLOBAL_SLOT";

/// The first port of slot 0's block.
const FLOOR: u16 = 20_000;

/// Ports per slot: far more than one scenario spawns (a service, re-spawned on a
/// restart, and an object store with its console).
const BLOCK: u16 = 100;

/// How many slots fit below the lowest ephemeral port (32768).
const SLOTS: u16 = (32_768 - FLOOR) / BLOCK;

/// A loopback port nothing listens on, drawn from this scenario's own block under
/// nextest and from the kernel otherwise.
pub fn free_port() -> u16 {
    slot().and_then(from_block).unwrap_or_else(kernel_assigned)
}

fn slot() -> Option<u16> {
    let slot: u16 = std::env::var(SLOT_ENV).ok()?.parse().ok()?;
    (slot < SLOTS).then_some(slot)
}

/// The next free port of the slot's block. The walk starts at an offset taken from the
/// pid, so a scenario that inherits a slot does not begin with the ports its
/// predecessor just released, and a counter moves it on, so two draws of one scenario
/// (the service and its object store) never return the same port.
fn from_block(slot: u16) -> Option<u16> {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let base = FLOOR + slot * BLOCK;
    let start = (std::process::id() % u32::from(BLOCK)) as u16;
    (0..BLOCK).find_map(|_| {
        let step = NEXT.fetch_add(1, Ordering::Relaxed) % BLOCK;
        let port = base + (start + step) % BLOCK;
        TcpListener::bind(("127.0.0.1", port)).ok().map(|_| port)
    })
}

fn kernel_assigned() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve a loopback port")
        .local_addr()
        .expect("read the reserved port")
        .port()
}
