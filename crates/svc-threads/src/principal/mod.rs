//! Who is asking: the principal every Threads decision receives, built from the
//! trusted Passport the gateway injects.
//!
//! A person is identified by the Passport and judged by Threads' own copy of
//! Identity's roster (`threads.known_users`), loaded by the engine's
//! principal-fact loader ([`facts`]) on every request and at every refresh. A
//! service account is judged by the scopes its Passport carries. Super-user
//! status is never read: it gives no access to Threads.

mod facts;
mod resolver;

use br_core_auth::{Passport, ScopeKey};
use service_engine::impact::Deps;
use service_engine::principal::{Principal, PrincipalId};
use service_engine::{PassportPrincipal, PrincipalRejected};
use uuid::Uuid;

pub use facts::loader;
pub use resolver::ThreadsPrincipalResolver;

/// The principal-fact bit of the caller's roster standing
/// (`threads.known_users`). The directory mirror stages it for every user whose
/// row moved.
pub const ROSTER_DEP: u8 = 0;
pub const ROSTER_DEPS: Deps = Deps::from_bits(1 << ROSTER_DEP);

/// The principal-fact bit of the threads the caller is a member of. A
/// membership change of a thread is a change of the thread itself (a resource
/// impact on it), and also of what the member's message windows may hold: the
/// change stages this bit for every user added or removed, so that their
/// windows over the thread's messages are repopulated at once.
pub const MEMBERSHIP_DEP: u8 = 1;
pub const MEMBERSHIP_DEPS: Deps = Deps::from_bits(1 << MEMBERSHIP_DEP);

/// A person's standing in Identity's roster, as `threads.known_users` holds it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RosterStanding {
    /// Never published to Threads.
    #[default]
    Unknown,
    /// Published once, since retracted: kept, marked out of the roster, so that
    /// what refers to them stays whole.
    OutOfRoster,
    /// In the roster, deactivated by Identity.
    Deactivated,
    /// In the roster and active, under this display name.
    Active { display_name: String },
}

impl RosterStanding {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }
}

/// What the principal-fact loader read for the caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CallerFacts {
    pub standing: RosterStanding,
}

#[derive(Debug, Clone)]
pub struct ThreadsPrincipal {
    id: PrincipalId,
    passport: Passport,
    facts: CallerFacts,
}

/// The service account Threads acts under when nobody commanded it: the sweep
/// that deletes the threads of an owner who left the roster. No Passport the
/// gateway issues reaches a gesture as Threads itself: its only gesture has no
/// GraphQL root.
const THREADS_ITSELF: Uuid = Uuid::from_u128(0x0199_7000_7468_7265_6164_7300_0000_0001);

impl ThreadsPrincipal {
    /// Threads itself, acting on its own.
    pub fn threads_itself() -> Self {
        Self {
            id: PrincipalId::from(THREADS_ITSELF),
            passport: Passport::service(THREADS_ITSELF, br_core_auth::PassportClaims::new()),
            facts: CallerFacts::default(),
        }
    }

    /// The frontier's actor of this principal.
    pub fn actor(&self) -> br_core_integration::Actor {
        self.passport.to_actor()
    }

    pub fn is_threads_itself(&self) -> bool {
        self.service_account() == Some(THREADS_ITSELF)
    }

    /// The person, when the caller is one.
    pub fn user(&self) -> Option<Uuid> {
        self.passport.user_id()
    }

    /// The service account, when the caller is one.
    pub fn service_account(&self) -> Option<Uuid> {
        self.passport.service_account_id()
    }

    /// The person, when the caller is one the roster holds as active: the only
    /// caller the human surface admits.
    pub fn active_user(&self) -> Option<Uuid> {
        self.user().filter(|_| self.facts.standing.is_active())
    }

    /// Whether the Passport grants `scope`. A declared scope is always a valid
    /// key (`crate::scopes`' tests); one that is not is held by nobody.
    pub fn holds_scope(&self, scope: &str) -> bool {
        ScopeKey::new(scope).is_ok_and(|key| self.passport.has_scope(&key))
    }

    pub fn facts(&self) -> &CallerFacts {
        &self.facts
    }

    pub(crate) fn facts_mut(&mut self) -> &mut CallerFacts {
        &mut self.facts
    }
}

impl Principal for ThreadsPrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

/// The Passport alone decides the one `401` a principal can cause; the roster
/// standing comes from the principal-fact loader.
impl PassportPrincipal for ThreadsPrincipal {
    fn from_passport(passport: Passport) -> Result<Self, PrincipalRejected> {
        let subject = passport
            .user_id()
            .or_else(|| passport.service_account_id())
            .ok_or_else(|| PrincipalRejected::new("the passport names no subject"))?;
        Ok(Self {
            id: PrincipalId::from(subject),
            passport,
            facts: CallerFacts::default(),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use br_core_auth::{AuthMethod, PassportClaims, SCOPES_CLAIM_KEY};

    use super::*;

    pub fn scope_claims(held: &[&str]) -> PassportClaims {
        let mut claims = serde_json::Map::new();
        claims.insert(SCOPES_CLAIM_KEY.to_string(), serde_json::json!(held));
        PassportClaims::from_map(claims)
    }

    /// A person at `standing`, super user or not.
    pub fn person(super_user: bool, standing: RosterStanding) -> ThreadsPrincipal {
        let passport = Passport::human(
            Uuid::now_v7(),
            super_user,
            true,
            AuthMethod::Jwt,
            None,
            scope_claims(&[]),
        );
        let mut principal =
            ThreadsPrincipal::from_passport(passport).unwrap_or_else(|_| panic!("a subject"));
        principal.facts_mut().standing = standing;
        principal
    }

    pub fn active() -> RosterStanding {
        RosterStanding::Active {
            display_name: "Ada".into(),
        }
    }

    pub fn service(held: &[&str]) -> ThreadsPrincipal {
        ThreadsPrincipal::from_passport(Passport::service(Uuid::now_v7(), scope_claims(held)))
            .unwrap_or_else(|_| panic!("a subject"))
    }

    #[test]
    fn only_an_active_person_is_an_active_user() {
        assert!(person(false, active()).active_user().is_some());
        for standing in [
            RosterStanding::Unknown,
            RosterStanding::OutOfRoster,
            RosterStanding::Deactivated,
        ] {
            assert_eq!(person(false, standing).active_user(), None);
        }
        assert_eq!(service(&[crate::scopes::RUNNER]).active_user(), None);
    }

    #[test]
    fn a_service_account_holds_the_scopes_of_its_passport() {
        assert!(service(&[crate::scopes::RUNNER]).holds_scope(crate::scopes::RUNNER));
        assert!(!service(&[]).holds_scope(crate::scopes::RUNNER));
        assert!(service(&[]).service_account().is_some());
    }
}
