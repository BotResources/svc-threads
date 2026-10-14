//! Everything the engine is told at boot, in one place: the principal, its
//! resolver and its facts, the declared scope, the directory mirror, and the
//! composed capabilities (each registers its own views, offer, accumulator and
//! blob kind).

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::principal::{self, ThreadsPrincipal, ThreadsPrincipalResolver};
use crate::scopes;

pub fn all(engine: &mut Engine<ThreadsPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(ThreadsPrincipalResolver)?;
    engine.register_principal_fact(principal::loader)?;
    // Declared by the boot kit's handshake (`BootPlan::declare_scopes`), which
    // holds readiness until Identity accepts them and fails loud on a refusal.
    engine.contribute_scope_defs(&scopes::DECLARED)?;
    engine.describe_scope_service(scopes::SERVICE_LABEL_KEY, scopes::SERVICE_DESCRIPTION_KEY)?;
    engine.register_mirror(crate::directory::directory_mirror())?;
    crate::slices::register(engine)
}
