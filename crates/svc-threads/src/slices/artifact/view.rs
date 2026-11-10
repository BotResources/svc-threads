//! The read side of an artifact: the two gates the engine's download asks
//! (`Query::download`) — the artifact's visibility to the caller and its blob
//! reference — and nothing else.
//!
//! - [`ArtifactsView`]: a member of its thread ([`Artifact`]'s visibility).
//!   Artifacts are shown to members inside messages, as `ARTIFACT` annotations.
//! - [`RunnerArtifactsView`]: a runner, for an `ATTACHED` artifact of any thread
//!   ([`RunnerReach`]). Artifacts are given to runners beside the conversation
//!   of a thread's context (`crate::slices::artifact::context`).
//!
//! Neither is subscribed to nor registered: each populates nothing, and the
//! engine's download reads one through its types alone.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use uuid::Uuid;

use super::state::{Artifact, ArtifactState, ArtifactStatus, RunnerReach};
use super::store::ArtifactStore;
use crate::principal::ThreadsPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactView {
    pub id: Uuid,
    pub thread_id: Uuid,
    pub message_id: Option<Uuid>,
    pub name: String,
    pub media_type: String,
    pub size: i64,
    pub attached: bool,
}

pub struct ArtifactsView;

impl Projector for ArtifactsView {
    type Principal = ThreadsPrincipal;
    type Noun = Artifact;
    type Store = ArtifactStore;
    type Query = ();
    type Out = ArtifactView;
    type Visibility = Artifact;

    const NAME: ProjectorName = ProjectorName::from_static("artifacts");

    async fn populate(
        _cx: &Populate<'_, ThreadsPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(BTreeSet::new()))
    }

    fn project(
        artifact: &ArtifactState,
        _principal: &ThreadsPrincipal,
    ) -> Result<ArtifactView, EngineError> {
        Ok(view_of(artifact))
    }
}

pub struct RunnerArtifactsView;

impl Projector for RunnerArtifactsView {
    type Principal = ThreadsPrincipal;
    type Noun = Artifact;
    type Store = ArtifactStore;
    type Query = ();
    type Out = ArtifactView;
    type Visibility = RunnerReach;

    const NAME: ProjectorName = ProjectorName::from_static("runner_artifacts");

    async fn populate(
        _cx: &Populate<'_, ThreadsPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(BTreeSet::new()))
    }

    fn project(
        artifact: &ArtifactState,
        _principal: &ThreadsPrincipal,
    ) -> Result<ArtifactView, EngineError> {
        Ok(view_of(artifact))
    }
}

fn view_of(artifact: &ArtifactState) -> ArtifactView {
    ArtifactView {
        id: artifact.id,
        thread_id: artifact.thread_id,
        message_id: artifact.message_id,
        name: artifact.name.clone(),
        media_type: artifact.media_type.clone(),
        size: artifact.size,
        attached: artifact.status == ArtifactStatus::Attached,
    }
}
