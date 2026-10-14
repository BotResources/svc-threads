use std::collections::BTreeSet;

use contract_threads::{ClosingStatus, CreateThread, ReasonCode, ThreadStatus};

use super::*;
use crate::slices::thread::state::tests::thread_with;

fn meta() -> FactMeta {
    FactMeta {
        actor: br_core_integration::Actor::Service(Uuid::now_v7().into()),
        acting_user_id: Some(Uuid::now_v7()),
        impersonator_id: None,
        correlation_id: Uuid::now_v7(),
        causation_id: None,
        occurred_at: Utc::now(),
    }
}

fn creation(owner: Option<Uuid>, users: &[Uuid]) -> CreateThread {
    CreateThread {
        thread_id: Uuid::now_v7(),
        owner_id: owner,
        user_ids: users.to_vec(),
        title: None,
        resources: Default::default(),
        parent_thread_id: None,
        origin_block_id: None,
        acting_user_id: None,
    }
}

fn roster(users: &[Uuid]) -> BTreeSet<Uuid> {
    users.iter().copied().collect()
}

fn fact_types(thread: &ThreadState) -> Vec<&'static str> {
    thread
        .pending_facts()
        .iter()
        .map(|fact| fact.event_type)
        .collect()
}

fn refused(verdict: Result<Applied, Refusal>) -> ReasonCode {
    verdict.expect_err("a refusal").code
}

#[test]
fn a_creation_makes_the_owner_a_member_at_version_one() {
    let (owner, a, b) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let command = creation(Some(owner), &[a, b, a]);
    let thread = create(
        &command,
        false,
        &roster(&[owner, a, b]),
        &ParentLookup::NotNamed,
        &meta(),
        Utc::now(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(thread.status, ThreadStatus::Open);
    assert_eq!(thread.version, 1);
    assert_eq!(thread.member_ids(), roster(&[owner, a, b]));
    assert_eq!(fact_types(&thread), [CREATED, MEMBERS_ADDED]);
    let added = &thread.pending_facts()[1].payload;
    assert_eq!(added["user_ids"].as_array().unwrap().len(), 3);
}

#[test]
fn an_ownerless_creation_has_no_owner_for_good() {
    let a = Uuid::now_v7();
    let thread = create(
        &creation(None, &[a]),
        false,
        &roster(&[a]),
        &ParentLookup::NotNamed,
        &meta(),
        Utc::now(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(thread.owner_id, None);
    assert!(thread.is_member(a));
}

#[test]
fn a_creation_is_refused_in_the_order_of_its_rules() {
    let (owner, stranger) = (Uuid::now_v7(), Uuid::now_v7());
    let known = roster(&[owner]);
    let refusal = |command: &CreateThread, exists: bool, parent: &ParentLookup| {
        create(command, exists, &known, parent, &meta(), Utc::now())
            .unwrap()
            .unwrap_err()
    };
    let plain = creation(Some(owner), &[]);
    assert_eq!(
        refusal(&plain, true, &ParentLookup::NotNamed).code,
        ReasonCode::KeyReused
    );

    let mut long = creation(Some(owner), &[]);
    long.title = Some("é".repeat(201));
    let too_long = refusal(&long, false, &ParentLookup::NotNamed);
    assert_eq!(too_long.code, ReasonCode::TitleTooLong);
    assert_eq!(too_long.params["chars"], 201);
    long.title = Some("é".repeat(200));
    assert!(
        create(
            &long,
            false,
            &known,
            &ParentLookup::NotNamed,
            &meta(),
            Utc::now()
        )
        .unwrap()
        .is_ok()
    );

    let unknown = refusal(
        &creation(Some(owner), &[stranger]),
        false,
        &ParentLookup::NotNamed,
    );
    assert_eq!(unknown.code, ReasonCode::UserUnknown);
    assert_eq!(unknown.params["user_ids"], serde_json::json!([stranger]));

    let mut orphan = creation(Some(owner), &[]);
    orphan.parent_thread_id = Some(Uuid::now_v7());
    assert_eq!(
        refusal(&orphan, false, &ParentLookup::Missing).code,
        ReasonCode::OriginBlockRequired
    );
    orphan.origin_block_id = Some(Uuid::now_v7());
    assert_eq!(
        refusal(&orphan, false, &ParentLookup::Missing).code,
        ReasonCode::ThreadNotFound
    );
    assert_eq!(
        refusal(&orphan, false, &ParentLookup::Found(roster(&[stranger]))).code,
        ReasonCode::OwnerNotParentMember
    );
    assert!(
        create(
            &orphan,
            false,
            &known,
            &ParentLookup::Found(roster(&[owner])),
            &meta(),
            Utc::now()
        )
        .unwrap()
        .is_ok()
    );
}

#[test]
fn an_ownerless_child_skips_the_owner_check() {
    let member = Uuid::now_v7();
    let mut child = creation(None, &[member]);
    child.parent_thread_id = Some(Uuid::now_v7());
    child.origin_block_id = Some(Uuid::now_v7());
    let thread = create(
        &child,
        false,
        &roster(&[member]),
        &ParentLookup::Found(BTreeSet::new()),
        &meta(),
        Utc::now(),
    )
    .unwrap()
    .unwrap();
    assert!(thread.origin.is_some());
}

#[test]
fn adding_members_already_there_changes_nothing() {
    let (owner, a, b) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut thread = thread_with(&[owner, a]);
    let known = roster(&[owner, a, b]);
    assert_eq!(
        thread.add_members(&[a], &known, &meta()).unwrap(),
        Ok(Applied::Unchanged)
    );
    assert_eq!(thread.version, 1);
    assert!(thread.pending.is_empty());
    assert_eq!(
        thread.add_members(&[a, b, b], &known, &meta()).unwrap(),
        Ok(Applied::Changed)
    );
    assert_eq!(thread.version, 2);
    assert_eq!(
        thread.pending.first(),
        Some(&ThreadChange::MembersAdded(vec![b]))
    );
}

#[test]
fn one_user_out_of_the_roster_refuses_the_whole_list() {
    let (owner, newcomer, gone) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut thread = thread_with(&[owner]);
    let verdict = thread
        .add_members(&[newcomer, gone], &roster(&[owner, newcomer]), &meta())
        .unwrap();
    assert_eq!(refused(verdict), ReasonCode::UserUnknown);
    assert!(!thread.is_member(newcomer));
}

#[test]
fn the_owner_is_never_removed_and_a_removed_member_loses_their_mark() {
    let (owner, a, b) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut thread = thread_with(&[owner, a, b]);
    thread.read_marks.insert(a, Uuid::now_v7());
    assert_eq!(
        refused(thread.remove_members(&[b, owner], &meta()).unwrap()),
        ReasonCode::OwnerNotRemovable
    );
    assert!(thread.is_member(b), "nothing is removed");
    assert_eq!(
        thread.remove_members(&[Uuid::now_v7()], &meta()).unwrap(),
        Ok(Applied::Unchanged)
    );
    assert_eq!(
        thread.remove_members(&[a], &meta()).unwrap(),
        Ok(Applied::Changed)
    );
    assert!(!thread.is_member(a));
    assert!(!thread.read_marks.contains_key(&a));
}

#[test]
fn every_member_of_an_ownerless_thread_can_be_removed() {
    let a = Uuid::now_v7();
    let mut thread = thread_with(&[a]);
    thread.owner_id = None;
    assert_eq!(
        thread.remove_members(&[a], &meta()).unwrap(),
        Ok(Applied::Changed)
    );
    assert!(thread.members.is_empty());
}

#[test]
fn resource_ids_are_a_set_per_key_and_an_emptied_key_disappears() {
    let mut thread = thread_with(&[Uuid::now_v7()]);
    let ids = |raw: &[&str]| raw.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    assert_eq!(
        thread
            .add_resource_ids("documents", &ids(&["d1", "d2", "d1"]), &meta())
            .unwrap(),
        Ok(Applied::Changed)
    );
    assert_eq!(thread.resources["documents"], ids(&["d1", "d2"]));
    assert_eq!(
        thread
            .add_resource_ids("documents", &ids(&["d2"]), &meta())
            .unwrap(),
        Ok(Applied::Unchanged)
    );
    assert_eq!(
        thread
            .remove_resource_ids("documents", &ids(&["d3"]), &meta())
            .unwrap(),
        Ok(Applied::Unchanged)
    );
    assert_eq!(
        thread
            .remove_resource_ids("documents", &ids(&["d1", "d2"]), &meta())
            .unwrap(),
        Ok(Applied::Changed)
    );
    assert!(!thread.resources.contains_key("documents"));
    assert_eq!(
        refused(thread.add_resource_ids(" ", &ids(&["x"]), &meta()).unwrap()),
        ReasonCode::ResourceKeyInvalid
    );
}

#[test]
fn a_title_is_at_most_200_characters_and_the_same_title_changes_nothing() {
    let mut thread = thread_with(&[Uuid::now_v7()]);
    assert_eq!(
        refused(
            thread
                .set_title(&"x".repeat(201), TitleSource::Command, &meta())
                .unwrap()
        ),
        ReasonCode::TitleTooLong
    );
    assert_eq!(
        thread
            .set_title("Q3 review", TitleSource::Command, &meta())
            .unwrap(),
        Ok(Applied::Changed)
    );
    assert_eq!(
        thread
            .set_title("Q3 review", TitleSource::TitleJob, &meta())
            .unwrap(),
        Ok(Applied::Unchanged)
    );
    assert_eq!(fact_types(&thread), [TITLE_CHANGED]);
}

#[test]
fn a_terminal_status_never_changes() {
    let mut thread = thread_with(&[Uuid::now_v7()]);
    assert_eq!(
        thread.close(ClosingStatus::Cancelled, &meta()).unwrap(),
        Ok(Applied::Changed)
    );
    assert_eq!(thread.status, ThreadStatus::Cancelled);
    assert_eq!(
        refused(thread.close(ClosingStatus::Completed, &meta()).unwrap()),
        ReasonCode::ThreadNotOpen
    );
    assert_eq!(fact_types(&thread), [CANCELLED]);
    assert!(thread.open_refusal().is_some());
}

#[test]
fn a_read_mark_records_no_fact_and_no_version() {
    let reader = Uuid::now_v7();
    let mut thread = thread_with(&[reader]);
    let message = Uuid::now_v7();
    thread.set_read_mark(reader, message);
    assert_eq!(thread.version, 1);
    assert!(thread.pending_facts().is_empty());
    assert_eq!(thread.read_marks.get(&reader), Some(&message));
}
