//! A runner's artifacts, end to end: the upload request (a runner operation
//! under `threads:runner`), the POST of the bytes to the time-bounded access it
//! answers — the object store enforcing the declared size and digest — and a
//! member's time-bounded read access, fetched like a browser fetches it.
//!
//! The suite never names an object key: the store is observed through the
//! accesses Threads hands out, and, for "its bytes are gone", through the
//! bucket's object count.

use std::time::Duration;

use br_core_auth::Passport;
use rusty_s3::actions::ListObjectsV2;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::infra::S3_REGION;
use super::{M_REQUEST_UPLOAD, Q_ARTIFACT_DOWNLOAD, TestContext, gql};

/// A file a runner produced.
#[derive(Clone)]
pub struct File {
    pub name: &'static str,
    pub media_type: &'static str,
    pub bytes: Vec<u8>,
}

impl File {
    pub fn csv(body: &str) -> Self {
        Self {
            name: "report.csv",
            media_type: "text/csv",
            bytes: body.as_bytes().to_vec(),
        }
    }

    pub fn sha256(&self) -> String {
        sha256_hex(&self.bytes)
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The runner operation's raw answer for `file` under `artifact`.
pub async fn request_upload_answer(
    ctx: &TestContext,
    runner: &Passport,
    thread: Uuid,
    artifact: Uuid,
    file: &File,
) -> Value {
    ctx.run(
        runner,
        M_REQUEST_UPLOAD,
        json!({
            "thread": thread,
            "artifact": artifact,
            "name": file.name,
            "mediaType": file.media_type,
            "size": file.bytes.len(),
            "sha256": file.sha256(),
        }),
    )
    .await
}

/// The upload access a runner receives for `file`.
pub async fn request_upload(
    ctx: &TestContext,
    runner: &Passport,
    thread: Uuid,
    artifact: Uuid,
    file: &File,
) -> Value {
    let answer = request_upload_answer(ctx, runner, thread, artifact, file).await;
    gql::data(&answer, "threadsRequestArtifactUpload").clone()
}

/// POSTs `bytes` to an upload access: the presigned fields, the file last.
/// Answers the object store's status.
pub async fn post(access: &Value, bytes: &[u8]) -> u16 {
    let url = access["url"]
        .as_str()
        .expect("an upload access carries its url");
    let mut form = reqwest::multipart::Form::new();
    for (key, value) in access["fields"]
        .as_object()
        .expect("an upload access carries its form fields")
    {
        let value = value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        form = form.text(key.clone(), value);
    }
    form = form.part("file", reqwest::multipart::Part::bytes(bytes.to_vec()));
    reqwest::Client::new()
        .post(url)
        .multipart(form)
        .send()
        .await
        .expect("reach the object store")
        .status()
        .as_u16()
}

/// A runner requests the upload of `file` for `thread` and lands its bytes.
pub async fn upload(ctx: &TestContext, runner: &Passport, thread: Uuid, file: &File) -> Uuid {
    let artifact = Uuid::now_v7();
    let access = request_upload(ctx, runner, thread, artifact, file).await;
    let status = post(&access, &file.bytes).await;
    assert!(
        (200..300).contains(&status),
        "the object store takes the declared bytes: {status}"
    );
    artifact
}

/// A member's read access to `artifact`: its raw answer.
pub async fn download_answer(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    artifact: Uuid,
) -> Value {
    ctx.run(
        passport,
        Q_ARTIFACT_DOWNLOAD,
        json!({ "thread": thread, "artifact": artifact }),
    )
    .await
}

/// A member's read access to `artifact`, or `None` — "not found".
pub async fn download_url(
    ctx: &TestContext,
    passport: &Passport,
    thread: Uuid,
    artifact: Uuid,
) -> Option<String> {
    let answer = download_answer(ctx, passport, thread, artifact).await;
    gql::data(&answer, "threadsArtifactDownload")
        .as_str()
        .map(str::to_string)
}

/// Fetches a read access: the status and the bytes.
pub async fn fetch(url: &str) -> (u16, Vec<u8>) {
    let response = reqwest::get(url).await.expect("reach the object store");
    let status = response.status().as_u16();
    let bytes = response.bytes().await.unwrap_or_default().to_vec();
    (status, bytes)
}

/// How many objects the bucket holds.
pub async fn object_count(ctx: &TestContext) -> usize {
    let bucket = Bucket::new(
        ctx.minio.endpoint().parse().expect("a valid endpoint url"),
        UrlStyle::Path,
        ctx.minio.bucket.clone(),
        S3_REGION.to_string(),
    )
    .expect("a valid bucket");
    let credentials = Credentials::new(ctx.minio.access_key(), ctx.minio.secret_key());
    let url = bucket
        .list_objects_v2(Some(&credentials))
        .sign(Duration::from_secs(60));
    let body = reqwest::get(url)
        .await
        .expect("list the bucket")
        .text()
        .await
        .expect("a listing");
    ListObjectsV2::parse_response(&body)
        .unwrap_or_else(|e| panic!("a bucket listing ({e}): {body}"))
        .contents
        .len()
}
