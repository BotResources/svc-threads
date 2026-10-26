//! `svc-threads [migrate|serve|schema]` — the engine boot kit dispatches on
//! argv: `migrate` (owner role), `serve` (the default), `schema` (prints the
//! SDL, touches no infrastructure).

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    svc_threads::boot::run().await?;
    Ok(())
}
