use silodb_server::{app, boot, maintenance_loop, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env();
    let state = boot(&config)?;
    tokio::spawn(maintenance_loop(state.writer.clone(), config.maintain_secs));

    let listener = tokio::net::TcpListener::bind(&config.addr).await?;
    println!(
        "silodb-server on http://{} (db: {}, maintain every {}s)",
        config.addr,
        config.db_path.display(),
        config.maintain_secs
    );
    axum::serve(listener, app(state)).await?;
    Ok(())
}
